# thumbnail_gen.py
import os, ffmpeg, aiohttp, asyncio, logging
from queue import Queue
from datetime import datetime
import time, subprocess, json
from collections import defaultdict

class ThumbnailGenerator:
    def __init__(self, thumbnail_queue: Queue, datastore, thumbnail_uploader, log_item, output_folder="thumb", temp_folder="temp"):
        self.thumbnail_queue = thumbnail_queue
        self.log_item = log_item
        self.datastore = datastore
        self.thumbnail_uploader = thumbnail_uploader
        self.output_folder = output_folder
        self.temp_folder = temp_folder

        # Ensure output and temp directories exist
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.temp_folder, exist_ok=True)

        self.metadata_queue = Queue()
        self.uploaded_uuids = self.thumbnail_uploader.get_all_uuids()
        self.video_lengths = self.datastore.get_all_runtimes()

    async def thumb_queue_handler(self, max_concurrent_tasks=4):
        """Continuously generate thumbnails from the queue with limited concurrency."""
        self.log_item("ThumbnailGenerator: Initializing...")
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        pending_tasks = set()

        # Stats counters
        stats = {"generated": 0, "skipped": 0, "errors": 0}

        # Start metadata writer
        metadata_task = asyncio.create_task(self.batch_save_metadata())

        try:
            while True:
                # Add new tasks from the queue
                self.add_tasks_from_queue(semaphore, pending_tasks)

                # Process completed tasks
                if pending_tasks:
                    await self.process_completed_tasks(pending_tasks, stats)
                else:
                    await asyncio.sleep(1)  # No pending tasks, prevent busy looping

        except Exception as e:
            self.log_item(f"Critical error in generate_thumbnails: {e}", logging.CRITICAL)
        finally:
            self.print_final_stats(stats)
            # Ensure metadata_task is properly handled
            metadata_task.cancel()
            try:
                await metadata_task
            except asyncio.CancelledError:
                self.log_item("batch_save_metadata: Task cancelled.", logging.DEBUG)


    async def download_video(self, url, filename):
        """Download video file from a URL."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(filename, "wb") as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)


    def generate_thumbnail(self, video_filename, thumbnail_path):
        """Generate a thumbnail using ffmpeg."""
        try:
            if not os.path.exists(video_filename):
                self.log_item(f"Video file {video_filename} does not exist. Skipping thumbnail generation.", logging.ERROR)
                return

            (
                ffmpeg
                .input(video_filename, ss=0)
                .filter("scale", 420, -1)
                .output(thumbnail_path, vframes=1)
                .global_args('-loglevel', 'error')  # Adjust logging verbosity for ffmpeg
                .run(overwrite_output=True)
            )
            self.log_item(f"Thumbnail successfully generated for {video_filename}.", logging.INFO)

        except ffmpeg.Error as e:
            self.log_item(f"Error generating thumbnail: {video_filename}: {e.stderr.decode() if e.stderr else str(e)}", logging.ERROR)
        except Exception as e:
            self.log_item(f"Unexpected error: {str(e)}", logging.ERROR)


    def get_video_length(self, file_path):
        """Get the length of a video file."""
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "format=duration", "-of", "csv=p=0", file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            output = result.stdout.strip()
            self.log_item(f"Metadata generated for {file_path}, Runtime: {output}", logging.DEBUG, True)
            if output:
                return float(output)
            else:
                self.log_item(f"Error getting video length: No duration found in ffprobe output for {file_path}", logging.ERROR)
        except ValueError:
            self.log_item(f"Error converting video length to float for {file_path}. Output: {output}", logging.ERROR)
        except Exception as e:
            self.log_item(f"Error getting video length for {file_path}: {e}", logging.ERROR)
        return None
    

    async def batch_save_metadata(self):
        """Periodically save metadata from the queue to runtime JSON."""
        self.log_item("BATCH SAVE METADATA Started.")
        changes_made = False
        last_modification = time.time()
        new_metadata = []

        while True:
            try:
                while not self.metadata_queue.empty():
                    entry = self.metadata_queue.get(timeout=0.1)
                    video_id = entry.get("Id")
                    length = entry.get("Length")

                    if not (video_id and length):
                        self.log_item(f"Invalid metadata entry skipped: {entry}", logging.ERROR)
                        continue

                    # Avoid overwriting identical entries
                    if video_id not in self.video_lengths:
                        self.video_lengths[video_id] = length
                        last_modification = time.time()
                        changes_made = True
                        new_metadata.append(entry)
                        self.log_item(f"Metadata for {video_id}, length {length} added to batch to be uploaded", logging.DEBUG)

                    elif self.video_lengths[video_id] != length:
                        self.log_item(f"Conflicting metadata for {video_id}. Keeping existing value: {self.video_lengths[video_id]}. New: {length}", logging.WARNING)
                    else:
                        self.log_item(f"Metadata already exists and is stored, skipping", logging.WARNING)

                # Save only if changes were made and, 30 changes are made, or 60 seconds have passed since last change 
                if (changes_made and 
                    (len(new_metadata) >= 50 or 
                     time.time() - last_modification >= (60) # waiting for re-uploading runtimes
                    )
                ):
                    # Upload runtime data
                    self.log_item(f"Uploading Batch Runtimes {len(new_metadata)}")
                    self.upload_runtime(new_metadata)
                    new_metadata.clear()

                    changes_made = False  # Reset flag after save

                await asyncio.sleep(1)  # Delay before the next batch
            except Exception as e:
                self.log_item(f"Error in batch_save_metadata: {e}", logging.CRITICAL)


    async def process_metadata(self, video_id, temp_video_path):
        """Process and generate video metadata."""
        try:
            loop = asyncio.get_event_loop()

            # Get video length
            video_length = await loop.run_in_executor(None, self.get_video_length, temp_video_path)
            if video_length is not None:
                self.log_item(f"Video length for {video_id}: {video_length:.2f} seconds", logging.DEBUG, True)

                # Push metadata to the queue
                self.metadata_queue.put({"Id": video_id, "Length": video_length})
                self.log_item(f"Added metadata to queue: {video_id}, Length: {video_length}", logging.DEBUG, True)
            else:
                self.log_item(f"Unable to determine video length for {video_id}.", logging.ERROR)
        except Exception as e:
            self.log_item(f"Error processing metadata for {video_id}: {e}", logging.ERROR)


    async def process_video(self, video):
        """Process a single video and generate a thumbnail if needed."""
        video_url = video["Attachment_URL"]
        video_id = video["Id"]

        temp_video_path = os.path.join(self.temp_folder, f"{video_id}.mp4")
        thumbnail_path = os.path.join(self.output_folder, f"{video_id}.png")

        try:
            # Check if metadata or thumbnail already exists
            metadata_exists = video_id in self.video_lengths
            thumbnail_exists = video_id in self.uploaded_uuids

            # print(f"Does metadata exist? {video_id} : {video_id in self.video_lengths}")
            # if not video_id in self.video_lengths:
            #     print(f"Double checking... Runtime?:{self.datastore.get_runtime_for_msg_id(video_id)}")

            if metadata_exists and thumbnail_exists:
                self.log_item(f"Metadata and thumbnail exist for {video_id}. Skipping.", logging.DEBUG, True)
                return {"status": "skipped"}

            # Download video if either element are missing
            if not metadata_exists or not thumbnail_exists:
                if not os.path.exists(temp_video_path):
                    self.log_item(f"Downloading video for {video_id}.", logging.DEBUG, True)
                    await self.download_video(video_url, temp_video_path)

            # Generate metadata if missing
            if not metadata_exists:
                await self.process_metadata(video_id, temp_video_path)

            # Generate thumbnail if missing
            if not thumbnail_exists:
                self.log_item(f"Generating thumbnail {video_id}")
                await asyncio.get_event_loop().run_in_executor(None, self.generate_thumbnail, temp_video_path, thumbnail_path)
            else:
                self.log_item(f"Thumbnail Exists {video_id}", logging.DEBUG, True)

            if os.path.exists(temp_video_path):
                os.remove(temp_video_path)
                self.log_item(f"Temporary file removed: {temp_video_path}", logging.DEBUG, True)

            # Upload Thumbnail
            self.upload_thumbnail(f"{video_id}.png")

            return {"status": "generated"}

        except Exception as e:
            self.log_item(f"Error processing video {video_id}: {e}", logging.ERROR)
            return {"status": "error"}


    def add_tasks_from_queue(self, semaphore, pending_tasks):
        """Add new tasks from the thumbnail queue to pending_tasks."""
        while not self.thumbnail_queue.empty():
            video = self.thumbnail_queue.get()
            task = asyncio.create_task(self.process_with_semaphore(video, semaphore))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)


    async def process_with_semaphore(self, video, semaphore):
        """Wrapper to process a video with a semaphore."""
        async with semaphore:
            return await self.process_video(video)


    async def process_completed_tasks(self, pending_tasks, stats):
        """Process completed tasks and update stats."""
        done, _ = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
        for completed_task in done:
            try:
                task_result = completed_task.result()
                if not task_result:  # Skip None results
                    continue

                # Update stats directly without adding metadata to the queue
                self.update_stats(stats, task_result.get("status"))

            except Exception as e:
                self.log_item(f"Error handling completed task: {e}", logging.ERROR)
                stats["errors"] += 1


    def update_stats(self, stats, status):
        """Update stats counters based on task status."""
        if status == "generated":
            stats["generated"] += 1
        elif status == "skipped":
            stats["skipped"] += 1
        elif status == "error":
            stats["errors"] += 1

    def print_final_stats(self, stats):
        """self.log_item final statistics after processing completes."""
        self.log_item(f"Thumbnails Generated: {stats['generated']}   Skipped: {stats['skipped']}   Errors: {stats['errors']}")


    def upload_runtime(self, runtime_batch):
        """Run the JSON upload"""
        self.datastore.push_batch_runtimes(runtime_batch)
        

    def upload_thumbnail(self, thumbnail_filename):
        """Run the PNG Thumbnail Upload"""
        try:
            self.log_item(f"Starting {thumbnail_filename} Upload", logging.DEBUG, True)
            self.thumbnail_uploader.upload_thumbnail(thumbnail_filename)
            self.log_item(f"Finished {thumbnail_filename} Upload", logging.DEBUG, True)
        except Exception as e:
            self.log_item(f"Error uploading runtime_data.json: {e}", logging.ERROR)
