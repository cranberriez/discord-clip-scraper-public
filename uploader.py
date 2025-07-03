# uploader.py
from google.cloud import storage
import os, logging
from dotenv import dotenv_values

# Load environment variables
env = dotenv_values(".env")
BUCKET_NAME = env.get("BUCKET_NAME", "discord-clip-site-thumbnails")
GOOGLE_APPLICATION_CREDENTIALS = env.get("GOOGLE_APPLICATION_CREDENTIALS")

# Optionally set credentials for google cloud
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# DEPRECATED CLASS, use datastore
class JSONUploader:
    def __init__(self, log_item, bucket_name=BUCKET_NAME, output_dir="messages"):
        self.log_item = log_item
        self.output_dir = output_dir
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def upload_json_data(self, channel_id):
        """Uploads the JSON file for the given channel ID if it exists."""
        filename = f"filtered_messages_{channel_id}.json"
        file_path = os.path.join(self.output_dir, filename)

        if not os.path.exists(file_path):
            self.log_item(f"JSONUploader: File {filename} does not exist. Skipping upload.")
            return

        destination_blob_name = f"messages/{filename}"  # Customize the GCS path
        self.upload_to_bucket(file_path, destination_blob_name)

    def upload_user_data(self, filenames, destination_folder="userData"):
        """Uploads additional user data files like runtime_data.json and user_icons.json."""
        for filename in filenames:
            file_path = os.path.join(self.output_dir, filename)

            if not os.path.exists(file_path):
                self.log_item(f"JSONUploader: File {filename} does not exist. Skipping upload.")
                continue

            destination_blob_name = f"{destination_folder}/{filename}"
            self.upload_to_bucket(file_path, destination_blob_name)

    def upload_to_bucket(self, file_path, destination_blob_name):
        """Uploads a file to the specified bucket, overwriting if it already exists."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(file_path)
        self.log_item(f"JSONUploader: Uploaded {file_path} to {destination_blob_name}")


class ThumbnailUploader:
    def __init__(self, log_item, bucket_name=BUCKET_NAME, thumbnail_dir="thumb"):
        self.log_item = log_item
        self.thumbnail_dir = thumbnail_dir
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.last_uploaded_files = set()

    def upload_thumbnail(self, thumbnail_filename):
        """Uploads new thumbnails to the Google Cloud bucket."""
        filename = thumbnail_filename

        if filename.endswith(".jpg") or filename.endswith(".png"):
            file_path = os.path.join(self.thumbnail_dir, filename)

            if filename not in self.last_uploaded_files:
                destination_blob_name = f"thumb/{filename}"
                self.upload_to_bucket(file_path, destination_blob_name)
                self.last_uploaded_files.add(filename)

    def upload_to_bucket(self, file_path, destination_blob_name):
        """Uploads a file to the specified bucket, overwriting if it already exists."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(file_path)
        self.log_item(f"ThumbnailUploader: Uploaded {file_path} to {destination_blob_name}", logging.DEBUG, True)

    def get_all_uuids(self):
        """Get all of the uuids of the images in the subfolder thumb in the bucket"""
        bucket = self.storage_client.get_bucket(self.bucket_name)

        # List all blobs with the prefix 'thumb/'
        blobs = bucket.list_blobs(prefix="thumb/")

        # Extract the UUIDs by removing 'thumb/' prefix and the file extension
        uuids = [blob.name[len("thumb/"):].rsplit('.', 1)[0] for blob in blobs]

        return uuids
