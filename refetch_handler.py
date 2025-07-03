# refetch_handler.py
import time, heapq, asyncio, logging
from datetime import datetime, timedelta

DEBUGGING_HEAP = True

class RefetchHandler:
    def __init__(self, bot, refetch_queue, datastore, log_item, check_interval=60, fetch_history=False):
        """
        :param bot: The Discord bot instance for message fetching.
        :param refetch_queue: Queue containing messages to be refetched.
        :param check_interval: Time in seconds between heap checks (default 5 minutes).
        """
        self.bot = bot
        self.refetch_queue = refetch_queue
        self.datastore = datastore
        self.refetch_heap = []  # Min-heap to track messages by expiration
        self.check_interval = check_interval  # Time between checking for expired messages
        self.log_item = log_item
        self.fetch_history = fetch_history

    def start(self):
        """Start monitoring the refetch queue and handling expired messages."""
        self.log_item("Starting refetch monitoring.")
        
        # Populate the refetch queue from the datastore if fetch_history is False
        if not self.fetch_history:
            self.populate_refetch_queue_from_datastore()
        
        print_sleeping = True
        last_message_time = 0  # log the last time we received a message from the queue

        while True:
            # Add new items from the queue into the heap
            while not self.refetch_queue.empty():
                last_message_time = time.time()

                expire_timestamp, message_id, discord_id, channel_id = self.refetch_queue.get()
                try:
                    if isinstance(expire_timestamp, (int, float)):
                        heapq.heappush(self.refetch_heap, (expire_timestamp, message_id, discord_id, channel_id))
                    else:
                        self.log_item(f"Invalid timestamp for message {message_id}: {expire_timestamp}", logging.ERROR)
                except ValueError as e:
                    self.log_item(f"Invalid expire timestamp for message {message_id}: {e}", logging.ERROR)
                print_sleeping = True

            # Check the heap for expired messages
            if last_message_time + (5 * 60) < time.time():
                self.process_expired_messages()

            # Wait for the next check
            if print_sleeping:
                self.log_item(f"Sleeping for {self.check_interval} seconds.")
                self.log_item(f"Refetch heap size: {len(self.refetch_heap)}", logging.DEBUG)
                if self.refetch_heap:
                    self.log_item(f"Lowest item {self.refetch_heap[0]}", logging.DEBUG)
                    
                print_sleeping = False
            time.sleep(self.check_interval)


    def populate_refetch_queue_from_datastore(self):
        """Retrieve all messages from the datastore and add them to the refetch queue."""
        if not self.datastore:
            self.log_item("DatastoreHandler is not provided; skipping datastore population.", logging.WARNING)
            return

        self.log_item("Populating refetch queue from datastore.")
        try:
            messages = self.datastore.get_all_messages()
            self.log_item(f"Retrieved {len(messages)} messages for refetching.", logging.DEBUG)

            for message in messages:
                expire_timestamp = message.get("Expire_Timestamp")  # Already a Unix timestamp
                discord_id = message.get("Discord_id")
                channel_id = message.get("channelId")
                if expire_timestamp and discord_id and channel_id:
                    self.refetch_queue.put((expire_timestamp, message["Id"], discord_id, channel_id))
                    self.log_item(f"Added message {message['Id']} to refetch queue.", logging.DEBUG, True)
                else:
                    self.log_item(f"Message {message.get('Id')} missing required fields, skipping.", logging.WARNING)
        except Exception as e:
            self.log_item(f"Error populating refetch queue: {e}", logging.ERROR)


    def process_expired_messages(self):
        """Process all expired messages in the heap."""
        now = int(time.time())

        while self.refetch_heap and self.refetch_heap[0][0] <= now:
            expiration_timestamp, message_id, discord_id, channel_id = heapq.heappop(self.refetch_heap)
            self.log_item(f"Refetching expired message {message_id} from channel {channel_id}.", logging.DEBUG)
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self.refetch_message(discord_id, channel_id), self.bot.loop
                )
                future.add_done_callback(
                    lambda f: self.log_item(f"Result for {message_id}: {f.result()}", logging.DEBUG)
                )
            except Exception as e:
                self.log_item(f"Failed to refetch message ID {message_id} in channel {channel_id}: {e}", logging.ERROR)
            
            time.sleep(1) # sleep for a second to avoid rate limiting


    async def refetch_message(self, discord_id, channel_id):
        """Refetch a specific message by Discord ID and channel."""
        try:
            self.log_item(f"Attempting to refetch message ID {discord_id} from channel {channel_id}.", logging.DEBUG)
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                self.log_item(f"Channel {channel_id} not found.", logging.ERROR)
                return

            # Fetch the message
            message = await channel.fetch_message(int(discord_id))
            # Process the message as needed (e.g., log, save, or enqueue for another task)
            return await self.bot.process_message(message)  # Ensure this is an async-safe method
        except Exception as e:
            self.log_item(f"Error refetching message {discord_id}: {e}", logging.ERROR)
