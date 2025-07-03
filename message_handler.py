# message_handler.py
import json, os, time, asyncio, logging
from queue import Queue, Empty

MSG_QUEUE_LOG = False

class MessageHandler:
    def __init__(self, message_queue: Queue, datastore, log_item, batch_size=30, flush_interval=60):
        self.message_queue = message_queue
        self.datastore = datastore
        self.log_item = log_item

        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.pending_messages = []
        self.pending_users = {}
        self.last_flush_time = time.time()


    def start_live_message_handling(self):
        while True:
            try:
                # Get a message from the queue with a timeout
                message, author = self.message_queue.get(timeout=0.1)
                self.pending_messages.append(message)

                username = author.get("Name")
                if username and username not in self.pending_users:
                    self.pending_users[username] = author

            except Empty:
                pass  # No messages to process

            # Check if we should flush, by batch size and flush interval
            if (
                len(self.pending_messages) >= self.batch_size or
                (time.time() - self.last_flush_time >= self.flush_interval)
            ):
                if self.pending_messages:
                    self.upload(self.pending_messages, self.pending_users)
                    self.pending_messages.clear()
                    self.last_flush_time = time.time()

            time.sleep(0.1)
    
    def upload(self, pending_messages, pending_users):
        self.datastore.push_batch_msgs(pending_messages)
        self.datastore.push_batch_user_data(pending_users)
        self.log_item(f"Uploading {len(pending_messages)} messages & {len(pending_users)} users")

    async def shutdown(self):
        print("MessageHandler: Shutting down...")
        if self.pending_messages or self.pending_users:
            self.upload(self.pending_messages, self.pending_users)
            self.pending_messages.clear()
            self.pending_users.clear()
        print("MessageHandler: Shutdown complete.")
