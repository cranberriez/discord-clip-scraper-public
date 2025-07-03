# discord_bot_handler.py
import json, os, hashlib, discord, logging, asyncio, time
from discord.ext import commands
from datetime import datetime, timedelta
from dotenv import dotenv_values

# Discord bot token
TOKEN = dotenv_values(".env")["TOKEN"]
LIMIT = None
MSG_LOG = False
ICON_LOG = False
NEVER_SKIP = False
GENERATE_THUMB = True

class DiscordBotHandler(commands.Bot):
    def __init__(self, message_queue, thumbnail_queue, CHANNEL_IDS, log_item, fetch_history=False, output_dir="messages"):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.log_item = log_item
        self.CHANNEL_IDS = CHANNEL_IDS
        
        self.message_queue = message_queue
        self.thumbnail_queue = thumbnail_queue
        self.fetch_history = fetch_history

        self.output_dir = output_dir

    # -------------------------
    # Discord Built-in Methods
    # -------------------------
    async def on_ready(self):
        self.log_item(f'Logged in as {self.user} (ID: {self.user.id})')
        self.log_item('Bot is ready and listening to channels.')

        if self.fetch_history:
            await self.read_message_history()
                

    async def on_message(self, message):
        if message.author == self.user:
            return

        if str(message.channel.id) in self.CHANNEL_IDS:
            self.log_item(f'{message.author.name} said: {message.content} with {len(message.attachments)} attachments')
            await self.process_message(message)

    # -------------------------
    # Message Handling
    # -------------------------
    async def process_message(self, message, reacquire = False):
        """Process a message and add it to the message queue."""
        channel_id = str(message.channel.id)
        author_name = str(message.author.name)
        author_id = message.author.id

        self.log_item(f"Processing message in channel {channel_id} by {author_name}", logging.DEBUG, True)

        if not message.attachments:
            if MSG_LOG: print(f"No attachments found in message. Skipping.")
            return

        for attachment in message.attachments:
            trimmed_url = self.trim_attachment_url(attachment.url)
            uuid = self.generate_message_id(author_id, message.created_at, trimmed_url)
            
            # Check if attachment is a video
            if ".mp4" not in attachment.url:
                continue

            # Update user icons for messages with attachments
            user_data = {
                "Name": author_name,
                "Url": message.author.avatar.url if message.author.avatar else None
            }

            # Add to thumbnail queue
            self.thumbnail_queue.put({
                "Id": uuid,
                "Attachment_URL": attachment.url
            })

            # Add to message queue if still valid
            message_data = self.build_message_data(message, uuid, attachment)
            self.message_queue.put(
                (message_data, user_data)
            )
        
            self.log_item(f"Message {uuid} added to message queue.", logging.DEBUG, True)

        return
        

    def trim_attachment_url(self, URL):
        """Trims a Discord attachment URL, removing everything after and including the last '?ex='."""
        if "?ex=" in URL:
            # Find the last occurrence of '?ex=' and trim everything from there
            trimmed_url = URL[:URL.rfind("?ex=")]
            return trimmed_url
        return URL  # Return the original URL if '?ex=' is not found


    def build_message_data(self, message, message_id, attachment):
        """Build the message data dictionary."""
        author_name = str(message.author)
        channel_id = message.channel.id
        date_str = message.created_at.timestamp()
        link_to_message = f"https://discord.com/channels/{message.guild.id}/{channel_id}/{message.id}"

        return {
            "Id": message_id, 
            "Discord_id": str(message.id),  # different id
            "Poster": author_name,
            "Date": date_str,
            "Link_to_message": link_to_message,
            "Description": message.clean_content,
            "Attachment_URL": attachment.url,
            "Filename": attachment.filename.replace('.mp4', ''),
            "Expire_Timestamp": self.get_expiration_timestamp(attachment.url),
            "channelId": str(channel_id)
        }


    def generate_message_id(self, author_name, created_at, attachment):
        """Generate a consistent unique ID for a message."""
        """Uses author name, created timestamp, and attachment full url. None change but are unique"""
        return hashlib.md5(f"{author_name}{created_at}{attachment}".encode()).hexdigest()[:8]


    def get_expiration_timestamp(self, url):
        """Extract expiration timestamp from the attachment URL, if available."""
        try:
            for param in url.split('?')[-1].split('&'):
                if param.startswith("ex="):
                    return int(param.split('=')[1], 16) # convert hex format date to int
        except ValueError:
            pass
        return None


    def is_expired(self, expire_time):
        return expire_time < int(time.time())

    # -------------------------
    # Message History Acquisition
    # -------------------------
    async def read_message_history(self):
        """Fetches message history for specified channels and checks against previous data."""
        await self.wait_until_ready()

        for channel_id in self.CHANNEL_IDS:
            channel = self.get_channel(int(channel_id))

            if channel is None:
                print(f"Error: Bot does not have access to channel {channel_id}")
                continue

            self.log_item(f"Fetching history for channel {channel_id}")
            await self.fetch_channel_history(channel)

        self.log_item("History fetch complete for all channels.")


    async def fetch_channel_history(self, channel):
        """Fetches and processes history for a specific channel."""
        async for message in channel.history(limit=LIMIT, oldest_first=False):
            await self.process_message(message)


    def run_bot(self):
        """Start the bot."""
        print("DiscordBotHandler: Starting the bot...")
        self.run(TOKEN)