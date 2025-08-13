### Updates probably won't be coming

# Discord Clip Scraper

A multi-threaded Python application for scraping and archiving all uploaded `.mp4` videos from specific Discord channels. Expired links are automatically reacquired and updated. The project uses Google Cloud Storage for persistence and supports thumbnail generation for video attachments.

---

## Features

-   **Automated Discord Scraping**: Fetches all `.mp4` attachments from specified channels.
-   **Link Expiry Handling**: Detects and reacquires expired CDN links.
-   **Google Cloud Storage Integration**: Uploads messages, thumbnails, and user data to a GCS bucket.
-   **Multi-threaded Architecture**: Uses separate threads for scraping, message handling, refetching, and thumbnail generation.
-   **Queue-based Communication**: Ensures safe, efficient inter-thread data transfer.
-   **Configurable via `.env`**: All secrets and configuration are loaded from environment variables.
-   **FFmpeg/FFprobe Support**: Required for thumbnail extraction and video processing.

---

## Requirements

-   **Python**: 3.8+
-   **Google Cloud Project**: [Google Cloud Console](https://console.cloud.google.com/)
-   **Discord Bot**: [Discord Developer Portal](https://discord.com/developers/applications)
-   **FFmpeg/FFprobe**: [FFmpeg Downloads](https://www.ffmpeg.org/)
-   **Google Cloud Storage Bucket**: With subfolders: `messages`, `thumb`, `userData`
-   **Service Account JSON**: (Optional, for explicit GCS authentication)

---

## Setup

1. **Clone the Repository**
2. **Place `ffmpeg.exe` and `ffprobe.exe`** in the project root.
3. **Create a `.env` file** (see `.sample.env` for example):
    - `TOKEN`: Your Discord bot token
    - `BUCKET_NAME`: Your GCS bucket name
    - `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account JSON (optional)
4. **Install dependencies:**
    ```sh
    pip install -r requirements.txt
    ```
5. **Run the project:**
    ```sh
    python overlord.py
    ```

---

## Configuration & Environment Variables

All configuration is managed via a `.env` file in the project root. See `.sample.env` for a template.

-   `TOKEN` – Discord bot token
-   `BUCKET_NAME` – Google Cloud Storage bucket name
-   `GOOGLE_APPLICATION_CREDENTIALS` – Path to GCP service account JSON (optional)

**Security:** Never commit your `.env` or credential files to source control!

---

## Project Structure

```
.
├── overlord.py            # Main entry point, thread orchestration
├── discord_bot_handler.py # Discord bot logic, message fetching
├── message_handler.py     # Handles file writes and message persistence
├── refetch_handler.py     # Handles reacquisition of expired CDN links
├── thumbnail_gen.py       # Generates thumbnails for video attachments
├── uploader.py            # Handles uploading thumbnails to Google Cloud Storage
├── datastore.py           # Handles data about video uploads
├── requirements.txt       # Python dependencies
├── .env                   # Your secrets/config (NOT in git)
├── .sample.env            # Example env file
├── ffmpeg.exe, ffprobe.exe# Video tools (must be present)
└── ...
```

---

## Message Data Format

Each processed message is stored as a JSON object. IDs are stringified for compatibility with large Discord IDs.

```json
{
	"Id": "Generated Unique ID",
	"Discord_id": "Stringified Discord Message ID",
	"Poster": "Author's Name",
	"Date": "Message Date",
	"Link_to_message": "Discord Link to the Message",
	"Description": "Cleaned Message Content",
	"Attachment_URL": "CDN Link",
	"Filename": "Attachment Filename (no .mp4)",
	"channelId": "Stringified Discord Channel ID"
}
```

---

## Threading & Program Flow

This project uses a multi-threaded architecture for efficiency and reliability. Each thread is responsible for a specific part of the workflow, and all communication is handled via thread-safe queues.

### Thread Overview

-   **Overlord Thread**: Main orchestrator, starts and monitors all other threads.
-   **Discord Bot Thread**: Fetches new and historical messages, pushes to queues.
-   **Message Handler Thread**: Writes messages to disk, manages persistence.
-   **Refetch Handler Thread**: Checks and reacquires expired CDN links.
-   **Thumbnail Generator Thread**: Extracts and uploads thumbnails for video attachments.

### Queues

-   **Message Queue**: Message objects to be written to disk.
-   **Refetch Queue**: Tuples for reacquisition (expiration, uuid, discord_id, channel_id).
-   **Thumbnail Queue**: Objects `{UUID, Channel_id, Attachment_url}` for thumbnail generation.

### Program Flow

1. **Startup**: `overlord.py` launches all handler threads.
2. **Monitoring**: Overlord restarts threads if they die (future feature), and cleans up on exit.
3. **Message Processing**:
    - DiscordBotThread fetches messages (live or via history).
    - Messages are processed, filtered (must have .mp4 attachments), and dispatched to queues.
    - MessageHandlerThread writes valid messages to disk.
    - RefetchHandlerThread reacquires expired links.
    - ThumbnailThread generates and uploads thumbnails.

---

## Usage Notes

-   **FFmpeg/FFprobe** must be present in the project root for thumbnailing.
-   The Google Cloud bucket must have the expected directory structure (`messages`, `thumb`, `userData`).
-   `.env` and credential files should always be excluded from version control.

---

## Contributing

Pull requests and issues are welcome! Please follow standard Python best practices and document your code.

---

## License

MIT License (or specify your own)

---

    "Expire_Timestamp": Expiration Time from the Attachment url,
    "channelId": Stringified Discord Channel ID

}

```

## Thread Descriptions

Basic descriptions of what each thread does and the functionality behind it.

TLDR: The discord bot thread handles fetching messages and new incoming messages and passing them to the message handler thread for file writes, the refetch handler thread so the attachments can be re-acquired, and the thumbnail generator thread.

All of that content is passed using queues

### Overlord Thread

The overlord thread is the main program that spins up, tracks and manages all of the other threads.

##### Queues

- Message Queue : Objects of message data passed to message\_handler thread
- Refetch Queue : Tuples(4) (expiration\_timestamp, uuid, discord\_id, channel\_id) sent refetch\_handler thread
- Thumnbail Queue : Ojbects {UUID, Channel\_id, Attachment\_url} sent to thumbnail_gen thread

#### Program Flow

1. Run > Start_threads, starts new thread for each handler function
    1. DiscordBotThread : run\_bot\_loop
    2. MessageHandlerThread : run\_message\_handle\r_loop
    3. RefetchHandlerThread : run\_refetch\_handler\_loop
    4. RefetchTestThread : run\_refetch\_test
    5. ThumbnailThread : run\_thumbnail\_generation

2. Monitor for threads dying and log them, it should restart them but not yet
3. Clean up and stop threads when exiting

### Discord Bot Thread

The bot thread handles all three queues currently. If the --fetch\_history flag is added the message history for each channel will be acquired and processed.
The bot thread always reads messages from the input chanels when they come in.

#### Process Flow

1. Messages already acquired are loaded into memory from file, all are added to refetch\_queue
2. Messages come from on\_mesage() or fetch\_channel\_history() and is sent to process\_message()
3. process\_message() handles everything:
    1. message is discarded if it has no attachments
    2. attachments in the message are looped over
        1. messages are skipped if they are already acquired
        2. if the attachment is not a .mp4 it is skipped
        3. send the author to update\_user\_icons() so it can be added if its not present
        4. add the message to the thumbnail\_queue
        5. add the message to the refetch\_queue
        6. build the message data and add it to the message\_queue

##### Fetch History

1. Read\_message\_history() loops through channel\_ids calls fetch\_channel\_history
2. Fetch\_channel\_history() loops through messages using discords .history() and passes them to process\_message()
```
