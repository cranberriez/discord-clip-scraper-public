# overlord.py
import threading, time, argparse, asyncio, traceback, logging, psutil
from queue import Queue
from datetime import datetime
from message_handler import MessageHandler
from thumbnail_gen import ThumbnailGenerator
from uploader import JSONUploader, ThumbnailUploader
from discord_bot_handler import DiscordBotHandler
from refetch_handler import RefetchHandler
from datastore import DatastoreHandler

from prometheus_client import Gauge, start_http_server

# IDS For channels we want to gather from
CHANNEL_IDS = [
    "675233762900049930",   # Escape From Tarkov
    "1188082042034983034",  # Warthunder
    "679675078719569920",   # Other Games
    "1180731401503506453",  # Lethal Company
    "677367926176874509",   # Overwatch
    "620814611137953812",   # Gaming Moments
    "796860146382405642",   # For Honor
    "1283675776780075058",  # Deadlock
    "680162852199596045",   # Destiny
    "869025643889819700",   # New World
    "1091159481200689262",  # CSGO
    "1205547794069463061",  # Helldivers
    "946788298096001094",    # Elden Ring
    "1040870367121657896",  # GMOD
    "1314719992276713543",  # Rivals
    "1309321497956974652",   # Path of exile
    "1332196848252883004" # ARMA
]

LOG_VERBOSE = True

class Overlord:
    def __init__(self, fetch_history=False):
        self.running = True  # Control flag for graceful shutdown
        # Queues for handling data flow
        self.message_queue = Queue()
        self.thumbnail_queue = Queue()
        self.refetch_queue = Queue()

        # Prometheus metrics
        self.overlord_uptime = Gauge('overlord_uptime_seconds', 'Uptime of the Overlord process in seconds')
        self.queue_sizes = Gauge('overlord_queue_size', 'Size of queues', ['queue_name'])
        self.thread_status = Gauge('overlord_thread_status', 'Status of threads (1=alive, 0=dead)', ['thread_name'])
        self.thread_cpu_time = Gauge('overlord_thread_cpu_time_seconds', 'CPU time used by threads in seconds', ['thread_name'])
        self.total_memory_usage = Gauge('overlord_total_memory_percent', 'Total memory usage as a percentage of available resources')
        self.total_cpu_usage = Gauge('overlord_total_cpu_percent', 'Total CPU usage as a percentage of available resources')

        # Handlers for each task
        # self.json_uploader = JSONUploader(log_item=log_item)
        self.datastore = DatastoreHandler(log_item=log_item)
        self.thumbnail_uploader = ThumbnailUploader(log_item=log_item)
        self.discord_bot_handler = DiscordBotHandler(self.message_queue, self.thumbnail_queue, CHANNEL_IDS, log_item=log_item, fetch_history=fetch_history)
        self.refetch_handler = RefetchHandler(self.discord_bot_handler, self.refetch_queue, self.datastore, log_item=log_item, fetch_history=fetch_history)
        self.message_handler = MessageHandler(self.message_queue, self.datastore, log_item=log_item)
        self.thumbnail_generator = ThumbnailGenerator(self.thumbnail_queue, self.datastore, self.thumbnail_uploader, log_item=log_item)

        self.process = psutil.Process()

    def start_threads(self, test_mode=None):
        """Start threads with optional test mode for individual threads."""
        threads = []

        if test_mode is not None:
            print(f"Overlord: Running in {test_mode} test mode.")

        # Main Runner Threads

        # Bot thread
        if test_mode == "discord_bot" or test_mode is None:
            discord_bot_thread = threading.Thread(target=self.run_bot_loop, daemon=True, name="DiscordBotThread")
            discord_bot_thread.start()
            threads.append(discord_bot_thread)

        # Message thread
        if test_mode == "message_handler" or test_mode is None:
            message_thread = threading.Thread(target=self.run_message_handler_loop, daemon=True, name="MessageHandlerThread")
            message_thread.start()
            threads.append(message_thread)

        # Refetch thread
        if test_mode == "refetch" or test_mode is None:
            refetch_thread = threading.Thread(target=self.run_refetch_handler_loop, daemon=True, name="RefetchHandlerThread")
            refetch_thread.start()
            threads.append(refetch_thread)

            if test_mode == "refetch":
                # Run the bot and fetch one message per channel, simulate expired messages
                refetch_test_thread = threading.Thread(target=self.run_refetch_test, daemon=True, name="RefetchTestThread")
                refetch_test_thread.start()
                threads.append(refetch_test_thread)
                return threads

        # Thumbnail thread
        if test_mode == "thumbnail" or test_mode is None:
            thumbnail_thread = threading.Thread(target=self.run_thumbnail_generation, daemon=True, name="ThumbnailThread")
            thumbnail_thread.start()
            threads.append(thumbnail_thread)
            
            if test_mode == "thumbnail":
                # Run the thumbnail gen function to load thumbnail info into queue
                refetch_test_thread = threading.Thread(target=self.run_thumbnail_test, daemon=True, name="ThumbTestThread")
                refetch_test_thread.start()
                threads.append(refetch_test_thread)
                return threads

        # Start the queue monitor
        # monitor_thread = threading.Thread(target=self.monitor_queues, daemon=True, name="QueueMonitorThread")
        # monitor_thread.start()
        # threads.append(monitor_thread)

        return threads


    def run_bot_loop(self):
        """Run the bot in its own thread."""
        while self.running:
            try:
                print("Overlord: Starting Discord bot.")
                self.discord_bot_handler.run_bot()
            except Exception as e:
                print_trace_back("DiscordBotThread", e)
                break


    def run_message_handler_loop(self):
        """Run the live message handler."""
        while self.running:
            try:
                self.message_handler.start_live_message_handling()
            except Exception as e:
                print_trace_back("MessageHandlerThread", e)
                break


    def run_refetch_handler_loop(self):
        """Run the refetch handler."""
        while self.running:
            try:
                self.refetch_handler.start()
            except Exception as e:
                print_trace_back("RefetchThread", e)
                break


    def run_thumbnail_generation(self):
        """Run the thumbnail generator."""
        try:
            print("Overlord: Starting Thumbnail Generator.")
            asyncio.run(self.thumbnail_generator.thumb_queue_handler())
        except Exception as e:
            print_trace_back("ThumbnailThread", e)


    def cleanup(self):
        """Perform cleanup tasks and terminate gracefully."""
        print("Overlord: Performing cleanup...")
        self.running = False
        
        # Join threads (if necessary) or allow daemon threads to stop
        time.sleep(1)  # Small delay to ensure threads notice the flag
        print("Overlord: Cleanup complete.")


    def monitor_threads(self, threads):
        """Monitor threads and update Prometheus metrics."""
        while self.running:
            for thread in threads:
                # Update thread status
                self.thread_status.labels(thread_name=thread.name).set(1 if thread.is_alive() else 0)

                if not thread.is_alive():
                    print(f"Thread {thread.name} stopped unexpectedly.")
                    threads.remove(thread)
            time.sleep(5)


    def start_prometheus_server(self, port=8000):
        """Start Prometheus metrics server."""
        start_http_server(port)
        print(f"Prometheus metrics available at http://localhost:{port}/metrics")


    def monitor_metrics(self, threads):
        """Unified monitoring for Prometheus metrics."""
        start_time = time.time()
        while self.running:
            current_time = time.time()

            # Update Overlord runtime (uptime)
            self.overlord_uptime.set(current_time - start_time)

            # Update queue sizes
            self.queue_sizes.labels(queue_name='message_queue').set(self.message_queue.qsize())
            self.queue_sizes.labels(queue_name='thumbnail_queue').set(self.thumbnail_queue.qsize())
            self.queue_sizes.labels(queue_name='refetch_queue').set(self.refetch_queue.qsize())

            # Monitor thread statuses and resource usage
            thread_info = self.process.threads()  # Get all thread stats
            for thread in threads:
                thread_name = thread.name
                self.thread_status.labels(thread_name=thread_name).set(1 if thread.is_alive() else 0)

                if thread.is_alive():
                    # Get thread-specific CPU time
                    for t in thread_info:
                        if t.id == thread.ident:  # Match thread ID
                            cpu_time = t.user_time + t.system_time
                            self.thread_cpu_time.labels(thread_name=thread_name).set(cpu_time)
                            break  # Exit loop early when match is found

            # Update total memory and CPU usage
            try:
                # System-wide memory usage
                memory = psutil.virtual_memory()
                self.total_memory_usage.set(memory.percent)

                # System-wide CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)  # Use 1-second sampling for accuracy
                self.total_cpu_usage.set(cpu_percent)

            except Exception as e:
                print_trace_back("Overlord", e)

            time.sleep(3)  # Unified update interval (3 seconds)


    def run(self, test_mode=None):
        """Main run function with test_mode option for selective thread testing."""
        print("Overlord process started.")
        if test_mode:
            print(f"Running in test mode for: {test_mode}")
        
        try:
            # Start Prometheus Server, Update metrics
            self.start_prometheus_server()

            # Start all threads
            threads = self.start_threads(test_mode)

            # Start metric and queue monitoring
            self.monitor_metrics(threads)

            # Start thread monitor
            self.monitor_threads(threads)
        except KeyboardInterrupt:
            print("\nOverlord: KeyboardInterrupt detected. Shutting down...")
            self.cleanup()


    def run_refetch_test(self):
        """Run the bot and execute the refetch test once ready."""
        from dotenv import dotenv_values
        TOKEN = dotenv_values(".env").get("TOKEN")
        if not TOKEN:
            raise ValueError("TOKEN not set in .env file")

        def start_bot():
            self.discord_bot_handler.run(TOKEN)

        # Start the bot in a separate thread
        bot_thread = threading.Thread(target=start_bot, daemon=True, name="BotThread")
        bot_thread.start()

        # Schedule the refetch test coroutine on the bot's loop
        async def refetch_test_task():
            print("Scheduling refetch test coroutine.")
            asyncio.run_coroutine_threadsafe(
                self.discord_bot_handler.run_refetch_test(), self.discord_bot_handler.loop
            ).result()  # Block until the coroutine completes

        print("Waiting for bot to initialize...")
        asyncio.run(refetch_test_task())  # Run the task
        print("Refetch test mode complete.")
    
    def run_thumbnail_test(self):
        try:
            self.thumbnail_generator.load_thumbnail_info(CHANNEL_IDS)
            print("Overlord: Thumbnail Data Loaded.")
        except Exception as e:
            print_trace_back("ThumbTestThread", e)


# Map of thread names to specific colors
THREAD_COLOR_MAP = {
    "DiscordBotThread": "\033[94m",   # Blue
    "MessageHandlerThread": "\033[93m",  # Yellow
    "RefetchHandlerThread": "\033[95m",  # Magenta
    "RefetchTestThread": "\033[96m",  # Cyan
    "ThumbnailThread": "\033[92m",  # Green
    "ThumbTestThread": "\033[91m"   # Red
}
DEFAULT_THREAD_COLOR = "\033[92m"  # Default to green

def log_item(message, level=logging.INFO, verbose=False):
    """Log a message from a specific thread at the given logging level."""
    if verbose and not LOG_VERBOSE:
        return

    thread_name = threading.current_thread().name
    current_time = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    # ANSI color codes for log levels
    level_colors = {
        logging.DEBUG: "\033[94m",   # Blue
        logging.INFO: "\033[92m",    # Green
        logging.WARNING: "\033[93m",  # Yellow
        logging.ERROR: "\033[91m",    # Red
        logging.CRITICAL: "\033[95m"  # Magenta
    }
    reset_color = "\033[0m"

    # Get the color for the current log level
    level_color = level_colors.get(level, "\033[92m")  # Default to green for INFO

    # Get the color for the current thread name
    thread_color = THREAD_COLOR_MAP.get(thread_name, DEFAULT_THREAD_COLOR)

    level_name = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL"
    }.get(level, "INFO")

    # Construct the log message with thread-specific and level-specific colors
    log_message = f"{current_time} {level_color}[{level_name}]{reset_color} {thread_color}{thread_name}{reset_color}: {message}"

    print(log_message)

def print_trace_back(thread_name, e):
    log_item(f"{thread_name}: Error - {e}", logging.CRITICAL)
    traceback.print_exc()
    

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the Overlord with optional single-thread testing and history fetching.")
    parser.add_argument("--test", choices=["discord_bot", "message_handler", "refetch", "thumbnail", "json_upload", "thumbnail_upload"],
                        help="Run a specific thread for testing purposes.")
    parser.add_argument("--fetch-history", action="store_true", help="Start message history fetching.")
    args = parser.parse_args()

    # Start the Overlord with the specified test mode
    overlord = Overlord(fetch_history=args.fetch_history)
    overlord.run(test_mode=args.test)
