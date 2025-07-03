import requests
import subprocess, os, json, time
import ffmpeg

def download_video(url, filename, byte_range=None):
    """
    Download a partial MP4 file for testing.
    
    Args:
        url (str): The URL of the MP4 file.
        filename (str): The name of the file to save the content.
        byte_range (str): The byte range to download (e.g., "0-4096").
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }
    if byte_range:
        headers["Range"] = f"bytes={byte_range}"

    response = requests.get(url, headers=headers, stream=True)

    if response.status_code == 206:  # Partial content success
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Partial MP4 file downloaded successfully: {filename}")
    elif response.status_code == 416:  # Requested Range Not Satisfiable
        print("Error: Requested byte range is invalid or unsupported.")
    else:
        print(f"Failed to download file. HTTP Status: {response.status_code}")

def get_video_length_with_ffprobe(video_filename):
    """Get video duration using ffprobe."""
    try:
        abs_path = os.path.abspath(video_filename)
        ffprobe_path = "./ffprobe.exe"  # Replace with your actual ffprobe path

        # Ensure the video file exists
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Video file not found: {abs_path}")

        start_time = time.time()

        # Run ffprobe to extract metadata
        result = subprocess.run(
            [
                ffprobe_path,
                "-v", "quiet",               # Suppress unnecessary output
                "-print_format", "json",     # Format output as JSON
                "-show_format", abs_path     # Show only the format metadata
            ],
            stdout=subprocess.PIPE,          # Capture standard output
            stderr=subprocess.PIPE,          # Capture standard error
            text=True                        # Decode output as a string
        )

        end_time = time.time()
        execution_time = end_time - start_time

        # Parse the JSON output
        metadata = json.loads(result.stdout)
        duration = float(metadata["format"]["duration"])  # Extract duration in seconds

        print(f"ffprobe execution time: {execution_time:.4f} seconds")
        return duration

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    except KeyError:
        print("Error: Duration not found in ffprobe output.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON output: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def generate_thumbnail(video_filename, thumbnail_path):
        """Generate a thumbnail using ffmpeg."""
        start_time = time.time()
        try:
            (
                ffmpeg
                .input(video_filename, ss=0)
                .filter("scale", 420, -1)
                .output(thumbnail_path, vframes=1)
                .run(overwrite_output=True, quiet=True)
            )
        except ffmpeg.Error as e:
            print(f"Error generating thumbnail: {e}")

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"ffmpeg execution time: {execution_time:.4f} seconds")

# Example usage
file_path = "./jacob_hurt_locker.mp4"
duration = get_video_length_with_ffprobe(file_path)
generate_thumbnail(file_path, file_path + ".png")

if duration is not None:
    print(f"Video duration: {duration:.2f} seconds")
else:
    print("Failed to retrieve video duration.")

# print(os.path.abspath("./jacob_hurt_locker.mp4"))
# download_video("https://cdn.discordapp.com/attachments/1180731401503506453/1291615821864501260/help.mp4?ex=67357a78&is=673428f8&hm=22eb4c67cdfeee79b6cf265cb4be24b5b5a3906091cc75eb49e0215b8673c6fb&", "partial_video_start.mp4", "0-5242879")