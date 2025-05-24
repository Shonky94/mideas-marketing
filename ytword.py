import os
import re
import time
import pandas as pd
import docx
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Folder paths
INPUT_FOLDER = r"C:\Users\Admin\Desktop\Final\data\youtubedoc"
OUTPUT_FOLDER = r"C:\Users\Admin\Desktop\Final\data\input\youtube"

def get_latest_docx(folder_path):
    """Finds the latest .docx file in the given folder."""
    if not os.path.exists(folder_path):
        print(f"Folder not found: {folder_path}")
        return None
    
    docx_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".docx")]
    if not docx_files:
        print("No .docx files found in the folder.")
        return None
    
    latest_file = max(docx_files, key=os.path.getmtime)
    print(f"Using latest file: {latest_file}")
    return latest_file

def extract_brands_and_links(doc_path):
    """Extracts brand names and corresponding YouTube links from a .docx file."""
    doc = docx.Document(doc_path)
    lines = [para.text.strip() for para in doc.paragraphs if para.text.strip()]
    brand_links = {}

    for i in range(0, len(lines) - 1, 2):
        brand_name = lines[i]
        channel_link = lines[i + 1]
        if re.match(r'https?://(www\.)?youtube\.com/@[\w\-]+', channel_link):
            brand_links[brand_name] = channel_link
    
    return brand_links

def get_channel_id(youtube, channel_url):
    """Extracts YouTube channel ID from a username link."""
    match = re.search(r"youtube\.com/@([\w\-]+)", channel_url)
    if match:
        username = match.group(1)
        try:
            response = youtube.channels().list(
                part="id",
                forHandle=username
            ).execute()
            if response["items"]:
                return response["items"][0]["id"]
        except HttpError as e:
            print(f"Error fetching channel ID for {username}: {e}")
    return None

def extract_hashtags(description):
    """Extracts hashtags from video descriptions."""
    return ", ".join(re.findall(r"#\w+", description))

def determine_video_format(video_id):
    """Determines if the video is a Short or a Long-form video."""
    if "shorts" in video_id:
        return "Shorts"
    return "Long-form"

def get_video_details(youtube, video_id):
    """Fetches video details like views, likes, comments, etc."""
    try:
        response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()
        if response["items"]:
            video = response["items"][0]
            snippet = video["snippet"]
            stats = video.get("statistics", {})
            
            # Extract hashtags from both "tags" and video description
            tag_hashtags = snippet.get("tags", [])
            description_hashtags = extract_hashtags(snippet.get("description", ""))
            all_hashtags = ", ".join(tag_hashtags) + (", " + description_hashtags if description_hashtags else "")

            return {
                "PLATFORM": "YouTube",
                "TITLE": snippet.get("title", ""),
                "VIEWS": stats.get("viewCount", "0"),
                "LIKES": stats.get("likeCount", "0"),
                "COMMENTS": stats.get("commentCount", "0"),
                "DATES": snippet.get("publishedAt", ""),
                "PRODUCT": "",  # Placeholder
                "CAMPAIGN": "",  # Placeholder
                "HASHTAGS": all_hashtags,  # Now includes tags + extracted hashtags
                "DESCRIPTION": snippet.get("description", ""),
                "EDITING": determine_video_format(video_id),  # Now determines Short/Long-form
            }
    except HttpError as e:
        print(f"Error fetching video details for {video_id}: {e}")
        return None

def get_channel_videos(youtube, channel_id, max_results=90):
    """Fetches up to max_results recent videos from a YouTube channel."""
    videos = []
    request = youtube.search().list(
        part="id,snippet",
        channelId=channel_id,
        maxResults=50,
        order="date"
    )
    while request and len(videos) < max_results:
        response = request.execute()
        for item in response.get("items", []):
            if "videoId" in item["id"]:
                video_id = item["id"]["videoId"]
                video_details = get_video_details(youtube, video_id)
                if video_details:
                    videos.append(video_details)
        request = youtube.search().list_next(request, response)
    return videos[:max_results]

def main():
    API_KEY = "AIzaSyAKJZgYml3roCACT2mbSxXNbmt0tuK8LDo"  # Replace with your actual API key

    input_doc = get_latest_docx(INPUT_FOLDER)
    if not input_doc:
        print("No valid input file found. Exiting...")
        return
    
    youtube = build("youtube", "v3", developerKey=API_KEY)
    brand_links = extract_brands_and_links(input_doc)

    if not brand_links:
        print("No valid YouTube channel links found in the document.")
        return

    output_file = os.path.join(OUTPUT_FOLDER, "youtube_data.xlsx")

    with pd.ExcelWriter(output_file) as writer:
        has_data = False
        for brand_name, channel_link in brand_links.items():
            channel_id = get_channel_id(youtube, channel_link)
            if channel_id:
                videos = get_channel_videos(youtube, channel_id)
                if videos:
                    df = pd.DataFrame(videos)
                    sheet_name = brand_name[:31]  # Excel sheet names are limited to 31 characters
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    has_data = True
            time.sleep(1)  # Avoid hitting rate limits

        if not has_data:
            df = pd.DataFrame({"Message": ["No valid channel data collected"]})
            df.to_excel(writer, sheet_name="Info", index=False)

    print(f"✅ Data saved to {output_file}")

if __name__ == "__main__":
    main()
