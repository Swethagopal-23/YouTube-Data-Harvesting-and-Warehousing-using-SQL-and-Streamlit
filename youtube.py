# Importing the necessary libraries
import streamlit as st
import mysql.connector
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from datetime import datetime 
import isodate 

# Defining API key
api_service_name = "youtube"
api_version = "v3"
api_key=' '#your api key
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Function to fetch channel details and convert to DataFrame
def fetch_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    
    ch_data = {
        "channel_name": response["items"][0]["snippet"]["title"],
        "channel_id": response["items"][0]["id"],
        "channel_description": response["items"][0]["snippet"]["description"],
        # "channel_published_at": datetime.strptime(response["items"][0]["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
        "channel_published_at": datetime.strptime(response["items"][0]["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        "channel_view_count": int(response["items"][0]["statistics"]["viewCount"]),
        "channel_subscriber_count": int(response["items"][0]["statistics"]["subscriberCount"]),
        "channel_video_count": int(response["items"][0]["statistics"]["videoCount"]),
        "channel_playlist_id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    }
    df = pd.DataFrame(ch_data, index=[0])
    return df

# Function to insert fetched channel data into MySQL database
def insert_channel_data(channel_ids):
    mydb = mysql.connector.connect(host="localhost", user="root", password="your_password", database="your_db_name")
    mycursor = mydb.cursor()

    insert_query = '''
        INSERT INTO channels (channel_id, channel_name, channel_description, channel_published_at, 
                              channel_view_count, channel_subscriber_count, channel_video_count, channel_playlist_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            channel_name=VALUES(channel_name),
            channel_description=VALUES(channel_description),
            channel_published_at=VALUES(channel_published_at),
            channel_view_count=VALUES(channel_view_count),
            channel_subscriber_count=VALUES(channel_subscriber_count),
            channel_video_count=VALUES(channel_video_count),
            channel_playlist_id=VALUES(channel_playlist_id)
    '''

    for channel_id in channel_ids:
        df = fetch_channel_data(channel_id)
        for _, row in df.iterrows():
            mycursor.execute(insert_query, (
                row['channel_id'], row['channel_name'], row['channel_description'],
                row['channel_published_at'], int(row['channel_view_count']), int(row['channel_subscriber_count']),
                int(row['channel_video_count']), row['channel_playlist_id']
            ))

    mydb.commit()
    mycursor.close()
    mydb.close()

#fetching video id using playlist id
def fetch_video_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    nextPage_token = None

    while True:
        request = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            # maxResults=50,
            pageToken=nextPage_token
        )
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

        nextPage_token = response.get('nextPageToken')
        if nextPage_token is None:
            break

    return video_ids

# Function to convert the duration to total seconds
def convert_duration(duration):
    # Parse the ISO 8601 duration string
    parsed_duration = isodate.parse_duration(duration)
    # Calculate the total seconds
    total_seconds = int(parsed_duration.total_seconds())
    return total_seconds

# Function to fetch video data and convert to DataFrame
def fetch_video_data(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            vid_data = {
                "channel_id": item['snippet']['channelId'],
                "video_id": item['id'],
                "video_title": item['snippet']['title'],
                "video_tags": ','.join(item.get('tags', [])),
                "video_thumbnail": item['snippet']['thumbnails']['default']['url'] if 'default' in item['snippet']['thumbnails'] else None,
                "video_description": item.get('description', ''),
                "video_published_at": datetime.strptime(item['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"),
                "video_duration": convert_duration(item['contentDetails']['duration']),
                "video_view_count": int(item['statistics'].get('viewCount', 0)),
                "video_comment_count": int(item['statistics'].get('commentCount', 0)),
                "video_favorite_count": int(item['statistics'].get('favoriteCount', 0)),
                "video_definition": item['contentDetails']['definition'],
                "video_caption_status": item['contentDetails']['caption'],
                "video_like_count": int(item['statistics'].get('likeCount', 0))
            }
        video_data.append(vid_data)
    df = pd.DataFrame(video_data)
    return df

# Function to insert fetched video data into MySQL database
def insert_video_data(video_ids):
    mydb = mysql.connector.connect(host="localhost", user="root", password="your_password", database="your_db_name")
    mycursor = mydb.cursor()

    insert_query = '''
        INSERT INTO videos (video_id, channel_id, video_title, video_tags, video_thumbnail, video_description, 
                            video_published_at, video_duration, video_view_count, video_comment_count, 
                            video_favorite_count, video_definition, video_caption_status, video_like_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            video_title=VALUES(video_title),
            video_tags=VALUES(video_tags),
            video_thumbnail=VALUES(video_thumbnail),
            video_description=VALUES(video_description),
            video_published_at=VALUES(video_published_at),
            video_duration=VALUES(video_duration),
            video_view_count=VALUES(video_view_count),
            video_comment_count=VALUES(video_comment_count),
            video_favorite_count=VALUES(video_favorite_count),
            video_definition=VALUES(video_definition),
            video_caption_status=VALUES(video_caption_status),
            video_like_count=VALUES(video_like_count)
    '''

    df = fetch_video_data(video_ids)
    for _, row in df.iterrows():
        mycursor.execute(insert_query, (
            row['video_id'], 
            row['channel_id'], 
            row['video_title'], 
            row['video_tags'],
            row['video_thumbnail'] if 'video_thumbnail' in row else None, 
            row['video_description'] if 'video_description' in row else '', 
            row['video_published_at'], 
            row['video_duration'], 
            int(row['video_view_count']), 
            int(row['video_comment_count']), 
            int(row['video_favorite_count']), 
            row['video_definition'], 
            row['video_caption_status'], 
            int(row['video_like_count'])
        ))

    mydb.commit()
    mycursor.close()
    mydb.close() 

# Retrieving comments data
def fetch_comment_data(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for i in response['items']:
                comm_data =  {
                    "comment_id": i['snippet']['topLevelComment']['id'],
                    "video_id": i['snippet']['topLevelComment']['snippet']['videoId'],
                    "comment_text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "comment_author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "comment_published_at": datetime.strptime(i['snippet']['topLevelComment']['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                    # "comment_published_at": datetime.strptime(i['snippet']['topLevelComment']['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
                }
                comment_data.append(comm_data)

    except HttpError as e:
        if e.resp.status == 403 and 'commentsDisabled' in str(e):
            print(f"Comments are disabled for video ID {video_id}. Skipping...")
        else:
            print(f"An error occurred for video ID {video_id}: {e}")
     
    df = pd.DataFrame(comment_data)
    return df

# Function to insert fetched comment data into MySQL database
def insert_comment_data(comment_data):
    mydb = mysql.connector.connect(host="localhost", user="root", password="your_password", database="your_db_name")
    mycursor = mydb.cursor()

    insert_query = '''
        INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            comment_text = VALUES(comment_text),
            comment_author = VALUES(comment_author),
            comment_published_at = VALUES(comment_published_at)
    '''
    df = fetch_comment_data(video_ids)
    for _, row in df.iterrows():
        mycursor.execute(insert_query, (
            row['comment_id'],
            row['video_id'],
            row['comment_text'],
            row['comment_author'],
            row['comment_published_at']
        ))

    mydb.commit()
    mycursor.close()
    mydb.close() 

# Streamlit 
st.subheader(''':rainbow[YOUTUBE DATA HARVESTING AND WAREHOUSING]''')
channel_ids_input = st.text_area('Enter YouTube Channel IDs (one per line)')

if st.button('Fetch and Store Channel Data'):
    channel_ids = channel_ids_input.split('\n')
    channel_ids = [channel_id.strip() for channel_id in channel_ids if channel_id.strip()]
    if channel_ids:
        insert_channel_data(channel_ids)
        st.write("Data for provided channel IDs stored successfully!")
    else:
        st.write("No valid channel IDs provided.")

if st.button('Fetch and Store Video Data'):
    channel_ids = channel_ids_input.split('\n')
    channel_ids = [channel_id.strip() for channel_id in channel_ids if channel_id.strip()]
    if channel_ids:
        video_ids = []
        for channel_id in channel_ids:
            video_ids.extend(fetch_video_ids(channel_id))
        insert_video_data(video_ids)
        st.write("Video data for provided channel IDs stored successfully!")
    else:
        st.write("No valid channel IDs provided.")
 
if st.button('Fetch and Store Comment Data'):
    channel_ids = channel_ids_input.split('\n')
    channel_ids = [channel_id.strip() for channel_id in channel_ids if channel_id.strip()]
    if channel_ids:
        video_ids = []
        for channel_id in channel_ids:
            video_ids.extend(fetch_video_ids(channel_id))
        insert_comment_data(video_ids)
        st.write("Comment data for provided channel IDs stored successfully!")
    else:
        st.write("No valid channel IDs provided.")

# Establish a connection to the database
mydb = mysql.connector.connect(host="localhost", user="root", password="your_password", database="your_db_name")

# Create a cursor object
cursor = mydb.cursor()

def execute_query(query):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    return df

st.sidebar.write("### Data Explorer")

# Show stored channel data
st.sidebar.write("#### Channels")
cursor.execute("SELECT channel_name FROM channels")
channel_names = [row[0] for row in cursor.fetchall()]
selected_channel = st.sidebar.selectbox("Select a channel:", channel_names)

if st.sidebar.button("Show Channel Data"):
    query = "SELECT * FROM channels WHERE channel_name = %s"
    cursor.execute(query, (selected_channel,))
    channel_data = cursor.fetchone()
    st.sidebar.write("## Channel Data:")
    st.sidebar.write(f"Channel Name: {channel_data[1]}")
    st.sidebar.write(f"Channel ID: {channel_data[0]}")
    
# Show stored video data
st.sidebar.write("#### Videos")
cursor.execute("SELECT video_title FROM videos")
video_titles = [row[0] for row in cursor.fetchall()]
selected_video = st.sidebar.selectbox("Select a video:", video_titles)

if st.sidebar.button("Show Video Data"):
    query = "SELECT * FROM videos WHERE video_title = %s"
    cursor.execute(query, (selected_video,))
    video_data = cursor.fetchone()
    st.sidebar.write("## Video Data:")
    st.sidebar.write(f"Video Title: {video_data[2]}")
    st.sidebar.write(f"Video ID: {video_data[0]}")
   
# Show stored comment data
st.sidebar.write("#### Comments")
cursor.execute("SELECT comment_text FROM comments")
comment_texts = [row[0] for row in cursor.fetchall()]
selected_comment = st.sidebar.selectbox("Select a comment:", comment_texts)

if st.sidebar.button("Show Comment Data"):
    query = "SELECT * FROM comments WHERE comment_text = %s"
    cursor.execute(query, (selected_comment,))
    comment_data = cursor.fetchone()
    st.sidebar.write("## Comment Data:")
    st.sidebar.write(f"Comment Text: {comment_data[2]}")
    st.sidebar.write(f"Comment Author: {comment_data[3]}")
   
# Establish a connection to the database
mydb = mysql.connector.connect(host="localhost", user="root", password="your_password", database="your_db_name")

# Create a cursor object
cursor = mydb.cursor()

def execute_query(query):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    return df

queries = {
    "Video Titles and Corresponding Channels": "SELECT v.video_title, c.channel_name FROM videos v JOIN channels c ON v.channel_id = c.channel_id",
    "Channels with Most Number of Videos": "SELECT c.channel_name, COUNT(v.video_id) AS num_videos FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name ORDER BY num_videos DESC",
    "Top 10 Most Viewed Videos": "SELECT v.video_title, c.channel_name, v.video_view_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.video_view_count DESC LIMIT 10",
    "Number of Comments per Video": "SELECT v.video_title, COUNT(c.comment_id) AS num_comments FROM videos v JOIN comments c ON v.video_id = c.video_id GROUP BY v.video_title",
    "Videos with Highest Number of Likes": "SELECT v.video_title, c.channel_name, v.video_like_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.video_like_count DESC",
    "Total Views per Channel": "SELECT c.channel_name, SUM(v.video_view_count) AS total_views FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name",
    "Channels that Published Videos in 2022": "SELECT DISTINCT c.channel_name FROM videos v JOIN channels c ON v.channel_id = c.channel_id WHERE v.video_published_at >= '2022-01-01' AND v.video_published_at < '2023-01-01'",
    "Average Duration of Videos per Channel": "SELECT c.channel_name, AVG(v.video_duration) AS avg_duration FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name",
    "Videos with Highest Number of Comments": "SELECT v.video_title, ch.channel_name, COUNT(c.comment_id) AS num_comments FROM videos v JOIN comments c ON v.video_id = c.video_id JOIN channels ch ON v.channel_id = ch.channel_id GROUP BY v.video_title, ch.channel_name ORDER BY num_comments DESC"
}
st.write("### Queries")
selected_query = st.selectbox("Select a query to execute", list(queries.keys()))

if st.button("Execute Query"):
    query = queries[selected_query]
    df = execute_query(query)
    st.write(f"## Results: {selected_query}")
    st.write(df)
