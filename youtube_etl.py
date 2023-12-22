
def yt_etl():
    import googleapiclient.discovery
    import pandas as pd
    import config
    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = config.YOUTUBE_API_KEY
    nextPageToken = None
    comments = []
    youtube = googleapiclient.discovery.build(api_service_name,api_version,developerKey=DEVELOPER_KEY)
    while True:
        request = youtube.commentThreads().list(part="snippet, replies",videoId="KudedLV0tP0",maxResults=200,pageToken=nextPageToken)
        response = request.execute()
        for comment in response["items"]:
                    author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                    publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    comment_info = {'author': author, 'comment': comment_text, 'published_at': publish_time}
                    comments.append(comment_info)
        nextPageToken = response.get('nextPageToken')                            
        if not nextPageToken:
                break
    df = pd.DataFrame(comments)    
    df.to_csv("s3://ankit-yt-pipe/youtube_comment.csv")