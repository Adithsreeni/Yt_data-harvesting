from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def Api_connect():
    Api_id = "AIzaSyC90GfC8UKjeze_aRmKZoGIwb4ByGvI4As"
    api_service_name = "youtube"
    api_version = "v3"

    youtube=build(api_service_name,api_version,developerKey=Api_id)

    return youtube

youtube=Api_connect()

#get channel info
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data

#get vid ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None 

    while True:
        response1= youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])  
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get_video_info
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id=video_id
        )
        response=request.execute()
       
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Title=item['snippet']['title'],
                    Video_Id=item['id'],
                    Video_Description=item['snippet'].get('description'),
                    Tags=item['snippet'].get('tags'),         
                    PublishedAt=item['snippet']['publishedAt'],
                    View_Count=item['statistics'].get('viewCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Dislike_Count=item['statistics'].get('dislikeCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Comment_Count=item['statistics'].get('commentCount'),
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#get comment info
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    channel_Id=item['snippet']['channelId'],
                    channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token=response.get('nextpageToken')
        if next_page_token is None:
            break
    return All_data    

#upload to MongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client['Youtube_data']


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    col1=db['channel_details']
    col1.insert_one({'channel_info':ch_details,
                     'playlist_info':pl_details,
                     'video_info':vi_details,
                     'comment_info':com_details
                     })
    return"upload completed successfully"

def channels_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Adhi@11004',
                        database='Youtube_data',
                        port='5432')
    cursor=mydb.cursor()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Channel_Views bigint,
                                                            Total_videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80)
                                                                )'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channel table already created")

    ch_list=[]
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
            ch_list.append(ch_data["channel_info"])
    df=pd.DataFrame(ch_list)
    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_id,
                                            Subscribers,
                                            Channel_Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id,
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Subscribers'],
                row['Channel_Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
    
        cursor.execute(insert_query,values)
        mydb.commit()

def playlist_table():
        mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Adhi@11004',
                        database='Youtube_data',
                        port='5432')

        cursor=mydb.cursor()
        mydb.commit()

        pl_list=[]
        db = client["Youtube_data"]
        coll1=db["channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_info":1}):
            for i in range(len(pl_data["playlist_info"])):
                pl_list.append(pl_data["playlist_info"][i])
        df1=pd.DataFrame(pl_list)  

        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                Title varchar(100),
                                                                channel_Id varchar(100),
                                                                channel_Name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_Count int
                                                                )''' 
        cursor.execute(create_query)
        mydb.commit()

        for index,row in df1.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                        Title,
                                                        channel_Id,
                                                        channel_Name,
                                                        PublishedAt,
                                                        Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['channel_Id'],
                        row['channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count']
                        )

        
                cursor.execute(insert_query,values)
                mydb.commit() 

def comments_table():
        mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Adhi@11004',
                        database='Youtube_data',
                        port='5432')
        cursor=mydb.cursor()

        com_list=[]
        db = client["Youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_info":1}):
           for i in range(len(com_data["comment_info"])):
                com_list.append(com_data["comment_info"][i])
        df2=pd.DataFrame(com_list)  

        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_id varchar(50),
                                                        Comment_Text text,                                                     
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
                                                                
        cursor.execute(create_query)
        mydb.commit()



        for index,row in df2.iterrows():
                        insert_query='''insert into comments(Comment_Id,                                                
                                                        Video_id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published)
                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                        values=(row['Comment_Id'],
                                row['Video_id'],
                                row['Comment_Text'],
                                row['Comment_Author'],
                                row['Comment_Published']
                                )

                        
                        cursor.execute(insert_query,values)
                        mydb.commit() 

def videos_table():
        mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Adhi@11004',
                        database='Youtube_data',
                        port='5432')
        cursor=mydb.cursor()


        vid_list=[]
        db = client["Youtube_data"]
        coll1=db["channel_details"]
        for vid_data in coll1.find({},{"_id":0,"video_info":1}):
           for i in range(len(vid_data["video_info"])):
                vid_list.append(vid_data["video_info"][i])
        df3=pd.DataFrame(vid_list)

        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Title varchar(100),
                                                        Video_Id varchar(30) primary key,                                                     
                                                        Video_Description text,
                                                        Tags text,
                                                        PublishedAt timestamp,
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Dislike_Count bigint,
                                                        Favorite_Count int,
                                                        Comment_Count int,
                                                        Duration interval,
                                                        Thumbnail varchar(200),
                                                        Caption_Status varchar(50)
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()

        cursor=mydb.cursor()
        for index,row in df3.iterrows():
                        insert_query='''insert into videos(Channel_Name,                                                
                                                            Channel_Id,
                                                            Video_Title,
                                                            Video_Id,
                                                            Video_Description,
                                                            Tags,
                                                            PublishedAt,
                                                            View_Count,
                                                            Like_Count,
                                                            Dislike_Count,
                                                            Favorite_Count,
                                                            Comment_Count,
                                                            Duration,
                                                            Thumbnail,
                                                            Caption_Status
                                                                )
                                                        
                                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                        values=(row['Channel_Name'],
                                row['Channel_Id'],
                                row['Video_Title'],
                                row['Video_Id'],
                                row['Video_Description'],
                                row['Tags'],
                                row['PublishedAt'],
                                row['View_Count'],
                                row['Like_Count'],
                                row['Dislike_Count'],
                                row['Favorite_Count'],
                                row['Comment_Count'],
                                row['Duration'],
                                row['Thumbnail'],
                                row['Caption_Status'],
                                )

                        
                        cursor.execute(insert_query,values)
                        mydb.commit()

def tables():
    channels_table()
    playlist_table()
    comments_table()
    videos_table()
    return "Tables Created Successfully"
Tables=tables()

def show_channels_table():
   ch_list=[]
   db = client["Youtube_data"]
   coll1=db["channel_details"]
   for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
      ch_list.append(ch_data["channel_info"])
   df=st.dataframe(ch_list)

   return df

def show_playlists_table():
    pl_list=[]
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(pl_data["playlist_info"])):
            pl_list.append(pl_data["playlist_info"][i])
    df1=st.dataframe(pl_list) 

    return df1

def show_comments_table():
    com_list=[]
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_info":1}):
       for i in range(len(com_data["comment_info"])):
         com_list.append(com_data["comment_info"][i])
    df2=st.dataframe(com_list) 

    return df2

def show_videos_table():
     vid_list=[]
     db = client["Youtube_data"]
     coll1=db["channel_details"]
     for vid_data in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_data["video_info"])):
            vid_list.append(vid_data["video_info"][i])
     df3=st.dataframe(vid_list)

     return df3

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":purple[skills]")
    st.caption(":blue[Python Scripting]")
    st.caption(":orange[Data Collection]")
    st.caption(":green[MongoDB]")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{'_id':0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("migrate to sql"):
    Tables=tables()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","COMMENTS","VIDEOS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="COMMENTS":
    show_comments_table()

elif show_table=="VIDEOS":
    show_videos_table()

#SQL CONN
mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Adhi@11004',
                        database='Youtube_data',
                        port='5432')
cursor=mydb.cursor()

question=st.selectbox("select your question",
                       ("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",  
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                        ))



if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select Video_Title as videos,Channel_Name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["Video title","Channel name"])
    st.write(df1)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_videos from channels order by total_videos Desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel name","No.of videos"])
    st.write(df2)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''SELECT channel_name AS Channel_Name, video_title AS Video_Title, view_count AS View_Count 
                                FROM videos
                                ORDER BY view_count DESC
                                LIMIT 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Channel name","Video title","Views"])
    st.write(df3)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select Comment_Count as no_Comment,Video_Title as video_title 
                from videos where Comment_Count is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No. of comments","Video title"])
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select video_title as videotitle,channel_name as channelname,
            like_count as likes from videos where like_count 
            is not null order by like_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likes"])
    st.write(df5)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''SELECT video_title AS Title,like_count AS Likes,dislike_count as Dislikes FROM videos ORDER BY likes DESC'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Title","Likes","Dislikes"])
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''SELECT channel_name AS Channel_Name, Channel_Views AS Views FROM channels ORDER BY views DESC'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel_Name","Views"])
    st.write(df7)

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8="""select Video_Title as video_title,PublishedAt as videorelease,Channel_Name as channelname from videos
                where extract(year from PublishedAt)=2022"""
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video title","Published at","Channel name"])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''SELECT channel_name channelname,AVG(duration) as averageduration from videos group by channel_name''' 
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    df9

    T9=[]
    for index,row in df9.iterrows():
            channel_title=row['channelname']
            average_duration=row['averageduration']
            average_duration_str=str(average_duration)
            T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''SELECT video_title as videotitle,channel_name as channelname,
                comment_count as comments from videos where comment_count is
                not null order by comment_count desc''' 
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10) 
     


