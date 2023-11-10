from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_key = 'AIzaSyCkqUbsadSkKZz6kRAW562fwY1OJHXEHB0'
youtube = build('youtube', 'v3', developerKey=api_key)


# getting channel information
def getting_ch_id(channel_id):
    request = youtube.channels().list(
            part="snippet,ContentDetails,statistics",
            id=channel_id
        )
    response=request.execute()
    

    for i in response['items']:
        data= dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                subscription_count=i['statistics']['subscriberCount'],
                channel_views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data
     
     # video id
def getting_video_id(channel_id):
    vdo_id=[]
    response = youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()

    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response_1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for i in range(len(response_1['items'])):
            vdo_id.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response_1.get('nextPageToken')
                
        if  next_page_token is None:
            break
    return vdo_id

Video_Id=getting_video_id('UCbpjEr8lHlnkf1SQ5tnDEYw')

#getting video info
def getting_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_Id=item['id'],
                        title=item['snippet']['title'],
                        tags=item['snippet'].get('tags'),
                        thumbnail=item['snippet']['thumbnails']['default']['url'],
                        description=item['snippet'].get('description'),
                        published_date=item['snippet']['publishedAt'],
                        duration=item['contentDetails']['duration'],
                        view_count=item['statistics'].get('viewCount'),
                        likes=item['statistics'].get('likeCount'),
                        comments=item['statistics'].get('commentCount'),
                        favourite_count=item['statistics']['favoriteCount'],
                        caption_status=item['contentDetails']['caption'],
                        definition=item['contentDetails']['definition']
            )
        video_data.append(data)

    return video_data

video_details=getting_video_info(Video_Id)

# getting comment information
def getting_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                            part='snippet',
                            videoId=video_id,
                            maxResults=100)
            response=request.execute()
            for item in response['items']:
                        data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                                video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        comment_data.append(data)
                
    except:
        pass
    return comment_data
                        

comment_details=getting_comment_info(Video_Id)

# getting playlist info
def getting_playlist_details(channel_id):

    next_page_token=None
    playlist_info=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token


        )

        response=request.execute()

        for item in response['items']:
            data=dict(playlist_Id=item['id'],
                    playlist_title=item['snippet']['title'],
                    channel_id=item['snippet']['channelId'],
                    channel_name=item['snippet']['channelTitle'],
                    channel_published=item['snippet']['publishedAt'],
                    video_count=item['contentDetails']['itemCount'])
            playlist_info.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_info


playlist_details=getting_playlist_details('UCbpjEr8lHlnkf1SQ5tnDEYw')

# connecting mongodb

client=pymongo.MongoClient("mongodb+srv://rjsound7:sowntharya@cluster0.eku2p1c.mongodb.net/?retryWrites=true&w=majority")
db=client['youtube_data']


def channel_details(channel_id):
     ch_details=getting_ch_id(channel_id)
     pl_details=getting_playlist_details(channel_id)
     vi_ids=getting_video_id(channel_id)
     vi_details=getting_video_info(vi_ids)
     com_details=getting_comment_info(vi_ids)

     coll1=db['channel_details']
     coll1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                       'video_information':vi_details,'comment_information':com_details})
     

     return 'upload completed successfully'
     
    # table creation
def channels_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sowntharya',
                        database='youtube_data',
                        port='5432')

    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(channel_name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subscription_count bigint,
                                                            channel_views bigint,
                                                            Total_videos int,
                                                            channel_description text,
                                                            playlist_id varchar(80)
                                                            )'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write('channel table created')

    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''INSERT into channels(channel_name,
                                            channel_id,
                                            subscription_count,
                                            channel_views,
                                            Total_videos,
                                            channel_description,
                                            playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['subscription_count'],
                row['channel_views'],
                row['Total_videos'],
                row['channel_description'],
                row['playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            st.write('channels rows and values are inserted')


# playlst table
def playlist_table():

    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sowntharya',
                        database='youtube_data',
                        port='5432')

    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(playlist_Id varchar(100) primary key,
                                                            playlist_title varchar(100) ,
                                                            channel_id varchar(100),
                                                            channel_name varchar(100),
                                                            channel_published timestamp,
                                                            video_count int
                                                            )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write('Playlist table created already')

    db=client['youtube_data']
    coll1=db['channel_details']
    pl_list=[]
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    df1=pd.DataFrame(pl_list)

    
    for index,row in df1.iterrows():
        insert_query='''INSERT into playlists(playlist_Id,
                                            playlist_title,
                                            channel_id,
                                            channel_name,
                                            channel_published,
                                            video_count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_Id'],
                row['playlist_title'],
                row['channel_id'],
                row['channel_name'],
                row['channel_published'],
                row['video_count'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            st.write('playlists rows and values are inserted')


def videos_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sowntharya',
                        database='youtube_data',
                        port='5432')

    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    
    try:


        create_query='''create table if not exists videos(Channel_name varchar(100),
                            channel_id varchar(100),
                            video_Id varchar(50) primary key,
                            title varchar(200),
                            tags text,
                            thumbnail varchar(200),
                            description text,
                            published_date timestamp,
                            duration interval,
                            view_count bigint,
                            likes bigint,
                            comments int,
                            favourite_count int,
                            caption_status varchar(50),
                            definition varchar(50)
                                                            )'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write('videos table created already')

    vi_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
            insert_query='''INSERT INTO videos(channel_name, 
                        channel_id,
                        video_Id,
                        title,
                        tags,
                        thumbnail,
                        description,
                        published_date,
                        duration,
                        view_count,
                        likes,
                        comments,
                        favourite_count,
                        caption_status,
                        definition)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['channel_name'],
                    row['channel_id'],
                    row['video_Id'],
                    row['title'],
                    row['tags'],
                    row['thumbnail'],
                    row['description'],
                    row['published_date'],
                    row['duration'],
                    row['view_count'],
                    row['likes'],
                    row['comments'],
                    row['favourite_count'],
                    row['caption_status'],
                    row['definition'])
            
            try:
            
                    cursor.execute(insert_query,values)
                    mydb.commit()
            except:
                    st.write('video values are inserted already')


# comment table
def comments_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sowntharya',
                        database='youtube_data',
                        port='5432')

    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(comment_id varchar(100) primary key,
                            video_id varchar(50),
                            comment_text text,
                            comment_author varchar(150),
                            comment_published timestamp
                            )'''
    cursor.execute(create_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(comment_id varchar(100) primary key,
                       video_id varchar(80),
                       comment_text text, 
                       comment_author varchar(150),
                       comment_published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Comment Table created already")

    com_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''INSERT INTO comments(comment_id,
                        video_id,
                        comment_text,
                        comment_author,
                        comment_published)
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published']
                )
        
        try:
                cursor.execute(insert_query,values)
                mydb.commit()
        except:
                st.write('comment are already exists')


# CREATING ALL TABLES
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return 'tables created successfully'

def view_channels_table():
    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    channels_table=st.dataframe(ch_list)

    return channels_table

def view_playlists_table():
    pl_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    playlist_table=st.dataframe(pl_list)

    return playlist_table

def view_videos_table():
    vi_list=[]
    db=client['youtube_data']
    coll2=db['channel_details']
    for vi_data in coll2.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    videos_table=st.dataframe(vi_list)

    return videos_table

def view_comments_table():
    com_list=[]
    db=client['youtube_data']
    coll3=db['channel_details']
    for com_data in coll3.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    comment_table=st.dataframe(com_list)

    return comment_table

# streamlit
with st.sidebar:
    st.title('red:[YOUTUBE DATA HARVESTING]')
    st.header('OVWERVIEW')
    st.caption('Data collected from youtube channels')
    st.caption('API Key')
    st.caption('Coded through Python Language')
    st.caption('Datas stored in Mongob')
    st.caption('Tables created and created as SQL')
    st.caption('showcasing through Streamlit UI')
    
channel_id= st.text_input('Enter the channel id')
channels=channel_id.split(',')
channels=[ch.strip() for ch in channels if ch]

if st.button('collect and store data'):
    for channel in channels:
        ch_ids=[]
        db=client['youtube_data']
        coll1=db[channel_details]
        for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
            ch_ids.append(ch_data['channel_information']['channel_id'])
            
        if channel_id in ch_ids:
            st.success('channel id ' + channel + 'already exists')
        else:
            insert=channel_details(channel)
            st.success('output')

if st.button('Migrate to sql'):
    display=tables()
    st.success(display)
    
show_table=st.radio('SELECT THE TABLES',(':blue[channels]',':red[playlists]',':green[videos]',':black[comments]'))

if show_table==':blue[channels]':
    view_channels_table()
    
elif show_table==':red[playlists]':
    view_playlists_table()
    
elif show_table==':green[videos]':
    view_videos_table()
    
elif show_table==':black[comments]':
    view_comments_table()

# sql connection
mydb=psycopg2.connect(host='localhost',
                    user='postgres',
                    password='sowntharya',
                    database='youtube_data',
                    port='5432')
cursor=mydb.cursor()

question=st.selectbox('Select your question',
                      ('1. All the videos and channel names',
                                              '2. Channels with most no.of videos',
                                              '3. Top 10 most viewed videos',
                                              '4. Comments in each video',
                                              '5. videos with highest likes ',
                                              '6. Likes of all videos',
                                              '7. views of each channels',
                                              '8. videos published on 2022'
                                              '9. Average duration of all videos in channels',
                                              '10. Videos with highest no.of. comments'))


if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

            
