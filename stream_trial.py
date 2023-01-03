
import streamlit as st
st.title('Spotify Data Explorer')
st.text('This is a web app to allow exploration of Spotify Charts Data')

import pandas as pd 
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import seaborn as sns

import matplotlib.pyplot as plt

# add_selectbox = st.sidebar.selectbox(
#     "What would you like to look at first",
#     ("Big data analysis", "Songs","About")
# )

#df1 = pd.read_csv('/Users/csuftitan/Downloads/charts.csv')
df1 = pd.read_csv('Spotify_data.csv')
df1.head()
st.header('Header of Dataframe')
st.write(df1.head())

spark = SparkSession.builder.appName("spark_app").getOrCreate()

#df = spark.read.csv(path='/Users/csuftitan/Downloads/charts.csv', inferSchema=True, header=True)
df = spark.read.csv('Spotify_data.csv', inferSchema=True, header=True)


df = df.withColumn("rank", f.col("rank").cast(t.LongType())).withColumn("date", f.col("date").cast(t.DateType())).withColumn("streams", f.col("streams").cast(t.IntegerType()))
df = df.na.drop()
df.count()
df.registerTempTable("charts")

#if add_selectbox == 'Big data analysis':    
n_country = st.selectbox(label='Please select a country', options=df1.region.unique()) 

query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = '{}' GROUP BY title  ORDER BY count DESC;".format(n_country) 

reg = spark.sql(query).toPandas().head(10) 
st.subheader("Based on the country selected, below graph represents which song has been at rank 1 most number of times")
check = st.checkbox('View query')
if check:
    st.code('query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = {} GROUP BY title  ORDER BY count DESC;".format(n_country)')
fig = plt.bar(reg['title'],reg['count'])
plt.xticks(rotation=90)
plt.title('Title Vs Count')
plt.xlabel('title')
plt.ylabel('count')
st.pyplot()
st.set_option('deprecation.showPyplotGlobalUse', False)
# Create a section for the dataframe statistics
##############################################################
    
###############################################################################
    #st.subheader("Below data is an extraction of highest lowest and average rank of all the songs by selected atrist")
    #n_art = st.selectbox(label='Please select an artist', options=df1.artist.unique())
    #avge = spark.sql("SELECT Title, MIN(rank) Highest, MAX(rank) Lowest, AVG(rank) Avg FROM charts WHERE artist like '{}' AND chart='top200' GROUP BY title ORDER BY Highest;").format(n_art)
    #avge
    
    # Draw
    
    
    
# elif add_selectbox == 'Song':
#     n_artist = st.selectbox(label='Please select your favourite artist', options=df1.artist.unique()) 
#     st.subheader("You can Select your favourite artist and get the list of songs and the urls to that song below")
#     song = ("Select title,url from charts where artist like 'Taylor Swift'")
#     song_panda = spark.sql(song).toPandas()
#     st.write(song_panda)
# elif add_selectbox == 'About':
#     st.subheader("A little about us")
#     st.write("Spotify Data Explorer is a web app that will help Spotify users to understand the trends in their favorite singer’s songs and their popularity. Artists can compare their successful songs with competitors and know where they stand in popularity among Spotify users. It will also suggest the region where the song is most streamed in and the region where the song had the highest and lowest rank in the form of graphs which will help the users understand the trends better than in a tabular format. Using the data, the user will also be able to predict if an artist will have a song in the ‘top 200’ category.")
#     st.write('If you would like to contact us, please find our details below:')
#     st.write("""Author: Manodnya Gaikwad'
#     Email id: <manodnyagaikwad@csu.fullerton.edu>'
#     'Github: '
#     'LinkedIn: <https://www.linkedin.com/in/manodnya-gaikwad/>'
#     """)
    
    
##streams per month graph
