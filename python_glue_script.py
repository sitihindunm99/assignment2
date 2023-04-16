import io
from io import BytesIO

import pandas as pd
import boto3
import datetime
import numpy as np

bucket = "is459-hindun-assignment2"

s3 = boto3.client('s3')
now = datetime.datetime.now()
timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

file_name = "raw/final.csv"
obj = s3.get_object(Bucket= bucket, Key= file_name) 

spotify_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
spotify_df = spotify_df.drop(['Unnamed: 0', 'danceability','energy','key','mode','loudness','speechiness','acousticness','instrumentalness','liveness','valence'], axis=1)
spotify_df['artist_names'] = spotify_df['artist_names'].str.replace(',', ' |')
spotify_df['track_name'] = spotify_df['track_name'].str.replace(',', ' ')
spotify_df['artist_individual'] = spotify_df['artist_individual'].str.replace(',', ' ')
spotify_df['source'] = spotify_df['source'].str.replace(',', ' ')
spotify_df['week'] = pd.to_datetime(spotify_df['week'], errors='coerce')
spotify_df = spotify_df.dropna(subset=['week'])

spotify_df.replace('', np.nan, inplace=True)
spotify_df.to_csv("s3://"+bucket+"/cleaned/cleaned_stf.csv", index=False, mode='a', date_format='%Y-%m-%d')