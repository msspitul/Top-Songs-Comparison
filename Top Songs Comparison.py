################################################################################
# Matthew Spitulnik ############################################################
# Advanced Big Data Management #################################################
# Top Songs Comparison #########################################################
# Project Summary: For this project, custom Python functions were created that 
# used the Spotipy API to collect music playlists and music attribute data directly 
# from Spotify. Spark, Python, and SQL scripts were also created that exported 
# and imported data directly into Neo4j for analysis. Neo4j was then used to 
# visualize and analyze relationships between songs, artists, playlists, and 
# musical genres.
################################################################################

################################################################################
### Install and load required packages #########################################
################################################################################

###install requierd packages
#%pip install spotipy
#%pip install pyspark
#%pip install os
#%pip install pandas
#%pip install numpy
#%pip install csv
#%pip install json
#%pip install time
#%pip install seaborn
#%pip install matplotlib

#setup pyspark and neo4j Spark init
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import translate, split, col, expr, regexp_replace, lit,  round, monotonically_increasing_id, udf
#from pyspark.sql.types import StringType, FloatType
from pyspark.sql.types import *
# NEO4J  CONFIGURATION
bolt_url = "bolt://neo4j:7687"
# Spark init
spark = SparkSession.builder \
    .master("local") \
    .appName('jupyter-pyspark') \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("Spark imported")

#copy the required neo4j apache file to the correct location
! sudo cp /home/jovyan/work/jars/neo4j-connector-apache-spark_2.12-4.1.0_for_spark_3.jar /usr/local/spark/jars/neo4j-connector-apache-spark_2.12-4.1.0_for_spark_3.jar

# Import dependencies
import os
import pandas as pd
import numpy as np
import csv
import json
import time
from time import sleep
import seaborn as sns
import matplotlib.pyplot as plt

# Credentials
# API dependencies
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Set up the spotipy functions to work with any client ID and secret to use the API
# Instructions can be found here for setting up spotipy credentials: https://developer.spotify.com/documentation/web-api/tutorials/getting-started
#cid = <your credentials here>
#secret = <your credentials here>
ccm = SpotifyClientCredentials(client_id = cid, client_secret = secret)
sp = spotipy.Spotify(client_credentials_manager = ccm)
print("Dependencies imported and spotipy functions enabled")

###This creates a "default_directory" variable, where the directory path to the
# data files folder containing all of the required data sets is saved so that it
# does not need to be constantly re-entered. Remember to use forward slashes
# instead of back slashes in the directory path. For example, if the datafiles
# folder is saved in "C:\home\project\datafiles", then "C:/home/project"
# would be inserted between the quotes below.
default_directory = "<DEFAULT DIRECTORY PATH HERE>"

#import the world_top_playlists data that will be used later for analysis
world_top_playlists = pd.read_csv(f'{default_directory}/datafiles/world_top_playlists.csv')

################################################################################
### Now create the custom functions that will be used to collect the top playlists and song info.

### Please note: the following custom functions were a collaboration between myself, 
# Ryan Richardson, and Victor Yamaykin. Ryan and Victor's information can be found here:

### *Ryan Richardson*
### *LinkedIn: linkedin.com/in/rmrichardson88*
### *GitHub: github.com/rmrichardson88*

### *Victor Yamaykin*
### *LinkedIn: linkedin.com/in/victor-yamaykin*
### *GitHub: github.com/victoryamaykin*
################################################################################

# Function to get the top Spotify playlist for each country
def get_top_playlists(country_codes):
    country_names = []

    for i,t in enumerate(countries):
        for c in country_codes:
            if c in t:
                if len(sorted(t)[1])==2:
                    country_names.insert(list(country_codes).index(c), sorted(t)[0])
                else:
                    country_names.insert(list(country_codes).index(c), sorted(t)[1])
    pl_names = []
    pl_ids = []

    for c in country_codes:
        response = sp.featured_playlists(country = c)

        while response:
            playlists = response['playlists']
            for i, item in enumerate(playlists['items']):
                pl_names.append(item['name'])
                pl_ids.append(item['id'])

            if playlists['next']:
                response = sp.next(playlists)
            else:
                response = None

    keys = ['country_name','pl_name','pl_id']
    top_pl = {}

    for i in range(len(country_codes)):
        sub_dict = {keys[0]: country_names[i], keys[1]: pl_names[i], keys[2]: pl_ids[i]}
        top_pl[country_codes[i]] = sub_dict

    return top_pl
print("get_top_playlists function created")

# Function to extract tracks from a playlist thats longer than 100 songs
def get_playlist_tracks(playlist_id):
    results = sp.playlist_tracks(playlist_id)
    tracks = results['items']
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])
    return tracks
print("get_playlist_tracks function created")

# Function to take all of the tracks from a playlist, collect the attributes for each track, then combine it into a dataframe.
def get_spotify_dataframes(playlist_name, playlist_id):
    # Set up empty lists for the relevant data
    artist_name, artist_id, album, album_id, track_name, track_ids, genre, popularity, explicit = [
    ], [], [], [], [], [], [], [], []

    # Use the get_playlist_tracks function to retrieve the track results
    track_results = get_playlist_tracks(playlist_id)

    for t in track_results:
        if t['track']:
            if sp.audio_features(t['track']['id'])[0]:
                try:
                    artist = t['track']['artists'][0]['name']
                    artist_name.append(artist)
                    artist_id.append(t['track']['artists'][0]['id'])
                    album.append(t['track']['album']['name'])
                    album_id.append(t['track']['album']['id'])
                    track_name.append(t['track']['name'])
                    track_ids.append(t['track']['id'])
                    genre.append(sp.artist(t['track']['artists'][0]['id'])['genres'])
                    popularity.append(t['track']['popularity'])
                    explicit.append(sp.track(t['track']['id'])['explicit'])
                except Exception as e:
                    print(
                        f"Error occurred while processing track: {t}. Error message: {str(e)}")
                    artist_name.append("")
                    artist_id.append("")
                    album.append("")
                    album_id.append("")
                    track_name.append("")
                    track_ids.append("")
                    genre.append("")
                    popularity.append("")
                    explicit.append("")
            else:
                next

    # Create a DataFrame with the basic song information and popularity
    tracks_df = spark.createDataFrame(zip(artist_name, artist_id, album, album_id, track_name, track_ids, genre, popularity, explicit),
                                      schema=['artist', 'artist_id', 'album', 'album_id', 'track_name', 'track_id', 'genre', 'popularity', 'explicit'])

    # Set up empty dictionary to hold the audio features
    audio_features = {}

    # Put all the ids in a list for the spotipy object to look up the audio features
    for idd in track_ids:
        try:
            audio_features[idd] = sp.audio_features(idd)[0]
        except Exception as e:
            print(
                f"Error occurred while retrieving audio features for track id: {idd}. Error message: {str(e)}")

    feature_list = ['key', 'tempo', 'time_signature', 'valence', 'liveness', 'energy', 'danceability', 'loudness',
                    'speechiness', 'acousticness', 'instrumentalness', 'mode', 'duration_ms']

    # Define a UDF to apply the lambda function
    extract_feature_udf = udf(
        lambda idd, feature: audio_features[idd][feature])

    # Add each audio feature to the tracks DataFrame
    for feature in feature_list:
        try:
            tracks_df = tracks_df.withColumn(
                feature, extract_feature_udf(col('track_id'), lit(feature)))
        except Exception as e:
            print(
                f"Error occurred while adding audio feature '{feature}' to DataFrame. Error message: {str(e)}")

    try:
        # Define column to normalize
        min_tempo_row = tracks_df.select(
            'tempo').agg({'tempo': 'min'}).collect()

        if min_tempo_row and min_tempo_row[0] and min_tempo_row[0][0] is not None:
            x = min_tempo_row[0][0]

            # Normalize the tempo variable
            tracks_df = tracks_df.withColumn(
                'norm_tempo', (tracks_df['tempo'] - x) / x)

            # Sort by popularity
            sorted_df = tracks_df.orderBy(col('popularity').desc())

            # Add additional columns
            sorted_df = sorted_df.withColumn('playlist_id', lit(playlist_id))
            sorted_df = sorted_df.withColumn(
                'top_playlist_name', lit(playlist_name))

            return sorted_df
        else:
            return None
    except Exception as e:
        print(
            f"Error occurred while normalizing tempo column. Error message: {str(e)}")
        return None


print("get_spotify_dataframes function created")

#I utilized these next code chunks to test playlists from two different countries to ensure the function worked
#test = get_spotify_dataframes('State of Mind', '37i9dQZF1DX1YPTAhwehsC')
#test.printSchema()

#test2 = get_spotify_dataframes('late night vibes', '37i9dQZF1DXdQvOLqzNHSW')
#test2.printSchema()

#create the function that will collect the playlist of top songs from each country that has Spotify
def make_sp_dataset(country_codes):
    top_pl = get_top_playlists(country_codes)
    dfs = []

    for k, v in top_pl.items():
        print(f"Making dataframe for {k}: {v['country_name']}")
        new_df = get_spotify_dataframes(v['pl_name'], v['pl_id'])
        if new_df is not None:  # Check if new_df is not None
            new_df = new_df.withColumn('country_code', lit(k))
            new_df = new_df.withColumn('country', lit(v['country_name']))
            dfs.append(new_df)
        print('Pausing for 90 seconds...')
        time.sleep(90)

    if len(dfs) == 0:
        return None

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.union(dfs[i])

    #df = df.withColumn("index", monotonically_increasing_id())

    print("Done!")
    return df
print("make_sp_dataset function created")

# Function to convert a CSV to JSON
# Takes the file paths as arguments

from collections import OrderedDict

def make_json(csvFilePath, jsonFilePath):
    csv_rows = []
    with open(csvFilePath, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames
        for row in reader:
            entry = OrderedDict()
            for field in title:
                entry[field] = row[field]
            csv_rows.append(entry)

    with open(jsonFilePath, 'w') as f:
        json.dump(csv_rows, f, sort_keys=True, indent=4, ensure_ascii=False)
        f.write('\n')
print("make_json function written")

#store a complete list of all country codes that will be iterated through to determine which countries have Spotify.
countries = [
            #A
            {"AD", "Andorra"},
            {"AE", "United Arab Emirates"},
            {"AF", "Afghanistan"},
            {"AG", "Antigua and Barbuda"},
            {"AI", "Anguilla"},
            {"AL", "Albania"},
            {"AM", "Armenia"},
            {"AO", "Angola"},
            {"AQ", "Antarctica"},
            {"AR", "Argentina"},
            {"AS", "American Samoa"},
            {"AT", "Austria"},
            {"AU", "Australia"},
            {"AW", "Aruba"},
            {"AX", "Åland Islands"},
            {"AZ", "Azerbaijan"},
            #B
            {"BA", "Bosnia and Herzegovina"},
            {"BB", "Barbados"},
            {"BD", "Bangladesh"},
            {"BE", "Belgium"},
            {"BF", "Burkina Faso"},
            {"BG", "Bulgaria"},
            {"BH", "Bahrain"},
            {"BI", "Burundi"},
            {"BJ", "Benin"},
            {"BL", "Saint Barthélemy"},
            {"BM", "Bermuda"},
            {"BN", "Brunei Darussalam"},
            {"BO", "Bolivia, Plurinational State of"},
            {"BQ", "Bonaire, Sint Eustatius and Saba"},
            {"BR", "Brazil"},
            {"BS", "Bahamas"},
            {"BT", "Bhutan"},
            {"BV", "Bouvet Island"},
            {"BW", "Botswana"},
            {"BY", "Belarus"},
            {"BZ", "Belize"},
            #C
            {"CA","Canada"},
            {"CC","Cocos (Keeling) Islands"},
            {"CD","Congo, the Democratic Republic of"},
            {"CF","Central African Republic"},
            {"CG","Congo"},
            {"CH","Switzerland"},
            {"CI","Côte d'Ivoire"},
            {"CK","Cook Islands"},
            {"CL","Chile"},
            {"CM","Cameroon"},
            {"CN","China"},
            {"CO","Colombia"},
            {"CR","Costa Rica"},
            {"CU","Cuba"},
            {"CV","Cabo Verde"},
            {"CW","Curaçao"},
            {"CX","Christmas Island"},
            {"CY","Cyprus"},
            {"CZ","Czech Republic"},
            #D
            {"DE","Germany"},
            {"DJ","Djibouti"},
            {"DK","Denmark"},
            {"DM","Dominica"},
            {"DO","Dominican Republic"},
            {"DZ","Algeria"},
            #E
            {"EC","Ecuador"},
            {"EE","Estonia"},
            {"EG","Egypt"},
            {"EH","Western Sahara"},
            {"ER","Eritrea"},
            {"ES","Spain"},
            {"ET","Ethiopia"},
            #F
            {"FI","Finland"},
            {"FJ","Fiji"},
            {"FK","Falkland Islands (Malvinas)"},
            {"FM","Micronesia, Federated States of"},
            {"FO","Faroe Islands"},
            {"FR","France"},
            #G
            {"GA","Gabon"},
            {"GB","United Kingdom of Great Britain and Northern Ireland"},
            {"GD","Grenada"},
            {"GE","Georgia"},
            {"GF","French Guiana"},
            {"GG","Guernsey"},
            {"GH","Ghana"},
            {"GI","Gibraltar"},
            {"GL","Greenland"},
            {"GM","Gambia"},
            {"GN","Guinea"},
            {"GP","Guadeloupe"},
            {"GQ","Equatorial Guinea"},
            {"GR","Greece"},
            {"GS","South Georgia and the South Sandwich Islands"},
            {"GT","Guatemala"},
            {"GU","Guam"},
            {"GW","Guinea-Bissau"},
            {"GY","Guyana"},
            #H
            {"HK","Hong Kong"},
            {"HM","Heard Island and McDonalds Islands"},
            {"HN","Honduras"},
            {"HR","Croatia"},
            {"HT","Haiti"},
            {"HU","Hungary"},
            #I
            {"ID","Indonesia"},
            {"IE","Ireland"},
            {"IL","Israel"},
            {"IM","Isle of Man"},
            {"IN","India"},
            {"IO","British Indian Ocean Territory"},
            {"IQ","Iraq"},
            {"IR","Iran, Islamic Republic of"},
            {"IS","Iceland"},
            {"IT","Italy"},
            #J
            {"JE","Jersey"},
            {"JM","Jamaica"},
            {"JO","Jordan"},
            {"JP","Japan"},
            #K
            {"KE","Kenya"},
            {"KG","Kyrgyzstan"},
            {"KH","Cambodia"},
            {"KI","Kiribati"},
            {"KM","Comoros"},
            {"KN","Saint Kitts and Nevis"},
            {"KP","Korea, Democratic People's Republic of"},
            {"KR","Korea, Republic of"},
            {"KW","Kuwait"},
            {"KY","Cayman Islands"},
            {"KZ","Kazakhstan"},
            #L
            {"LA","Lao People's Democratic Republic"},
            {"LB","Lebanon"},
            {"LC","Saint Lucia"},
            {"LI","Liechtenstein"},
            {"LK","Sri Lanka"},
            {"LR","Liberia"},
            {"LS","Lesotho"},
            {"LT","Lithuania"},
            {"LU","Luxembourg"},
            {"LV","Latvia"},
            #M
            {"MA","Morocco"},
            {"MC","Monaco"},
            {"MD","Moldova, Republic of"},
            {"ME","Montenegro"},
            {"MF","Saint Martin (French part)"},
            {"MG","Madagascar"},
            {"MH","Marshall Islands"},
            {"MK","Macedonia, the former Yugoslav Republic of"},
            {"ML","Mali"},
            {"MM","Myanmar"},
            {"MN","Mongolia"},
            {"MO","Macao"},
            {"MP","Northern Mariana Islands"},
            {"MQ","Martinique"},
            {"MR","Mauritania"},
            {"MS","Montserrat"},
            {"MT","Malta"},
            {"MU","Mauritius"},
            {"MV","Maldives"},
            {"MW","Malawi"},
            {"MX","Mexico"},
            {"MY","Malaysia"},
            {"MZ","Mozambique"},
            #N
            {"NA","Namibia"},
            {"NC","New Caledonia"},
            {"NE","Niger"},
            {"NF","Norfolk Island"},
            {"NG","Nigeria"},
            {"NI","Nicaragua"},
            {"NL","Netherlands"},
            {"NO","Norway"},
            {"NP","Nepal"},
            {"NR","Nauru"},
            {"NU","Niue"},
            {"NZ","New Zealand"},
            #O
            {"OM","Oman"},
            #P
            {"PA","Panama"},
            {"PE","Peru"},
            {"PF","French Polynesia"},
            {"PG","Papua New Guinea"},
            {"PH","Philippines"},
            {"PK","Pakistan"},
            {"PL","Poland"},
            {"PM","Saint Pierre and Miquelon"},
            {"PN","Pitcairn"},
            {"PR","Puerto Rico"},
            {"PS","Palestine, State of"},
            {"PT","Portugal"},
            {"PW","Palau"},
            {"PY","Paraguay"},
            #Q
            {"QA","Qatar"},
            #R
            {"RE","Réunion"},
            {"RO","Romania"},
            {"RS","Serbia"},
            {"RU","Russian Federation"},
            {"RW","Rwanda"},
            #S
            {"SA","Saudi Arabia"},
            {"SB","Solomon Islands"},
            {"SC","Seychelles"},
            {"SD","Sudan"},
            {"SE","Sweden"},
            {"SG","Singapore"},
            {"SH","Saint Helena, Ascension and Tristan da Cunha"},
            {"SI","Slovenia"},
            {"SJ","Svalbard and Jan Mayen"},
            {"SK","Slovakia"},
            {"SL","Sierra Leone"},
            {"SM","San Marino"},
            {"SN","Senegal"},
            {"SO","Somalia"},
            {"SR","Suriname"},
            {"SS","South Sudan"},
            {"ST","Sao Tome and Principe"},
            {"SV","El Salvador"},
            {"SX","Sint Maarten (Dutch part)"},
            {"SY","Syrian Arab Republic"},
            {"SZ","Swaziland"},
            #T
            {"TC","Turks and Caicos Islands"},
            {"TD","Chad"},
            {"TF","French Southern Territories"},
            {"TG","Togo"},
            {"TH","Thailand"},
            {"TJ","Tajikistan"},
            {"TK","Tokelau"},
            {"TL","Timor-Leste"},
            {"TM","Turkmenistan"},
            {"TN","Tunisia"},
            {"TO","Tonga"},
            {"TR","Turkey"},
            {"TT","Tuvalu"},
            {"TW","Taiwan, Province of China"},
            {"TZ","Tanzania, United Republic of"},
            #U
            {"UA","Ukraine"},
            {"UG","Uganda"},
            {"UM","United States Minor Outlying Islands"},
            {"US","United States of America"},
            {"UY","Uruguay"},
            {"UZ","Uzbekistan"},
            #V
            {"VA","Holy See"},
            {"VC","Saint Vincent and the Grenadines"},
            {"VE","Venezuela, Bolivarian Republic of"},
            {"VG","Virgin Islands, British"},
            {"VI","Virgin Islands, U.S."},
            {"VN","Viet Nam"},
            {"VU","Vanuatu"},
            #W
            {"WF","Wallis and Futuna"},
            {"WS","Samoa"},
            #Y
            {"YE","Yemen"},
            {"YT","Mayotte"},
            #Z
            {"ZA","South Africa"},
            {"ZM","Zambia"},
            {"ZW","Zimbabwe"}
]
print("Countries list created")

######get a list of just the country codes
countryCodeList=[]
for i in range(len(countries)):
    for x in countries[i]:
        if len(x)==2:
            countryCodeList.append(x)

#Not all countries have Spotify. This function tests which countries currently do have Spotify and which do not, and then seperates them into two different lists.
'''working_countrycode_list = []
fail_countrycode_list = []
for i in countryCodeList:
    try:
        get_top_playlists([i])
        working_countrycode_list.append(i)
    except:
        print(f'{i} does not have Spotify yet')
        fail_countrycode_list.append(i)'''

#results in 181 countries/country codes that we can get featured playlists for, 66 countries that we cannot:
working_countrycode_list=['AD',
 'AE',
 'AG',
 'AL',
 'AM',
 'AO',
 'AR',
 'AT',
 'AU',
 'AZ',
 'BA',
 'BB',
 'BD',
 'BE',
 'BF',
 'BG',
 'BH',
 'BI',
 'BJ',
 'BN',
 'BO',
 'BR',
 'BS',
 'BT',
 'BW',
 'BY',
 'BZ',
 'CA',
 'CD',
 'CG',
 'CH',
 'CI',
 'CL',
 'CM',
 'CO',
 'CR',
 'CV',
 'CW',
 'CY',
 'CZ',
 'DE',
 'DJ',
 'DK',
 'DM',
 'DO',
 'DZ',
 'EC',
 'EE',
 'EG',
 'ES',
 'ET',
 'FI',
 'FJ',
 'FM',
 'FR',
 'GA',
 'GB',
 'GD',
 'GE',
 'GH',
 'GM',
 'GN',
 'GQ',
 'GR',
 'GT',
 'GW',
 'GY',
 'HK',
 'HN',
 'HR',
 'HT',
 'HU',
 'ID',
 'IE',
 'IL',
 'IN',
 'IQ',
 'IS',
 'IT',
 'JM',
 'JO',
 'JP',
 'KE',
 'KG',
 'KH',
 'KI',
 'KM',
 'KN',
 'KR',
 'KW',
 'KZ',
 'LA',
 'LB',
 'LC',
 'LI',
 'LK',
 'LR',
 'LS',
 'LT',
 'LU',
 'LV',
 'MA',
 'MC',
 'MD',
 'ME',
 'MG',
 'MH',
 'MK',
 'ML',
 'MN',
 'MO',
 'MR',
 'MT',
 'MU',
 'MV',
 'MW',
 'MX',
 'MY',
 'MZ',
 'NA',
 'NE',
 'NG',
 'NI',
 'NL',
 'NO',
 'NP',
 'NR',
 'NZ',
 'OM',
 'PA',
 'PE',
 'PG',
 'PH',
 'PK',
 'PL',
 'PS',
 'PT',
 'PW',
 'PY',
 'QA',
 'RO',
 'RS',
 'RW',
 'SA',
 'SB',
 'SC',
 'SE',
 'SG',
 'SI',
 'SK',
 'SL',
 'SM',
 'SN',
 'SR',
 'ST',
 'SV',
 'SZ',
 'TD',
 'TG',
 'TH',
 'TJ',
 'TL',
 'TN',
 'TO',
 'TR',
 'TT',
 'TW',
 'TZ',
 'UA',
 'UG',
 'US',
 'UY',
 'UZ',
 'VC',
 'VE',
 'VN',
 'VU',
 'WS',
 'ZA',
 'ZM',
 'ZW']
fail_countrycode_list=['AF',
 'AI',
 'AQ',
 'AS',
 'AW',
 'AX',
 'BL',
 'BM',
 'BQ',
 'BV',
 'CC',
 'CF',
 'CK',
 'CN',
 'CU',
 'CX',
 'EH',
 'ER',
 'FK',
 'FO',
 'GF',
 'GG',
 'GI',
 'GL',
 'GP',
 'GS',
 'GU',
 'HM',
 'IM',
 'IO',
 'IR',
 'JE',
 'KP',
 'KY',
 'MF',
 'MM',
 'MP',
 'MQ',
 'MS',
 'NC',
 'NF',
 'NU',
 'PF',
 'PM',
 'PN',
 'PR',
 'RE',
 'RU',
 'SD',
 'SH',
 'SJ',
 'SO',
 'SS',
 'SX',
 'SY',
 'TC',
 'TF',
 'TK',
 'TM',
 'UM',
 'VA',
 'VG',
 'VI',
 'WF',
 'YE',
 'YT']

print(len(working_countrycode_list))
print(len(fail_countrycode_list))
#working_countrycode_list
#fail_countrycode_list

################################################################################
### Now collect all of the song info for each countries top playlist ###########
################################################################################

#####function for staggering data collection so that the API doesn't lock us out but also doesn't timeout
#IMPORTANT NOTE# This code took a while to run, so once it finished I
# exported the CSV file so that it can just be imported going forward.
# There is code to import it at the top of the page already, the file is world_top_playlists
'''count=0
schema = StructType([])
ds_full_list=spark.createDataFrame([],schema)
for i in range(0,len(working_countrycode_list)):
    temp_df=make_sp_dataset([working_countrycode_list[i]])
    if len(ds_full_list.columns)==0:
        ds_full_list=temp_df
    else:
        ds_full_list=ds_full_list.union(temp_df)
    count=count+1
    if count == 5:
        ds_full_list.toPandas().to_csv(f'{default_directory}/datafiles/top_playlists_{working_countrycode_list[0]}to{working_countrycode_list[i]}.csv', index = False, header = True)
        count=0
        print('Pausing for 3 minutes...')
        time.sleep(180)'''

#use the csv to json function to convert the world_top_playlists to a json:
make_json('world_top_playlists.csv', 'world_top_playlists.json')

################################################################################
### Now use neo4j to analyze the data ##########################################
################################################################################

file_name = "file:///f'{default_directory}/datafiles/world_top_playlists.json'"
all_json_df=spark.read.option("multiline", True).option("header", True).json(file_name)
all_json_df=all_json_df.withColumn('genre',translate('genre','[]\'','')).withColumn('genreList',split(col('genre'),',').alias('genreList')).drop('genre')
all_json_df.toPandas()

"""***before beginning, ensure elements don't exist in neo4j already (run in neo4j)***

DROP CONSTRAINT con_albumID; \
DROP CONSTRAINT con_artistID; \
DROP CONSTRAINT con_countryCode; \
DROP CONSTRAINT con_genreName; \
DROP CONSTRAINT con_playlistID; \
DROP CONSTRAINT con_trackID; \

MATCH (p:Playlists) DETACH DELETE p; \
MATCH (t:Tracks) DETACH DELETE t; \
MATCH (a:Artists) DETACH DELETE a; \
MATCH (a:Albums) DETACH DELETE a; \
MATCH (c:Countries) DETACH DELETE c; \
MATCH (g:GenreCounts) DETACH DELETE gc
"""

#create the Playlists node labels
cypher_Playlists='''
MERGE (p:Playlists{playlistID:event.playlist_id})
ON CREATE SET p.playlistName=event.top_playlist_name, p.countryCode=event.country_code,p.trackID=[event.track_id]
ON MATCH SET p.trackID=p.trackID+event.track_id
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_playlistID FOR (p:Playlists) REQUIRE p.playlistID IS UNIQUE;""")\
    .option("query", cypher_Playlists)\
    .save()
print(cypher_Playlists)

#create the Tracks node labels
cypher_Tracks='''
MERGE (t:Tracks {trackID:event.track_id})
ON CREATE SET
t.trackName=event.track_name,
t.artistID=event.artist_id,
t.albumID=event.album_id,
t.trackName=event.track_name,
t.explicit=toBoolean(event.explicit),
t.duration=toInteger(event.duration_ms),
t.acousticness=toFloat(event.acousticness),
t.danceability=toFloat(event.danceability),
t.energy=toFloat(event.energy),
t.index=event.index,
t.instrumentalness=toFloat(event.instrumentalness),
t.key=toInteger(event.key),
t.liveness=toFloat(event.liveness),
t.loudness=toFloat(event.loudness),
t.mode=toInteger(event.mode),
t.normTempo=toFloat(event.norm_tempo),
t.popularity=toInteger(event.popularity),
t.speechiness=toFloat(event.speechiness),
t.tempo=toFloat(event.tempo),
t.timeSignature=toFloat(event.time_signature),
t.valence=toFloat(event.valence),
t.genre=toStringList(event.genreList)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_trackID FOR (t:Tracks) REQUIRE t.trackID IS UNIQUE;""")\
    .option("query", cypher_Tracks)\
    .save()
print(cypher_Tracks)

#create the Artists node labels
cypher_Artists='''
MERGE (a:Artists{artistID:event.artist_id})
ON CREATE SET a.artistName=event.artist
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_artistID FOR (a:Artists) REQUIRE a.artistID IS UNIQUE;""")\
    .option("query", cypher_Artists)\
    .save()
print(cypher_Artists)

#create the Albums node labels
cypher_Albums='''
MERGE (a:Albums{albumID:event.album_id})
ON CREATE SET a.albumName=event.album, a.artistID=event.artist_id
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_albumID FOR (a:Albums) REQUIRE a.albumID IS UNIQUE;""")\
    .option("query", cypher_Albums)\
    .save()
print(cypher_Albums)

#create the Countries node labels
cypher_Countries='''
MERGE (c:Countries{countryCode:event.country_code})
ON CREATE SET c.country=event.country
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_countryCode FOR (c:Countries) REQUIRE c.countryCode IS UNIQUE;""")\
    .option("query", cypher_Countries)\
    .save()
print(cypher_Countries)

#create a total count of songs for each music genre
cypher_GenreCounts='''
MATCH (t:Tracks)
FOREACH (item in t.genre |
MERGE (g:GenreCounts{genreName:item})
ON CREATE SET g.totalSongs=1
ON MATCh SET g.totalSongs=g.totalSongs+1)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("script", """CREATE CONSTRAINT con_genreName FOR (g:GenreCounts) REQUIRE g.genreName IS UNIQUE;""")\
    .option("query", cypher_GenreCounts)\
    .save()
print(cypher_GenreCounts)

####################now create basic relationships
#artist performs track relationship
cypher_relationships_PERFORMS='''
MATCH (t:Tracks),(a:Artists)
WHERE (t.artistID)=(a.artistID)
CREATE (a) - [:PERFORMS] -> (t)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_PERFORMS)\
    .save()
print(cypher_relationships_PERFORMS)

#artist creates album relationship
cypher_relationships_CREATES='''
MATCH (al:Albums),(a:Artists)
WHERE (al.artistID)=(a.artistID)
CREATE (a) - [:CREATES] -> (al)
'''
all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_CREATES)\
    .save()
print(cypher_relationships_CREATES)

#album contains a track relationship
cypher_relationships_CONTAINS='''
MATCH (a:Albums),(t:Tracks)
WHERE (a.albumID)=(t.albumID)
CREATE (a) - [:CONTAINS] -> (t)
'''
all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_CONTAINS)\
    .save()
print(cypher_relationships_CONTAINS)

#track added to playlist relationship
cypher_relationships_ADDED_TO='''
MATCH (t:Tracks),(p:Playlists)
WHERE (t.trackID IN p.trackID)
CREATE (t) - [:ADDED_TO] -> (p)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_ADDED_TO)\
    .save()
print(cypher_relationships_ADDED_TO)

#playlist popular in a country relationship
cypher_relationships_POPULAR_IN='''
MATCH (p:Playlists),(c:Countries)
WHERE (p.countryCode)=(c.countryCode)
CREATE (p) - [:POPULAR_IN] -> (c)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_POPULAR_IN)\
    .save()
print(cypher_relationships_POPULAR_IN)

#merge a relationship that will be used to show the traits of songs for each country
cypher_relationships_COUNTRY_TRACKS='''
MATCH (t:Tracks) - [:ADDED_TO] -> (p:Playlists) - [:POPULAR_IN] -> (c:Countries)
MERGE (c) - [:COUNTRY_TRACKS] -> (t)
'''
all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_COUNTRY_TRACKS)\
    .save()
print(cypher_relationships_COUNTRY_TRACKS)

#track genre to genre count relationship
cypher_relationships_IS_TYPE='''
MATCH (t:Tracks),(g:GenreCounts)
WHERE (g.genreName IN t.genre)
CREATE (t) - [:IS_TYPE] -> (g)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_IS_TYPE)\
    .save()
print(cypher_relationships_IS_TYPE)

#genrecount per country relationship
cypher_relationships_COUNTRY_GENRE='''
MATCH (c:Countries) - [:COUNTRY_TRACKS] -> (t:Tracks) - [:IS_TYPE] -> (g:GenreCounts)
MERGE (c) - [:COUNTRY_GENRE] -> (g)
'''

all_json_df.write.format("org.neo4j.spark.DataSource").mode("Overwrite")\
    .option("url", bolt_url)\
    .option("query", cypher_relationships_COUNTRY_GENRE)\
    .save()
print(cypher_relationships_COUNTRY_GENRE)

"""***look at data graphs (performed in neo4j)***

### view bigger playlist picture
MATCH (t:Tracks) - [:ADDED_TO] -> (p:Playlists) - [:POPULAR_IN] -> (c:Countries) \
RETURN t,p,c;

### view the different country playlists that the song "C'est bon" is on
MATCH (t:Tracks {trackName:"C'est bon"}) - [:ADDED_TO] -> (p:Playlists) - [:POPULAR_IN] -> (c:Countries) \
RETURN t,p,c;

### show songs directly connected to the playlists country
MATCH (c) - [p:COUNTRY_TRACKS] -> (t) \
RETURN t,p,c;

### create a relationship between artists and how many playlists they are on
MATCH (a:Artists) - [:PERFORMS] -> (t:Tracks) - [:ADDED_TO] -> (p:Playlists) \
MERGE (a) - [:ARTIST_PLAYLISTS] -> (p)
RETURN a,t,p

### show countrys directly connected to genre count
MATCH (c) - [:COUNTRY_GENRE] -> (g) \
RETURN c,t,g
"""

####read back out from neo4j all of the tracks with their audio features and how many playlists a song appears on
cypher_track_Pcount='''
MATCH (t:Tracks) - [r:ADDED_TO] -> ()
RETURN  t.trackName as trackName, t.trackID as trackID, count(r) as count,
t.acousticness as acousticness,
t.danceability as danceability,
t.duration as duration,
t.energy as energy,
t.instrumentalness as instrumentalness,
t.key as key,
t.liveness as liveness,
t.loudness as loudness,
t.normTempo as normTempo,
t.popularity as popularity,
t.speechiness as speechiness,
t.tempo as tempo,
t.timeSignature as timeSignature,
t.valence as valence
'''

track_Pcount=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_track_Pcount)\
    .load()
track_Pcount_PD=track_Pcount.toPandas().sort_values('count', ascending=False)
track_Pcount_PD

track_Pcount_PD.corr()

track_Pcount_PD_heatmap=sns.heatmap(track_Pcount_PD.corr())
plt.savefig(r'images/track_Pcount_PD_heatmap.jpg')
print(track_Pcount_PD_heatmap)

#get the average stats for songs in each country
cypher_country_avgStats='''
MATCH (c:Countries) - [r:COUNTRY_TRACKS] -> (t:Tracks)
RETURN  c.country as countryName,
count(r) as count,
avg(t.acousticness) as acousticness,
avg(t.danceability) as danceability,
avg(t.duration) as duration,
avg(t.energy) as energy,
avg(t.instrumentalness) as instrumentalness,
avg(t.key) as key,
avg(t.liveness) as liveness,
avg(t.loudness) as loudness,
avg(t.normTempo) as normTempo,
avg(t.popularity) as popularity,
avg(t.speechiness) as speechiness,
avg(t.tempo) as tempo,
avg(t.timeSignature) as timeSignature,
avg(t.valence) as valence'''

country_avgStats=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_country_avgStats)\
    .load()
country_avgStats.toPandas().sort_values(['popularity','energy'],ascending=False)

#now the the differential between a countries song traits averages and the overall collection of songs averages
cypher_countryStats_diff='''
MATCH (s:Tracks)
MATCH (c:Countries) - [r:COUNTRY_TRACKS] -> (t:Tracks)
RETURN  c.country as countryName, count(distinct(r)) as count,
avg(t.acousticness)-avg(s.acousticness) as acousticness,
avg(t.danceability)-avg(s.danceability) as danceability,
avg(t.duration)-avg(s.duration) as duration,
avg(t.energy)-avg(s.energy) as energy,
avg(t.instrumentalness)-avg(s.instrumentalness) as instrumentalness,
avg(t.key)-avg(s.key) as key,
avg(t.liveness)-avg(s.liveness) as liveness,
avg(t.loudness)-avg(s.loudness) as loudness,
avg(t.normTempo)-avg(s.normTempo) as normTempo,
avg(t.popularity)-avg(s.popularity) as popularity,
avg(t.speechiness)-avg(s.speechiness) as speechiness,
avg(t.tempo)-avg(s.tempo) as tempo,
avg(t.timeSignature)-avg(s.timeSignature) as timeSignature,
avg(t.valence)-avg(s.valence) as valence'''

countryStats_diff=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_countryStats_diff)\
    .load()
countryStats_diff.toPandas().sort_values(['popularity','energy'],ascending=False)

#show how many playlists each artist shows up on
cypher_artist_PLCount='''
MATCH (a:Artists) - [r:ARTIST_PLAYLISTS] -> ()
RETURN  a.artistName as artistName, count(r) as count
'''
artist_PLCount=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_artist_PLCount)\
    .load()
artist_PLCount.toPandas().sort_values('count',ascending=False)

#look at the average song traits for each artist and how many tracks they have across all of the playlists
cypher_artist_avgStats='''
MATCH (a:Artists) - [r:PERFORMS] -> (t:Tracks)
RETURN a.artistName as name,
count(r) as trackCount,
avg(t.acousticness) as acousticness,
avg(t.danceability) as danceability,
avg(t.duration) as duration,
avg(t.energy) as energy,
avg(t.instrumentalness) as instrumentalness,
avg(t.key) as key,
avg(t.liveness) as liveness,
avg(t.loudness) as loudness,
avg(t.normTempo) as normTempo,
avg(t.popularity) as popularity,
avg(t.speechiness) as speechiness,
avg(t.tempo) as tempo,
avg(t.timeSignature) as timeSignature,
avg(t.valence) as valence'''

artist_avgStats=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_artist_avgStats)\
    .load()
artist_avgStats.toPandas().sort_values(['popularity','energy'],ascending=False)

#see how many songs are of each genre type
cypher_genre_songCount='''
MATCH (g:GenreCounts)
RETURN g.genreName as Genre, g.totalSongs as totalSongs
ORDER BY totalSongs DESC
'''

genre_songCount=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_genre_songCount)\
    .load()
genre_songCount.toPandas().sort_values('totalSongs',ascending=False)

#show genres per country
cypher_country_genreCount='''
MATCH (c) - [r:COUNTRY_GENRE] -> (g)
RETURN c.country as Country,
g.genreName as Genre,
g.totalSongs as TotalSongs
'''

country_genreCount=spark.read.format("org.neo4j.spark.DataSource")\
    .option("url",bolt_url)\
    .option("query",cypher_country_genreCount)\
    .load()
country_genreCount.toPandas().sort_values(['country','totalSongs'],ascending=False)