# Data Modelling with Postgres
**Building a relational database with Postgres**


### Follwoing are the used libraries
os, glob, psycopg2, pandas


### Project Motivation
A startup called Sparkify, needs to have a relational database to perform queries regarding the songs, users are listening to. The provided data consists of json files, which need to be reordered into new tables.
Following are tables, which were created during the project: 

#### Fact table:
songplays: [songplay_id, start_time, user_id, level, song_id, 
                                artist_id, session_id, location, user_agent]

#### Dimension tables:
users:     [user_id, first_name, last_name, gender, level]
songs:     [song_id, title, artist_id, year, duration]
artists:   [artist_id, name, location, latitude, longitude]
time:      [start_time, hour, day, week, month, year, weekday]



### File Descriptions
sql_queries.p; 
create_tables.p; 
etl.py

To run, first the "create_tables.py" needs to be run which creates the tables. After, "etl.py" can be run, which fills the table with data with an etl-pipeline. 


### Licensing, Authors, Acknowledgements
I thank Sparkify for offering the data.
