# Data Modelling with Postgres
**Building a relational database with Postgres**


### Follwoing are the used libraries
os<br />
glob<br />
psycopg2<br />
pandas<br />


### Project Motivation
A startup called Sparkify, needs to have a relational database to perform queries regarding the songs, users are listening to. The provided data consists of json files, which need to be reordered into new tables.
Following are tables, which were created during the project: 

#### Fact table:
songplays: [songplay_id, start_time, user_id, level, song_id, <br />
                                 artist_id, session_id, location, user_agent]

#### Dimension tables:
users:     [user_id, first_name, last_name, gender, level]<br />
songs:     [song_id, title, artist_id, year, duration]<br />
artists:   [artist_id, name, location, latitude, longitude]<br />
time:      [start_time, hour, day, week, month, year, weekday]<br />


### File Descriptions
sql_queries.py<br />
create_tables.py<br />
etl.py

To run, first the "create_tables.py" needs to be run which creates the tables. The user and password needs to be updated. After, "etl.py" can be run, which fills the table with data with an etl-pipeline. 


### Licensing, Authors, Acknowledgements
I thank Sparkify for offering the data.
