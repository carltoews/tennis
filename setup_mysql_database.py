#!/usr/bin/env python

'''
File:    setup_mysql_database.py
Author:  Carl Toews
Date:    9/12/17

Description:    
    This script takes data in CSV files and stores it in a MySQL 
database.  ATP match data is located at https://github.com/JeffSackmann/tennis_atp
in CSV format. Betting records are stored at http://www.tennis-data.co.uk/alldata.


Details:
    This script assumes that the ATP match data is located in a directory called
"tennis_atp-master", and that the file names of the ATP data are as
follows:
    *  "atp_matches_xxxx.csv" (xxxx represents a year between 1968 and 2017)
    *  "atp_matches_futures_xxxx.csv" (xxxx is a year between 1991 and 2017)
    *  "atp_matches_qual_chall_xxxx.csv" (xxxx is a year between 1991 and 2017)
There is also a single CSV file with player info:
    * "atp_players.csv"
and week-by-week rankings by decade, stored in files with names as follows:
    *  "atp_rankings_xxs.csv" (xx represents a decade--70,80,90,00, or 10)
The rankings of the current decade are stored in
    *  "atp_rankings_current.csv"
The script assumes that the odds data is stored in directory called "odd_data"
and the files in this directory are of the form
    * "xxxx.csv" (xxxx a year between 2001 and 2017)
The names and paths of these subdirectories can be configured on the first lines
of the script below.  The script also assumes a MySQL database named "tennis" 
exists, and there there is a user called "testuser" with password "test623" who
can create tables in this database.  Again, username and password can be configured
below. 

'''

#%%

#########################
# CONFIGURABLE PARAMETERS
#########################

# name of database
db_name = "tennis"
# name of db user
username = "testuser"
# db password for db user
password = "test623"
# location of atp data files
atpfile_directory = "/Users/ctoews/Projects/Data_Science/Tennis/tennis_atp-master/"
# location of odds data files
oddsfiles_directory = "/Users/ctoews/Projects/Data_Science/Tennis/odds_data/"

#%%

#####################
# IMPORT STATEMENTS
#####################

import sqlalchemy # pandas-mysql interface library
import sqlalchemy.exc # exception handling
from   sqlalchemy import create_engine  # needed to define db interface
import glob # for file manipulation
import sys # for defining behavior under errors

#%%

# This cell tries to connect to the mysql database "db_name" with the login
# info supplied above.  If it succeeds, it prints out the version number of 
# mysql, if it fails, it exits gracefully.

# create an engine for interacting with the MySQL database
try:
    eng_str = 'mysql+mysqldb://' + username + ':' + password + '@localhost/' + db_name
    engine = create_engine(eng_str)
    connection = engine.connect()
    version = connection.execute("SELECT VERSION()")
    print("Database version : ")
    print(version.fetchone())

# report what went wrong if this fails.    
except sqlalchemy.exc.DatabaseError as e:
    reason = e.message
    print("Error %s:" % (reason))
    sys.exit(1)

# close the connection
finally:            
    if connection:    
        connection.close()
    else:
        print("Failed to create connection.")

#%%
        
# This cell first checks to see if the table "matches" already exists.  If it
# does, the code does nothing. If it does not, the code creates it, and then 
# reads in the data from the relevant .csv files, the location of which is 
# specified above.  Note that there are three different match types:  regular 
# matches, qualifying matches, and futures matches. By default, all three get 
# read into the table, though you can edit this behavior in the opening lines.
# by default, all three types of matches will be read into the table "matches"

all_match_types = ['atp_matches','atp_matches_qual_chall','atp_matches_futures']
        
# start a transaction with the database 'tennis'
with engine.begin() as connection:
    # find out if table "matches" exists 
    query="""SELECT count(*) FROM information_schema.TABLES \
            WHERE (TABLE_SCHEMA = '""" + db_name + """') AND (TABLE_NAME = 'matches');
            """
    res = connection.execute(query)

    # if "matches" does not exist, create it
    if (res.fetchone()[0]!=0):
        print("Table MATCHES already exists, skipping table creation.")
    else:
        connection.execute("CREATE TABLE matches (tourney_id VARCHAR(256));")
        connection.execute("ALTER TABLE matches ADD COLUMN tourney_name VARCHAR(256);")
        connection.execute("ALTER TABLE matches ADD COLUMN surface VARCHAR(256);")
        connection.execute("ALTER TABLE matches ADD COLUMN draw_size TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN tourney_level CHAR(1);")
        connection.execute("ALTER TABLE matches ADD COLUMN tourney_date DATE;")
        connection.execute("ALTER TABLE matches ADD COLUMN match_num SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_id MEDIUMINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_seed TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_entry VARCHAR(2);")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_name VARCHAR(256);")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_hand CHAR(1);")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_ht TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_ioc CHAR(3);")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_age DECIMAL(5,3);")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_rank SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN winner_rank_points SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_id MEDIUMINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_seed TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_entry VARCHAR(2);")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_name VARCHAR(256);") 
        connection.execute("ALTER TABLE matches ADD COLUMN loser_hand CHAR(1);")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_ht TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_ioc CHAR(3);")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_age DECIMAL(5,3);")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_rank SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN loser_rank_points SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN score VARCHAR(256);")
        connection.execute("ALTER TABLE matches ADD COLUMN best_of TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN round VARCHAR(4);")
        connection.execute("ALTER TABLE matches ADD COLUMN minutes SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_ace TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_df TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_svpt SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_1stIn TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_1stWon TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_2ndWon TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_SvGms TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_bpSaved TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN w_bpFaced TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_ace TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_df TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_svpt SMALLINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_1stIn TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_1stWon TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_2ndWon TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_SvGms TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_bpSaved TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN l_bpFaced TINYINT UNSIGNED;")
        connection.execute("ALTER TABLE matches ADD COLUMN match_type VARCHAR(256);")

        # loop through all three match types, and store data for each       
        for match_type in all_match_types:
            
            # store names of all .csv files for this match-type
            all_files = glob.glob(atpfile_directory + match_type + "_[0-9]*.csv")

            # load each file, and make sure missing data is stored as NULL
            for file in all_files:
                query = """LOAD DATA LOCAL INFILE '""" + file + \
                """' INTO TABLE matches \
                COLUMNS TERMINATED BY ',' \
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES \
                (@v0, @v1, @v2, @v3, @v4, @v5, @v6, @v7, @v8, @v9, @v10, @v11, @v12, @v13, @v14, @v15, @v16, @v17, @v18, @v19, @v20, @v21, @v22, @v23, @v24, @v25, @v26, @v27, @v28, @v29, @v30, @v31, @v32, @v33, @v34, @v35, @v36, @v37, @v38, @v39, @v40, @v41, @v42, @v43, @v44, @v45, @v46, @v47, @v48) \
                SET \
                tourney_id = nullif(@v0,''), \
                tourney_name = nullif(@v1,''), \
                surface = nullif(@v2,''), \
                draw_size = nullif(@v3,''), \
                tourney_level = nullif(@v4,''), \
                tourney_date = nullif(@v5,''), \
                match_num = nullif(@v6,''), \
                winner_id = nullif(@v7,''), \
                winner_seed = nullif(@v8,''), \
                winner_entry = nullif(@v9,''), \
                winner_name = nullif(@v10,''), \
                winner_hand = nullif(@v11,''), \
                winner_ht = nullif(@v12,''), \
                winner_ioc = nullif(@v13,''), \
                winner_age = nullif(@v14,''), \
                winner_rank = nullif(@v15,''), \
                winner_rank_points = nullif(@v16,''), \
                loser_id = nullif(@v17,''), \
                loser_seed = nullif(@v18,''), \
                loser_entry = nullif(@v19,''), \
                loser_name = nullif(@v20,''), \
                loser_hand = nullif(@v21,''), \
                loser_ht = nullif(@v22,''), \
                loser_ioc = nullif(@v23,''), \
                loser_age = nullif(@v24,''), \
                loser_rank = nullif(@v25,''), \
                loser_rank_points = nullif(@v26,''), \
                score = nullif(@v27,''), \
                best_of = nullif(@v28,''), \
                round = nullif(@v29,''), \
                minutes = nullif(@v30,''), \
                w_ace = nullif(@v31,''), \
                w_df = nullif(@v32,''), \
                w_svpt = nullif(@v33,''), \
                w_1stIn = nullif(@v34,''), \
                w_1stWon = nullif(@v35,''), \
                w_2ndWon = nullif(@v36,''), \
                w_SvGms = nullif(@v37,''), \
                w_bpSaved = nullif(@v38,''), \
                w_bpFaced = nullif(@v39,''), \
                l_ace = nullif(@v40,''), \
                l_df = nullif(@v41,''), \
                l_svpt = nullif(@v42,''), \
                l_1stIn = nullif(@v43,''), \
                l_1stWon = nullif(@v44,''), \
                l_2ndWon = nullif(@v45,''), \
                l_SvGms = nullif(@v46,''), \
                l_bpSaved = nullif(@v47,''), \
                l_bpFaced = nullif(@v48,''), \
                match_type = '""" + match_type + """' \
                ;"""

                connection.execute(query)   

#%%

# This cell checks to see if the table "players" exists.  If it does, the code
# does nothing, if it does not, the code creates it and populates it with the 
# data in the files "players.csv".


# start a transaction with the "tennis" database
with engine.begin() as connection:

    # find out if the table "players" exists
    query="""\
    SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '""" + db_name + """') AND (TABLE_NAME = 'players');\
    """
    res = connection.execute(query)

    # if "players does not exist, create it
    if (res.fetchone()[0]!=0):
        print("Table PLAYERS already exists, skipping table creation.")        
    else:
        connection.execute("CREATE TABLE players \
                     (player_id MEDIUMINT UNSIGNED, \
                     first_name VARCHAR(256), \
                     last_name VARCHAR(256), \
                     hand CHAR(1), \
                     birth_date DATE, \
                     country_code CHAR(3)) \
                     ;") 
        
        # read in the appropriate .csv files
        file = atpfile_directory + "atp_players.csv"
        query =  """LOAD DATA LOCAL INFILE '""" + file + """' INTO TABLE players \
                        COLUMNS TERMINATED BY ',' \
                        LINES TERMINATED BY '\n' \
                        (@v0, @v1, @v2, @v3, @v4, @v5) \
                        SET \
                        player_id = nullif(@v0,''), \
                        first_name = nullif(@v1,''), \
                        last_name = nullif(@v2,''), \
                        hand = nullif(@v3,''), \
                        birth_date = nullif(@v4,''), \
                        country_code = nullif(@v5,'')
                        ;"""
        connection.execute(query)



#%%

# This cell checks to see if the table "rankings" exists.  If it does, the code
# does nothing, if it does not, the code creates it and populates it with the 
# data in the relevant .csv files (see doc-string above for naming conventions.) 

with engine.begin() as connection:
    query="""\
    SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '""" + db_name + """') AND (TABLE_NAME = 'rankings');\
    """
    r1 = connection.execute(query)
    if (r1.fetchone()[0]!=0):
        print("Table RANKINGS already exists, skipping table creation.")
    else:
        connection.execute("CREATE TABLE rankings \
                     (ranking_date DATE, \
                     ranking SMALLINT UNSIGNED, \
                     player_id MEDIUMINT UNSIGNED, \
                     ranking_points SMALLINT UNSIGNED) \
                     ;") 
        # store in a list the names of all the relevant .csv files
        all_files = glob.glob("/Users/ctoews/Projects/Data_Science/Tennis/tennis_atp-master/atp_rankings_*.csv")

        # load each file, and make sure missing data is stored as NULL
        for file in all_files:
            query =  """LOAD DATA LOCAL INFILE '""" + file + """' INTO TABLE rankings \
                        COLUMNS TERMINATED BY ',' \
                        LINES TERMINATED BY '\n' \
                        (@v0, @v1, @v2, @v3, @v4) \
                        SET
                        ranking_date = nullif(@v0,''), \
                        ranking = nullif(@v1,''), \
                        player_id = nullif(@v2,''), \
                        ranking_points = nullif(@v3,'') \
                        ;"""
            connection.execute(query)



