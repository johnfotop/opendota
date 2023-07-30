import sqlite3
import requests
import time

conn = sqlite3.connect('dota.sqlite')
cur = conn.cursor()

heroes_call = 'https://api.opendota.com/api/heroes'

# get hero id - name correlation from the API
try:
    response = requests.get(heroes_call)
    response.raise_for_status()
    heroes = response.json()

    cur.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Heronames'")
    # count : how many rows in sqlite_master table match the condition
    table_exists = cur.fetchone()[0]
    if table_exists:
        print('Table \'Heronames\' already exists. Insert statements will not be executed.')
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS Heronames (
            Hero_id PRIMARY KEY, 
            Hero_name TEXT
        )''')

        for list_element in heroes:
            hero_id = list_element['id']
            hero_name = list_element['localized_name']
            cur.execute('''INSERT INTO Heronames (Hero_id, Hero_name)
                VALUES (?, ?)''', (hero_id, hero_name))
        conn.commit()
        print("Data inserted successfully in 'Heronames' table!")
except requests.exceptions.RequestException as sql_error:
    print("An error occurred while making the HTTP request:", sql_error)
except sqlite3.Error as sql_error:
    print("An error occurred while interacting with the database:", sql_error)

print("Moving on to all matches call for retrieving all turbo match IDs")

all_matches = 'https://api.opendota.com/api/players/197497174/matches?limit=10&game_mode=23&significant=0'

try:
    response = requests.get(all_matches)
    response.raise_for_status()
    matches = response.json()
    cur.execute('''CREATE TABLE IF NOT EXISTS Matches (
        Match_id PRIMARY KEY, 
        Game_mode INTEGER,
        Radiant_score INTEGER, 
        Dire_score INTEGER, 
        Radiant_win INTEGER    
    )''')

    for list_element in matches:
        match_id = list_element['match_id']
        game_mode = list_element['game_mode']
        cur.execute('''INSERT OR IGNORE INTO Matches (Match_id, Game_mode)
        VALUES (?, ?)''', (match_id, game_mode))
    conn.commit()
    print("Data from 2nd call inserted (or ignored :) successfully in 'Matches' table!")
except requests.exceptions.RequestException as sql_error:
    print("An error occurred while making the HTTP request:", sql_error)
except sqlite3.Error as sql_error:
    print("An error occurred while interacting with the database:", sql_error)

print("Moving on to individual matches call for retrieving individual match details from Match IDs")

baseurl = 'https://api.opendota.com/api/matches/'

# making the final call url
cur.execute("SELECT Match_id FROM Matches ORDER BY Match_id DESC")

# Fetch all the rows returned by the query (as a list of tuples)
matchids = cur.fetchall()

#counter to follow how many matches we have retrieved
count = 0

hero_id = None  # we used this variable in the first table
sql_error = None  # we use e as success  flag
for matchid in matchids:
    indiv_matches_call = baseurl + str(matchid[0])
    try:
        response = requests.get(indiv_matches_call)
        response.raise_for_status()
        details = response.json()

        cur.execute('''CREATE TABLE IF NOT EXISTS Players (
            Account_id PRIMARY KEY      
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Roster (
            Match_id     INTEGER,
            Account_id   INTEGER,
            Hero_id      INTEGER,
            Player_slot  INTEGER,
            PRIMARY KEY (Match_id, Account_id, Hero_id )   
        )''')

        radiant_score = details['radiant_score']
        dire_score = details['dire_score']
        radiant_win = details['radiant_win']
        players_data = details['players']
        for player in players_data:
            account_id = player['account_id']
            hero_id = player['hero_id']
            player_slot = player['player_slot']
            cur.execute('''INSERT OR IGNORE INTO Players (Account_id)
                VALUES (?)''', (account_id,))
            cur.execute('''INSERT OR IGNORE INTO Roster (Match_id, Account_id, Hero_id, Player_slot )
                       VALUES (?,?,?,?)''', (matchid[0],account_id,hero_id,player_slot))

        cur.execute('''UPDATE Matches 
                    SET Radiant_score = ?, Dire_score = ?, Radiant_win = ?
                    WHERE Match_id = ?''', (radiant_score, dire_score, radiant_win, matchid[0]))

        conn.commit()
        time.sleep(1)
        count = count + 1
        print('Retrieved game number: ', count)

    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break

    except requests.exceptions.RequestException as sql_error:
        print("An error occurred while making the HTTP request:", sql_error)
    except sqlite3.Error as sql_error:
        print("An error occurred while interacting with the database:", sql_error)
if sql_error is None:
    print("Data inserted (or ignored :) successfully in 'Picks' table!")
    print("Data inserted (or ignored :) successfully in 'Players' table!")

cur.close()
conn.close()


# CHANGES:
# Adopted another schema for better and easier querying :
# 1) changed "Players" table to just include 1 column with account_ids (no pick slots)
# 2) reworked "Picks" table into "Roster" table so that each row represents one player slot of a specific match_id
# 3) removed heroes list at last loop
# 4) switched Insert statement for Roster table into INSERT OR IGNORE so as to avoid duplicating errors on
# program rerun.
# 5) implemented time.sleep(1) function so as to avoid flooding the API and keyboardinterrupt exceptions.
# 6) Implemented counter and print message to follow which Match we are currently commiting


# THINGS TO DO:
# 1) decide between INSERT OR INGORE and INSERT OR REPLACE statements depending on which is more efficient.
# 2) might remove Account_id from PK of Roster table due to some account having Null values
# 3) fix keyboardinterrupt not working properly
# 4) use last Match_id fetched to resume retrieval from new entries
