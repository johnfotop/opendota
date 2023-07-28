import sqlite3
import requests

conn = sqlite3.connect('../dota.sqlite')
cur = conn.cursor()

heroes_call = 'https://api.opendota.com/api/heroes'

# get hero id - name correlation from the API
try:
    response = requests.get(heroes_call)
    response.raise_for_status()
    heroes = response.json()

    cur.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Heronames'")
    table_exists = cur.fetchone()[0]  # if table_exists: is true for table_exists /= 0 and false for table_exists = 0
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


all_matches = 'https://api.opendota.com/api/players/197497174/matches?limit=30&game_mode=23&significant=0'

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
    print("Data inserted (or ignored :) successfully in 'Matches' table!")
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

hero_id = None  # we used this variable in the first table
sql_error = None        # success flag
for matchid in matchids:
    indiv_matches_cal = baseurl + str(matchid[0])
    try:
        response = requests.get(indiv_matches_cal)
        response.raise_for_status()
        details = response.json()

        cur.execute('''CREATE TABLE IF NOT EXISTS Picks (
            Match_id PRIMARY KEY,
            Pick_slot_1 INTEGER,
            Pick_slot_2 INTEGER,
            Pick_slot_3 INTEGER,
            Pick_slot_4 INTEGER,
            Pick_slot_5 INTEGER,
            Pick_slot_6 INTEGER,
            Pick_slot_7 INTEGER,
            Pick_slot_8 INTEGER,
            Pick_slot_9 INTEGER,
            Pick_slot_10 INTEGER
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS Players (
            Match_id PRIMARY KEY,
            Player_slot_1 INTEGER, 
            Player_slot_2 INTEGER, 
            Player_slot_3 INTEGER, 
            Player_slot_4 INTEGER, 
            Player_slot_5 INTEGER, 
            Player_slot_6 INTEGER, 
            Player_slot_7 INTEGER, 
            Player_slot_8 INTEGER, 
            Player_slot_9 INTEGER, 
            Player_slot_10 INTEGER          
        )''')
        radiant_score = details['radiant_score']
        dire_score = details['dire_score']
        radiant_win = details['radiant_win']
        players_data = details['players']
        accounts = []
        heroes = []
        for player in players_data:
            account_id = player['account_id']
            hero_id = player['hero_id']
            player_slot = player['player_slot']
            accounts.append(account_id)
            heroes.append(hero_id)

        cur.execute('''UPDATE Matches 
                    SET Radiant_score = ?, Dire_score = ?, Radiant_win = ?
                    WHERE Match_id = ?''', (radiant_score, dire_score, radiant_win, matchid[0]))

        cur.execute('''INSERT OR IGNORE INTO Picks (Match_id, Pick_slot_1, Pick_slot_2, Pick_slot_3, Pick_slot_4,
            Pick_slot_5, Pick_slot_6, Pick_slot_7, Pick_slot_8, Pick_slot_9, Pick_slot_10)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?)''', (matchid[0], heroes[0], heroes[1], heroes[2], heroes[3], heroes[4], heroes[5], heroes[6], heroes[7],
                    heroes[8], heroes[9]))

        cur.execute('''INSERT OR IGNORE INTO Players (Match_id, Player_slot_1, Player_slot_2, Player_slot_3, 
            Player_slot_4, Player_slot_5, Player_slot_6, Player_slot_7, Player_slot_8, Player_slot_9, Player_slot_10)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?)''', (matchid[0], accounts[0], accounts[1], accounts[2], accounts[3], accounts[4], accounts[5],
                    accounts[6],  accounts[7], accounts[8], accounts[9]))

        conn.commit()

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
# 1) changed table names (for e.g. Match_IDs to Matches, Game_Details to Picks, Player_details to Players)
# 2) removed counts from last loops
# 3) changed columns first letters to Uppercase
# 4) reworked database schema and removed player_slot_i columns and relevant variables
# 5) used an Update statement (instead of insert) to fix faulty data insertion in the last 3 columns of the "Matches"
# table
# 6) changed some variable names to be more explanatory (like call1 , call2 , call3 and error variables)
# 7) removed unnecessary for loop to read json 1st level dictionaries


# THINGS TO DO:
# 1) put sleep time to not overwhelm API
# 2) change database model because of bad querying functionality
# 3)
