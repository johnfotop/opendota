import sqlite3
import requests

conn = sqlite3.connect('../dota.sqlite')
cur = conn.cursor()

call1 = 'https://api.opendota.com/api/heroes'

# get hero id - name correlation from the API
try:
    response = requests.get(call1)
    response.raise_for_status()
    heroes = response.json()

    cur.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Heronames'")  # count : how many rows in sqlite_master table match the condition
    table_exists = cur.fetchone()[0]  # if table_exists: is true for table_exists /= 0 and false for table_exists = 0
    if table_exists:
        print('Table \'Heronames\' already exists. Insert statements will not be executed.')
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS Heronames (
            hero_id PRIMARY KEY, 
            hero_name TEXT
        )''')

        for list_element in heroes:
            hero_id = list_element['id']
            hero_name = list_element['localized_name']
            cur.execute('''INSERT INTO Heronames (hero_id, hero_name)
                VALUES (?, ?)''', (hero_id, hero_name))
        conn.commit()
        print("Data inserted successfully in 'Heronames' table!")
except requests.exceptions.RequestException as e:
    print("An error occurred while making the HTTP request:", e)
except sqlite3.Error as e:
    print("An error occurred while interacting with the database:", e)

print("Moving on to call2 for retrieving all turbo match IDs")

# you can change the limit argument in the call bellow to make it faster. with 50 games it will take about 30 seconds to run
call2 = 'https://api.opendota.com/api/players/197497174/matches?limit=50&game_mode=23&significant=0'

try:
    response = requests.get(call2)
    response.raise_for_status()
    matches = response.json()
    cur.execute('''CREATE TABLE IF NOT EXISTS Match_IDs (
        match_id PRIMARY KEY, 
        game_mode INTEGER
    )''')

    for list_element in matches:
        match_id = list_element['match_id']
        game_mode = list_element['game_mode']
        cur.execute('''INSERT OR IGNORE INTO Match_IDs (match_id, game_mode)
        VALUES (?, ?)''', (match_id, game_mode))
    conn.commit()
    print("Data inserted (or ignored :) successfully in 'Match_IDs' table!")
except requests.exceptions.RequestException as e:
    print("An error occurred while making the HTTP request:", e)
except sqlite3.Error as e:
    print("An error occurred while interacting with the database:", e)

print("Moving on to call3 for retrieving individual match details from match IDs")

baseurl = 'https://api.opendota.com/api/matches/'

# making the final call url
cur.execute("SELECT match_id FROM Match_IDs ORDER BY match_id DESC")

# Fetch all the rows returned by the query (as a list of tuples)
matchids = cur.fetchall()

hero_id = None # we used this variable in the first table
# e as success  flag
e = None
for matchid in matchids:
    call3 = baseurl + str(matchid[0])
    try:
        response = requests.get(call3)
        response.raise_for_status()
        details = response.json()

        cur.execute('''CREATE TABLE IF NOT EXISTS Game_Details (
            id INTEGER PRIMARY KEY , 
            match_id INTEGER UNIQUE, 
            radiant_score INTEGER, 
            dire_score INTEGER, 
            radiant_win INTEGER
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS Players (
            id INTEGER PRIMARY KEY, 
            match_id INTEGER UNIQUE, 
            player_slot_1 INTEGER, 
            account_id_1 INTEGER, 
            hero_id_1 INTEGER,
            player_slot_2 INTEGER, 
            account_id_2 INTEGER, 
            hero_id_2 INTEGER,
            player_slot_3 INTEGER, 
            account_id_3 INTEGER, 
            hero_id_3 INTEGER,
            player_slot_4 INTEGER, 
            account_id_4 INTEGER, 
            hero_id_4 INTEGER,
            player_slot_5 INTEGER, 
            account_id_5 INTEGER, 
            hero_id_5 INTEGER,
            player_slot_6 INTEGER, 
            account_id_6 INTEGER, 
            hero_id_6 INTEGER,
            player_slot_7 INTEGER, 
            account_id_7 INTEGER, 
            hero_id_7 INTEGER,
            player_slot_8 INTEGER, 
            account_id_8 INTEGER, 
            hero_id_8 INTEGER, 
            player_slot_9 INTEGER, 
            account_id_9 INTEGER, 
            hero_id_9 INTEGER,
            player_slot_10 INTEGER, 
            account_id_10 INTEGER, 
            hero_id_10 INTEGER           
        )''')
        for key in details:
            radiant_score = details['radiant_score']
            dire_score = details['dire_score']
            radiant_win = details['radiant_win']
            players_data = details['players']
            count = 0
            accounts = []
            heroes = []
            slots = []
            for player in players_data:
                account_id = player['account_id']
                hero_id = player['hero_id']
                player_slot = player['player_slot']
                count = count + 1
                accounts.append(account_id)
                heroes.append(hero_id)
                slots.append(player_slot)

        cur.execute('''INSERT OR IGNORE INTO Players (match_id, account_id_1, hero_id_1, player_slot_1, account_id_2, hero_id_2,
            player_slot_2, account_id_3, hero_id_3, player_slot_3, account_id_4, hero_id_4, player_slot_4, account_id_5, hero_id_5,
            player_slot_5, account_id_6, hero_id_6, player_slot_6, account_id_7, hero_id_7, player_slot_7, account_id_8, hero_id_8,
            player_slot_8, account_id_9, hero_id_9, player_slot_9, account_id_10, hero_id_10, player_slot_10)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?)''', (matchid[0],  accounts[0], heroes[0], slots[0], accounts[1], heroes[1], slots[1], \
                        accounts[2], heroes[2], slots[2], accounts[3], heroes[3], slots[3], \
                        accounts[4], heroes[4], slots[4], accounts[5], heroes[5], slots[5], \
                        accounts[6], heroes[6], slots[6], accounts[7], heroes[7], slots[7], \
                        accounts[8], heroes[8], slots[8], accounts[9], heroes[9], slots[9]))

        cur.execute('''INSERT OR IGNORE INTO Game_Details (match_id, radiant_score, dire_score, radiant_win)
            VALUES (?, ?, ?, ?)''', (matchid[0], radiant_score, dire_score, radiant_win))

        conn.commit()

    except requests.exceptions.RequestException as e:
        print("An error occurred while making the HTTP request:", e)
    except sqlite3.Error as e:
        print("An error occurred while interacting with the database:", e)
if e is None:
    print("Data inserted (or ignored :) successfully in 'Game_Details' table!")
    print("Data inserted (or ignored :) successfully in 'Player_Details' table!")


cur.close()
conn.close()

# THINGS TO DO:
# 1) somehow make CREATE and INSERT statements easier for Players table
# 2) Decide between INSERT OR IGNORE VS INSERT OR REPLACE STATEMENTS
# 3) write some retrieval scripts
