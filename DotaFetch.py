# What this program does:
##
#  1) Creates the heros ids - names table. Also doesn't recreate it if it already there next time.
# for that we use the heroes call1 = "https://api.opendota.com/api/heroes"
#
#  2) Creates a table with match IDs of matches played.
# For that we use the matches call2 = 'https://api.opendota.com/api/players/197497174/matches?limit=20&game_mode=23&significant=0'

#
# 3) individual games data we get from the matches call:
#call3: https://api.opendota.com/api/matches/4996693630
#
#
#4) puts everything in database
#
import sqlite3
import requests

conn = sqlite3.connect('../dota.sqlite')   #create a connect object to the sqlite database
cur = conn.cursor()                        #create a cursor object. its like flie handle but for databases

call1 = 'https://api.opendota.com/api/heroes'

#get hero id - name correlation from the API
try:
    response = requests.get(call1)
    response.raise_for_status()
    heroes = response.json()

    cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Heronames'")
    table_exists = cur.fetchone()[0]
    if table_exists:
        print("Table 'Heronames' already exists. Insert statements will not be executed.")
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS Heronames
                    (hero_id PRIMARY KEY, hero_name TEXT)''')

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

call2 = 'https://api.opendota.com/api/players/197497174/matches?limit=20&game_mode=23&significant=0'

try:
    response = requests.get(call2)
    response.raise_for_status()
    matches = response.json()

    cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Match_IDs'")
    table_exists = cur.fetchone()[0]
    if table_exists:
        print('Table \'Match_IDs\' already exists. Insert statements will not be executed.')
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS Match_IDs
                    (match_id PRIMARY KEY, game_mode INTEGER)''')
        for list_element in matches:
            match_id = list_element['match_id']
            game_mode = list_element['game_mode']
            cur.execute('''INSERT INTO Match_IDs (match_id, game_mode)
            VALUES (?, ?)''', (match_id, game_mode))
        conn.commit()
        print("Data inserted successfully in 'Match_IDs' table!")
except requests.exceptions.RequestException as e:
    print("An error occurred while making the HTTP request:", e)
except sqlite3.Error as e:
    print("An error occurred while interacting with the database:", e)

print("Moving on to call3 for retrieving individual match details from match IDs")


cur.execute("SELECT * FROM Match_IDs ORDER BY match_id")

row = cur.fetchall()[0]
num = row[0]
print(num)

baseurl = 'https://api.opendota.com/api/matches/'
call3 = baseurl + str(num)


try:
    response = requests.get(call3)
    response.raise_for_status()
    details = response.json()

    cur.execute('''CREATE TABLE IF NOT EXISTS Game_Details
            (id PRIMARY KEY, match_id INTEGER, radiant_score INTEGER, dire_score INTEGER, radiant_win INTEGER)''')
    # cur.execute('''CREATE TABLE IF NOT EXISTS Player_Details
    #             (id PRIMARY KEY, match_id INTEGER, player_slot INTEGER, account_id INTEGER, hero_id INTEGER)''')
    for key in details:
        # details.values():
        match_id = details['match_id']
        radiant_score = details['radiant_score']
        dire_score = details['dire_score']
        radiant_win = details['radiant_win']
        cur.execute('''INSERT INTO Game_Details (match_id, radiant_score, dire_score, radiant_win)
        VALUES (?, ?, ?, ?)''', (match_id, radiant_score, dire_score, radiant_win))
    conn.commit()
    print("Data inserted successfully in 'Game_Details' table!")
    print("Data inserted successfully in 'Player_Details' table!")
except requests.exceptions.RequestException as e:
    print("An error occurred while making the HTTP request:", e)
except sqlite3.Error as e:
    print("An error occurred while interacting with the database:", e)

cur.close()
conn.close()

# THINGS TO DO:
# 1) second call should check if there are new Match_IDs and update table with them
# Check column number
# cur.execute(f"PRAGMA table_info({'Matches'})")
# rows = cur.fetchall()
# num_columns = len(rows)
# print("Number of columns in the table:", num_columns)


# 2) check to see how i can read match ids from old to new and vise versa
# 3) check how to iterate match ids so as to incrementaly get call 3




