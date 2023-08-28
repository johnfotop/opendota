```
   ____                   ____        __       
  / __ \____  ___  ____  / __ \____  / /_____ _
 / / / / __ \/ _ \/ __ \/ / / / __ \/ __/ __ `/
/ /_/ / /_/ /  __/ / / / /_/ / /_/ / /_/ /_/ / 
\____/ .___/\___/_/ /_/_____/\____/\__/\__,_/  
    /_/                                        
```

# OpenDota API tool

This tool calls the Open Dota API and collects data for past games.

# Work in progress

## TODO

1. Decide between INSERT OR INGORE and INSERT OR REPLACE statements depending on which is more efficient.
2. Might remove Account_id from PK of Roster table due to some account having Null values.
3. Fix keyboardinterrupt not working properly.
4. Use last Match_id fetched to resume retrieval from new entries.
