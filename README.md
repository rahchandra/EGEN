# EGEN
ETL Data Challenge

## Decription:
An ETL process to load the covid information for each county in New York from API into in Memory database. Implementation done using python3, sqlite database, multithreading concepts.

## Files:
  1. ETL_scheduler.py
  2. ETLdriver.py
  3. SQLLiteDatabase.py
  
## How to execute:
  Start : nohup python ETL_scheduler.py > output.log &  
  Stop  : ps -ef | grep 'python ETL_scheduler.py' | grep -v grep | awk '{print $2}' | xargs kill
  
## Overview of the code:
  1  : The ETL process is initiated in the background using 'nohup &' command.   
  2  : ETL_scheduler.py starts and the scheduler is set to run everyday at 9AM. There is a flag 'isHistoric' here which indicates that if this code will load all historic data or do an incremental load by just adding the last day data.    
  3  : ETLdriver.py code is initiated in the scheduler.  
  4  : ETLdriver.py fetches the data from API and creates county wise thread objects to load data in SQLite DB, using DB object referencing to class in SQLLiteDatabase.py.    
  5  : In SQLLiteDatabase.py an SQLite DB class is present which handles all the DB releated calls like - create, insert and select.    
       
