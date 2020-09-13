# Project: Data Modeling with Postgres

---

## Summary

---

This project has been processed in Sparkify.
Sparkify launched their new music streaming app a year ago, and they wanted to analyze their user activity. Since the log data was in JSON format, it was hard to query information effectively.
As a Data Engineer in Sparkify, our team worked on transferring the data from JSON files in Postgres using Python and SQL. On this project, We created a database schema and ETL pipeline, and then tested the database so that everything was working properly. As a result of this project, the data analysists were able to query the data easily and analyze data effectively.


## How to run the Python scripts

---

In order to run the Python scripts, first you must run the create_tables python file. Then run the cells in etl.ipynb to read a single file from song_data and log_data and load the data into the tables. To check if it worked well, open the test.ipynb and run the cells. Once you are done, run etl.py in your console to load all of the log data into the tables. Finally, try testing the results with test notebook file.  


## Explanation of the files

---

#### sql_queries.py

This python file contains a list of queries of dropping tables and creating tables.
It also contains some insert queries to insert data later by running the etl.py file. 


#### create_tables.py

This file contains several functions that creates the database and drops and creates the table.
It is used to reset the databse so you must run this file before running other files.

#### etl.ipynb

This file is for developing the ETL process before completing the etl.py.
It performs ETL on a single file and first line of the data for practice.

#### etl.py

This file performs the full ETL process. It is a combination of all code from etl notebook but the difference is it inserts the whole data to all of the files. 

#### test.ipynb

This file is used for testing purposes. It can be used after running the cells of etl notebook or after running the etl.py. 


