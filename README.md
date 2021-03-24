Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening but they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Database

The ETL consist of a database with Star schema with Users (who), Dates (when), Songs and Artists as Dimesions, and Song Plays (measures or records) as Facts. 

Dimension Tables: 

- Users: App users data like free/paid tier, names, etc. This table need to account for changes in user tier across time.
- Artists: Artists data in the subset
- Songs: song data in the subset
- Time: Timestamp records of each played song in the app broken down to multiple time intervals. These timestamps need to be unique.

Fact Tables:
- Song plays: records of songs (platform, location, etc). This data will be updated incrementally using a serial behaviour specified when creating the table. Multiple records per user per song are allowed, as long as they have unique timestamps.

By using this schema it is easier to keep records denormalized which can optimize the simplicity of the queries executed by the analytics team. This schema will not be optimal in some cases as it will require multiple JOIN statements which can be very slow. This simple schema is what the business needs at the moment to start making simple descriptive queries from the messy log files.

## Folder Structure


- `dwh.cfg`: Configuration file with credentials and specifications for cluster creation.
- `sql_queries.py`: Script with all PostgreSQL queries, ready to read.
- `create_tables.py`: Script to create tables
- `etl.py`: ETL jon script
- `cluster.ipynb`: Notebook to create cluster on Redshift and initialize Host and ARN.

## Running the ETL pipeline

Inside your enviroment run the following commands:

- Initialize the cluster on the `cluster.ipynb` notebook and extract the HOST and the ARN into the `dwh.cfg` file.
- `python create_tables.py`. This will create the empty tables and prepare the schema for data insertion. The creation of tables is separately hanlded by on `sql_queries.py`
- `python etl.py`. This script will go through each of the JSON files and insert into the database






## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_Your question_**

You may review any questions on the code and the DB on the Knowledge section:
- https://knowledge.udacity.com/questions/530757
- https://knowledge.udacity.com/questions/530814

