# Do pip install cassandra-driver
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from sqlalchemy import create_engine
import pandas as pd

# Postgres' connection parameters
db_config = {
    'dbname': "postgres",
    'user': "ignasprak@gmail.com",
    'password': "password",
    'host': "localhost",
    'port': "5433"
}

# Postgres connection
conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(conn_string)

# 2021,2023 edition data query
query = """
SELECT
    v.editionyear,
    p.participantname,
    v.votemode,
    SUM(v.totalvotes) AS total_votes
FROM
    musiccompdimdb_c20424992.votesfact v
JOIN
    musiccompdimdb_c20424992.participantdimension p ON v.participantname = p.participantname
WHERE
    v.editionyear IN (2021, 2023)
GROUP BY
    v.editionyear, p.participantname, v.votemode;
"""

# Put data into DataFrame
votes_df = pd.read_sql(query, engine)

# Process data into a dictionary
votes_by_participant = {}
for _, row in votes_df.iterrows():
    year = row["editionyear"]
    participant = row["participantname"]
    vote_mode = row["votemode"]
    total_votes = row["total_votes"]
    
    if (year, participant) not in votes_by_participant:
        votes_by_participant[(year, participant)] = {}
    votes_by_participant[(year, participant)][vote_mode] = total_votes

# Connect to Cassandra
cluster = Cluster(["localhost"], port=9042)  

# Use  keyspace
session.set_keyspace("c20424992")

# Insert data into the table
insert_query = """
INSERT INTO votes_by_participant (edition_year, participant_name, votes)
VALUES (%s, %s, %s)
"""
batch = BatchStatement()

for (year, participant), votes in votes_by_participant.items():
    session.execute(insert_query, (year, participant, votes))

print("Data migration to Cassandra done")


