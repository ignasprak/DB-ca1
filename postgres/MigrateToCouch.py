import couchdb
import pandas as pd
import requests
import uuid
import json
from collections import defaultdict
from sqlalchemy import create_engine

# PostgreSQL connection parameters
db_config = {
    'dbname': "postgres",
    'user': "ignasprak@gmail.com",
    'password': "53174",
    'host': "localhost",
    'port': "5433"
}

# CouchDB connection parameters
couchdb_url = "http://localhost:5984/"
couchdb_user = "admin"
couchdb_password = "53174"
db_name = "c20424992"

# Function to create and save documents in CouchDB
def create_document(couch_db, doc_id, doc_type, data):
    doc = {"_id": doc_id, "type": doc_type, "data": data}
    try:
        couch_db.save(doc)
    except couchdb.http.ResourceConflict:
        print(f"Document with ID {doc_id} already exists. Skipping.")
    return doc

# Connect to CouchDB
couch = couchdb.Server(f"http://{couchdb_user}:{couchdb_password}@localhost:5984/")
if db_name in couch:
    del couch[db_name]
response = requests.put(f"http://{couchdb_user}:{couchdb_password}@localhost:5984/{db_name}?partitioned=true")
if response.status_code in (201, 202):
    print("Partitioned database created successfully.")
else:
    print(f"Database creation failed: {response.text}")
    exit()
couch_db = couch[db_name]

# Connect to PostgreSQL
conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(conn_string)

# Extract data for 2021 and 2023 editions
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
result_data_df = pd.read_sql(query, engine)

# Generate and save documents
documents_to_save = []
fact_docs = defaultdict(lambda: {"votes": []})  # One fact doc per participant per year

for _, row in result_data_df.iterrows():
    edition_year = row["editionyear"]
    participant_name = row["participantname"]
    vote_mode = row["votemode"]
    total_votes = row["total_votes"]

    # Create vote document
    vote_doc_id = f"{edition_year}:vote:{participant_name}:{vote_mode[0].upper()}"
    vote_doc = {
        "edition_year": edition_year,
        "participant_name": participant_name,
        "vote_mode": vote_mode,
        "total_votes": total_votes
    }
    create_document(couch_db, vote_doc_id, "vote", vote_doc)
    documents_to_save.append(vote_doc)

    # Add unique vote document ID to the corresponding fact document
    fact_doc_id = f"{edition_year}:fact:{participant_name}"
    fact_docs[fact_doc_id]["edition_year"] = edition_year
    fact_docs[fact_doc_id]["participant_name"] = participant_name
    if vote_doc_id not in fact_docs[fact_doc_id]["votes"]:
        fact_docs[fact_doc_id]["votes"].append(vote_doc_id)

# Save fact documents
for fact_doc_id, fact_doc in fact_docs.items():
    create_document(couch_db, fact_doc_id, "fact", fact_doc)
    documents_to_save.append({"_id": fact_doc_id, "type": "fact", "data": fact_doc})

# Save documents to JSON file
with open("couchdb_documents.json", "w") as json_file:
    json.dump(documents_to_save, json_file, indent=4)

print(f"Migration completed: {len(fact_docs)} fact documents and {len(documents_to_save) - len(fact_docs)} vote documents created.")
