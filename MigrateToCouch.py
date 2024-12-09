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

# Voting charges
voting_charges = {
    "2013-2021": {"Facebook": 0.20, "Instagram": 0.20, "Web": 0.50, "Phone": 0.50},
    "2022-2024": {"Facebook": 0.50, "Instagram": 0.50, "Web": 1.00, "Phone": 1.00},
}

# Helper function to calculate income
def calculate_income(edition_year, vote_mode, total_votes):
    charges = voting_charges["2013-2021"] if edition_year <= 2021 else voting_charges["2022-2024"]
    return charges[vote_mode] * total_votes

# Function to create and save documents in CouchDB
def create_document(couch_db, doc_id, doc_type, data):
    doc = {"_id": doc_id, "type": doc_type, "data": data}
    couch_db.save(doc)
    return doc

# Check if CouchDB is up and running
while True:
    user_input = input("Is CouchDB up and running? (yes/no): ").strip().lower()
    if user_input in ('yes', 'y'):
        try:
            response = requests.get(f"http://{couchdb_user}:{couchdb_password}@localhost:5984/")
            if response.status_code == 200:
                print("CouchDB is up and running!")
                break
        except requests.ConnectionError:
            print("CouchDB is not reachable. Ensure it's running.")
        else:
            print("Unexpected status code from CouchDB:", response.status_code)
    else:
        print("Migration cancelled.")
        exit()

# Connect to CouchDB
couch = couchdb.Server(f"http://{couchdb_user}:{couchdb_password}@localhost:5984/")
if db_name in couch:
    del couch[db_name]  # Delete the database if it exists
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
    v.totalvotes
FROM
    musiccompdimdb_c20424992.votesfact v
JOIN
    musiccompdimdb_c20424992.participantdimension p ON v.participantname = p.participantname
WHERE
    v.editionyear IN (2021, 2023);
"""
result_data_df = pd.read_sql(query, engine)

# Debugging
print("Columns in the DataFrame:", result_data_df.columns)
print("DataFrame shape:", result_data_df.shape)
print(result_data_df.head())

for _, row in result_data_df.iterrows():
    print(row) 
    edition_year = row["editionyear"] 


# Generate and save documents
documents_to_save = []
fact_docs = defaultdict(lambda: {"votes": []})  

for _, row in result_data_df.iterrows():
    edition_year = row["editionyear"]
    participant_name = row["participantname"]
    vote_mode = row["votemode"]
    total_votes = row["totalvotes"]

    # Calculate income
    income = calculate_income(edition_year, vote_mode, total_votes)

    # Create vote document
    vote_doc_id = f"{edition_year}::{participant_name}::{vote_mode[0].upper()}-{uuid.uuid4()}"
    vote_doc = {
        "_id": vote_doc_id,
        "type": "vote",
        "data": {
            "edition_year": edition_year,
            "participant_name": participant_name,
            "vote_mode": vote_mode,
            "total_votes": total_votes,
            "income_earned": income
        }
    }
    create_document(couch_db, vote_doc_id, "vote", vote_doc["data"])
    documents_to_save.append(vote_doc)

    # Add vote document ID to the corresponding fact document
    fact_doc_id = f"{edition_year}::{participant_name}"
    fact_docs[fact_doc_id]["edition_year"] = edition_year
    fact_docs[fact_doc_id]["participant_name"] = participant_name
    fact_docs[fact_doc_id]["votes"].append(vote_doc_id)

# Save fact documents
for fact_doc_id, fact_doc in fact_docs.items():
    fact_doc_data = {
        "_id": fact_doc_id,
        "type": "fact",
        "data": fact_doc
    }
    create_document(couch_db, fact_doc_id, "fact", fact_doc)
    documents_to_save.append(fact_doc_data)

# Save documents to JSON file for bulk import
with open("couchdb_documents.json", "w") as json_file:
    json.dump(documents_to_save, json_file, indent=4)

print("Data migration completed. Documents saved to CouchDB and couchdb_documents.json.")
