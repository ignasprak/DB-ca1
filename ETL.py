import couchdb
import psycopg2
import uuid 

# PostgreSQL details
pg_conn_params = {
    "dbname": "musiccompdimdb_c20424992",
    "user": "c20424992@mytudublin.ie ",
    "password": "x",
    "host": "localhost",
    "port": 5432,
}

# CouchDB connection details
couchdb_url = "http://localhost:5984/"
couchdb_db_name = "c20424992"
couchdb_user = "admin"
couchdb_password = "x"

# Voting charges
voting_charges = {
    "2013-2021": {"Facebook": 0.20, "Instagram": 0.20, "Web": 0.50, "Phone": 0.50},
    "2022-2024": {"Facebook": 0.50, "Instagram": 0.50, "Web": 1.00, "Phone": 1.00},
}

# Connect to PostgreSQL
pg_conn = psycopg2.connect(**pg_conn_params)
pg_cursor = pg_conn.cursor()

# Connect to CouchDB
couch = couchdb.Server(couchdb_url)
couch.resource.credentials = (couchdb_user, couchdb_password)

# Create CouchDB database
if couchdb_db_name in couch:
    db = couch[couchdb_db_name]
else:
    db = couch.create(couchdb_db_name)

# Ensure partitioning is enabled
db.resource.put("_partitioned", {"enabled": True})

# Helper function to calculate income
def calculate_income(edition_year, vote_mode, total_votes):
    charges = voting_charges["2013-2021"] if edition_year <= 2021 else voting_charges["2022-2024"]
    return charges[vote_mode] * total_votes

# ETL process
for year in [2021, 2023]:
    # Query data from PostgreSQL
    pg_cursor.execute("""
        SELECT 
            v.edition_year, 
            p.participantname, 
            v.votemode, 
            v.totalvotes
        FROM musiccompdimdb_c20424992.votesfact v
        JOIN musiccompdimdb_c20424992.participantdimension p ON v.participantname = p.participantname
        WHERE v.edition_year = %s
    """, (year,))
    
    results = pg_cursor.fetchall()

    # Organize data by participant
    participant_data = {}
    for edition_year, participant_name, vote_mode, total_votes in results:
        income_earned = calculate_income(edition_year, vote_mode, total_votes)
        
        # Generate unique ID for each vote document
        unique_id = str(uuid.uuid4())  # Add a unique identifier

        # Create vote document
        vote_doc_id = f"{edition_year}::{participant_name}::{vote_mode[0].upper()}-{unique_id}"
        vote_doc = {
            "_id": vote_doc_id,
            "type": "vote",
            "data": {
                "vote_mode": vote_mode,
                "total_votes": total_votes,
                "income_earned": income_earned
            }
        }
        db.save(vote_doc)
        
        # Add vote document to participant's list
        if participant_name not in participant_data:
            participant_data[participant_name] = {
                "_id": f"{edition_year}::{participant_name}",
                "type": "fact",
                "data": {
                    "pname": participant_name,
                    "year": edition_year,
                    "votes": []
                }
            }
        participant_data[participant_name]["data"]["votes"].append(vote_doc_id)
    
    # Save fact documents
    for fact_doc in participant_data.values():
        db.save(fact_doc)

# Close PostgreSQL connection
pg_cursor.close()
pg_conn.close()

print(f"ETL process completed for CouchDB database '{couchdb_db_name}'")
