import requests
import json

# CouchDB connection ==
couchdb_url = "http://localhost:5984/"
couchdb_user = "admin"
couchdb_password = "password"
db_name = "c20424992"

# Query for fetching the  fact documents
url = f"{couchdb_url}{db_name}/_find"
headers = {"Content-Type": "application/json"}
query_payload = {
    "selector": {
        "type": "fact"
    },
    "fields": ["_id", "data.participant_name", "data.edition_year", "data.votes"]
}

# Execute the query
response = requests.post(url, headers=headers, json=query_payload, auth=(couchdb_user, couchdb_password))

# Error handling
if response.status_code == 200:
    results = response.json()
    print("Global query results:")
    print(json.dumps(results, indent=4))
else:
    print(f"Failed to execute global query. Status code: {response.status_code}")
    print(response.text)
