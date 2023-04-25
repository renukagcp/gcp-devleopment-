from google.cloud import bigquery python,py

client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT * FROM "gcp-renuka.renuka_python.test_table"
    'WHERE age > 30'
    )
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)
