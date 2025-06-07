import os
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="phonepe_data",
    user="postgres",
    password="0000",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

folder_path = "pulse/data/aggregated/transaction/country/india/state/"
states = os.listdir(folder_path)

for state in states:
    state_path = os.path.join(folder_path, state)
    for year in os.listdir(state_path):
        for file in os.listdir(os.path.join(state_path, year)):
            with open(os.path.join(state_path, year, file)) as f:
                data = json.load(f)
                for trans in data["data"]["transactionData"]:
                    transaction_type = trans["name"]
                    count = trans["paymentInstruments"][0]["count"]
                    amount = trans["paymentInstruments"][0]["amount"]
                    quarter = int(file.strip(".json"))
                    cursor.execute("""
                        INSERT INTO aggregated_transaction
                        (state, year, quarter, transaction_type, transaction_count, transaction_amount)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (state, int(year), quarter, transaction_type, count, amount))

conn.commit()
cursor.close()
conn.close()
print("✅ Data inserted successfully!")