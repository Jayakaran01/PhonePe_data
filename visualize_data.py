import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# 1. Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="phonepe_data",
    user="postgres",
    password="0000",  # Replace with your actual PostgreSQL password
    host="localhost",
    port="5432"
)

# 2. Write the SQL query
query = """
SELECT state, SUM(transaction_amount) AS total_amount
FROM aggregated_transaction
GROUP BY state
ORDER BY total_amount DESC;
"""

# 3. Read data from PostgreSQL into a DataFrame
df = pd.read_sql(query, conn)
conn.close()  # Always close the connection

# 4. Plot the data
plt.figure(figsize=(14, 6))
plt.bar(df['state'], df['total_amount'], color='skyblue')
plt.xticks(rotation=90)
plt.title("💰 Total Transaction Amount by State")
plt.ylabel("Amount (₹)")
plt.xlabel("State")
plt.tight_layout()
plt.show()