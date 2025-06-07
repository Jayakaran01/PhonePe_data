import psycopg2
from psycopg2 import sql

# Database config
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "0000"
DB_NAME = "phonepe_data"  # Target DB to create

# Step 1: Connect to default 'postgres' DB to create a new DB
def create_database():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Check if DB already exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (DB_NAME,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            print(f"✅ Database '{DB_NAME}' created successfully.")
        else:
            print(f"ℹ️ Database '{DB_NAME}' already exists.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error creating database: {e}")

# Step 2: Connect to the new DB and create tables
def create_tables():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # SQL script to create all tables
        create_script = """
        CREATE TABLE IF NOT EXISTS aggregated_transaction (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            transaction_type TEXT,
            transaction_count BIGINT,
            transaction_amount DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS aggregated_user (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            brand TEXT,
            user_count BIGINT,
            percentage DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS aggregated_insurance (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            insurance_type TEXT,
            insurance_count BIGINT,
            insurance_amount DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS map_transaction (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            district TEXT,
            transaction_count BIGINT,
            transaction_amount DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS map_user (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            district TEXT,
            registered_users BIGINT,
            app_opens BIGINT
        );

        CREATE TABLE IF NOT EXISTS map_insurance (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            district TEXT,
            insurance_count BIGINT,
            insurance_amount DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS top_transaction (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            pincode TEXT,
            transaction_count BIGINT,
            transaction_amount DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS top_user (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            pincode TEXT,
            registered_users BIGINT
        );

        CREATE TABLE IF NOT EXISTS top_insurance (
            id SERIAL PRIMARY KEY,
            state TEXT,
            year INT,
            quarter INT,
            pincode TEXT,
            insurance_count BIGINT,
            insurance_amount DOUBLE PRECISION
        );
        """

        cur.execute(create_script)
        conn.commit()
        print("✅ All tables created successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error creating tables: {e}")

if __name__ == "__main__":
    create_database()
    create_tables()