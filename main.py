
import os
import pandas as pd
import pyodbc
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# Replace these with your actual database and S3 credentials
DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER = os.getenv("DB_DRIVER")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION")

TABLES_TO_EXPORT = ["table1", "table2", "table3"] # Replace with your table names

CHUNK_SIZE = 1000000  # One million records per CSV

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the SQL Server database."""
    conn_str = f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}"
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}")
        return None

# --- S3 Connection ---
def get_s3_client():
    """Establishes a connection to the S3 bucket."""
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )
        return s3_client
    except NoCredentialsError:
        print("S3 credentials not found.")
        return None

# --- Main Export Logic ---
def export_table_to_csv(table_name, conn, s3_client):
    """Exports a single table to CSV files in chunks."""
    print(f"Exporting table: {table_name}")
    try:
        offset = 0
        file_count = 1
        while True:
            query = f"SELECT * FROM {table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {CHUNK_SIZE} ROWS ONLY"
            df = pd.read_sql(query, conn)

            if df.empty:
                break

            file_name = f"{table_name}_part_{file_count}.csv"
            csv_buffer = df.to_csv(index=False)

            try:
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=f"{table_name}/{file_name}", Body=csv_buffer)
                print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME}")
            except Exception as e:
                print(f"Error uploading to S3: {e}")
                return

            offset += CHUNK_SIZE
            file_count += 1
        print(f"Finished exporting table: {table_name}")
    except Exception as e:
        print(f"Error exporting table {table_name}: {e}")


def main():
    """Main function to run the export process."""
    db_conn = get_db_connection()
    s3_client = get_s3_client()

    if not db_conn or not s3_client:
        print("Could not establish connections. Exiting.")
        return

    for table in TABLES_TO_EXPORT:
        export_table_to_csv(table, db_conn, s3_client)

    db_conn.close()
    print("Export process completed.")

if __name__ == "__main__":
    main()
