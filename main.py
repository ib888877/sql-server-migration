
import os
import pandas as pd
import pymssql
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import json

load_dotenv()

# --- Configuration ---
DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION")

TABLES_TO_EXPORT = ['bkup_rpt_tr04_rcpt_20220614',
'bkup_v_vam_comm_shop_day_20221117',
'cbt_process_mapping',
'changelog_ct_product',
'changelog_ct_product_mapping',
'changelog_ct_product_mapping_dscrpt',
'changelog_ct_shop',
'co_country',
'co_enrolment_list',
'co_et_fsp_redemption_report',
'co_et_fsp_upload_report',
'co_et_intervention',
'co_et_intervention_cycle',
'co_intervention',
'co_intervention_cycle',
'co_ke_raw_commodity_price',
'co_lb_loyalty_programme',
'co_payment_list',
'co_ps_raw_product',
'co_ps_raw_redemptions',
'co_st_co_payment_list',
'co_st_fsp_redemption_report',
'co_st_fsp_upload_report',
'co_st_ke_raw_mpesa',
'co_st_lb_ct_shop',
'co_st_lb_raw_bank',
'co_st_lb_raw_market_data',
'co_st_lb_raw_mrso',
'ct_brand',
'ct_brand_mapping',
'ct_brand_mapping_en',
'ct_cat_category',
'ct_cat_commodity',
'ct_cat_family',
'ct_cat_subcategory',
'ct_cat_type',
'ct_file_proc_status',
'ct_geo_admin_area',
'ct_geo_area_type',
'ct_geo_country',
'ct_geo_location',
'ct_geo_location_type',
'ct_geo_market',
'ct_geo_regional_bureau',
'ct_geo_suboffice',
'ct_manufacturer',
'ct_product',
'ct_product_mapping',
'ct_product_mapping_dscrpt',
'ct_product_type_en',
'ct_product_type_mapping',
'ct_product_vam',
'ct_retailer',
'ct_shop',
'ct_shop_size',
'ct_shop_status',
'ct_shop_tmp',
'ct_tr_error',
'ct_units_measure',
'fsp_payment_instrument_status_report',
'fsp_redemption_report',
'fsp_upload_report',
'log_data_proc',
'log_file_proc',
'log_load_errors',
'log_ssis_import',
'log_tr_errors',
'log_tr_errors_prod',
'newitems',
'quantum_raw_transactions_copy',
'raw_bank',
'raw_enrolment_list',
'raw_exchange_rate_usd',
'raw_household_size',
'raw_market_research_data',
'raw_mdtry_prod_price',
'raw_mdtry_prod_price_so',
'raw_mpesa_report',
'raw_st_proc',
'raw_tr_prod',
'raw_transactions',
'raw_transactions_Hani_draft',
'rpt_tr04_rcpt',
'rpt2_bk01_shop_month',
'rpt2_bk02_shop_day',
'rpt2_tr01_shop_prod_month_prog',
'rpt2_tr0101_area_prod_month',
'rpt2_tr0101_prod_month_prog',
'rpt2_tr0101_prod_month_prog_l1',
'rpt2_tr0101_prod_month_prog_l2',
'rpt2_tr010101_category_month_prog',
'rpt2_tr0102_shop_month_ix',
'rpt2_tr0102_shop_month_ix_l1',
'rpt2_tr0102_shop_month_ix_l2',
'rpt2_tr02_shop_day_prog',
'rpt2_tr02bk01_shop_month',
'rpt2_tr03_anomaly_qty_nfi',
'rpt2_tr05_shop_prod_day_prog',
'rpt2_tr06_prod_month_median_price',
'sales_draft',
'sysdiagrams',
'tmp_newitems',
'tmp_newrecords',
'user_access_control',
'user_access_level'
]

CHUNK_SIZE = 1000000
PROGRESS_FILE = "progress.json"

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the SQL Server database using pymssql."""
    print("--- Attempting to connect to the database with pymssql: ---")
    print(f"Server: {DB_SERVER}")
    print(f"Database: {DB_DATABASE}")
    print(f"Username: {DB_USERNAME}")
    print("-----------------------------------------------------------")
    try:
        conn = pymssql.connect(
            server=DB_SERVER,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        print("Database connection successful!")
        return conn
    except pymssql.Error as ex:
        print(f"Database connection error: {ex}")
        return None

# --- S3 Connection ---
def get_s3_client():
    """Establishes a connection to the S3 bucket."""
    print("--- Attempting to connect to S3 with the following parameters: ---")
    print(f"Bucket Name: {S3_BUCKET_NAME}")
    print(f"Region: {S3_REGION}")
    print("-------------------------------------------------------------")
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print("S3 connection successful!")
        return s3_client
    except NoCredentialsError:
        print("S3 credentials not found.")
        return None
    except Exception as e:
        print(f"S3 connection error: {e}")
        return None

# --- Progress Handling ---
def load_progress():
    """Loads the export progress from the progress file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress):
    """Saves the export progress to the progress file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=4)

# --- Main Export Logic ---
def export_table_to_csv(table_name, conn, s3_client, progress):
    """Exports a single table to CSV files in chunks, resuming from saved progress."""
    print(f"Exporting table: {table_name}")

    table_progress = progress.get(table_name, {"offset": 0, "file_count": 1})
    offset = table_progress["offset"]
    file_count = table_progress["file_count"]

    try:
        while True:
            query = f"SELECT * FROM {table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {CHUNK_SIZE} ROWS ONLY"
            df = pd.read_sql(query, conn)

            if df.empty:
                break

            file_name = f"{table_name}_part_{file_count}.csv"
            csv_buffer = df.to_csv(index=False)

            try:
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=f"DB_Export/{table_name}/{file_name}", Body=csv_buffer)
                print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME}")

                offset += CHUNK_SIZE
                file_count += 1
                progress[table_name] = {"offset": offset, "file_count": file_count}
                save_progress(progress)

            except Exception as e:
                print(f"Error uploading to S3: {e}")
                print("The process will stop. You can resume it later.")
                return

        print(f"Finished exporting table: {table_name}")
        # Reset progress for the table once it's fully exported
        if table_name in progress:
            del progress[table_name]
            save_progress(progress)

    except Exception as e:
        print(f"Error exporting table {table_name}: {e}")


def main():
    """Main function to run the export process."""
    db_conn = get_db_connection()
    s3_client = get_s3_client()

    if not db_conn or not s3_client:
        print("Could not establish connections. Exiting.")
        return

    if not TABLES_TO_EXPORT or TABLES_TO_EXPORT == [""]:
        print("No tables to export. Please update the TABLES_TO_EXPORT list in main.py")
        return

    progress = load_progress()

    for table in TABLES_TO_EXPORT:
        export_table_to_csv(table, db_conn, s3_client, progress)

    db_conn.close()
    print("Export process completed.")

if __name__ == "__main__":
    main()
