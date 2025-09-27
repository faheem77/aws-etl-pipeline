import json
import boto3
import pandas as pd
import io
import json
import re
import snowflake.connector
import configparser
from elasticsearch import Elasticsearch, exceptions
from elasticsearch import helpers
from snowflake.connector.pandas_tools import write_pandas

from image.src.data_wrangling import (
    column_rename,
    change_status,
    parse_name,
    transform_open_house,
    generate_full_address,
    split_emails,
    generate_transaction_id,
    clean_phone_numbers,
    clean_columns
)

# Load config
config = configparser.ConfigParser()
config.read("config.ini")

class SnowflakeConnector:
    def __init__(self, config):
        self.sf_cfg = config["snowflake"]
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish connection to Snowflake"""
        self.conn = snowflake.connector.connect(
            user=self.sf_cfg["user"],
            password= self.sf_cfg["password"],
            account=self.sf_cfg["account"],
            warehouse=self.sf_cfg["warehouse"],
            database=self.sf_cfg["database"],
            schema=self.sf_cfg["schema"],
        )
        self.cursor = self.conn.cursor()
        print("✅ Connection established")
        return self.conn

    def test_connection(self):
        """Run a simple test query to validate connection"""
        try:
            self.cursor.execute("SELECT CURRENT_VERSION()")
            result = self.cursor.fetchone()
            print("✅ Connection successful. Snowflake version:", result[0])
            return result[0]
        except Exception as e:
            print("❌ Connection test failed:", e)
            raise

    def execute_query(self, query):
        """Execute a query and return results"""
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print("❌ Query failed:", e)
            raise
    def load_dataframe(self, df):
        """
        Creates the table with predefined schema and loads the DataFrame into Snowflake.
        """
        # Step 1: Define column SQL
        columns = {
            "property_status": "STRING",
            "price": "FLOAT",
            "bedrooms": "FLOAT",
            "bathrooms": "FLOAT",
            "square_feet": "FLOAT",
            "address_line_1": "STRING",
            "address_line_2": "STRING",
            "street_number": "STRING",
            "street_name": "STRING",
            "street_type": "STRING",
            "pre_direction": "STRING",
            "unit_type": "STRING",
            "unit_number": "STRING",
            "city": "STRING",
            "state": "STRING",
            "zip_code": "STRING",
            "latitude": "FLOAT",
            "longitude": "FLOAT",
            "compass_property_id": "FLOAT",
            "property_type": "STRING",
            "year_built": "FLOAT",
            "brokered_by": "STRING",
            "presented_by_mobile": "STRING",
            "mls": "STRING",
            "list_date": "STRING",
            "pending_date": "STRING",
            "listing_office_id": "STRING",
            "listing_agent_id": "STRING",
            "email": "STRING",
            "page_link": "STRING",
            "scraped_date": "STRING",
            "presented_by_first_name": "STRING",
            "presented_by_middle_name": "STRING",
            "presented_by_last_name": "STRING",
            "oh_startTime": "STRING",
            "oh_company": "STRING",
            "oh_contactName": "STRING",
            "full_address": "STRING",
            "email_1": "STRING",
            "email_2": "STRING",
            "id": "STRING"
        }

        col_defs_sql = ", ".join([f'{col} {dtype}' for col, dtype in columns.items()])


        # Step 2: Create table only if not exists
        table_name = "DATA_PRISM.TRANSFORMED.TRANSFORMED_TRANSACTIONS_DATA"
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs_sql})"

        self.cursor.execute(create_sql)
        print(f"✅ Table 'transformed_transactions_data' ensured")

        # Step 3: Load data using write_pandas (this will append to the table)
        success, nchunks, nrows, _ = write_pandas(self.conn, df, "TRANSFORMED_TRANSACTIONS_DATA", schema="TRANSFORMED", quote_identifiers=False)
        if success:
            print(f"✅ Loaded {nrows} rows into 'transformed_transactions_data' in {nchunks} chunks")
        else:
            print("❌ Failed to load data")
    def close(self):
        """Close cursor and connection safely"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("🔒 Connection closed")


class ElasticClient:
    def __init__(self, config_path="config.ini"):
        # Load config
        es_cfg = config["elasticsearch"]
        self.index_name = 'property-data-standardized'

        # Connect using API key
        self.es = Elasticsearch(
            es_cfg["url"],
            api_key=es_cfg["api_key"]
        )

        # Test connection
        if self.es.ping():
            print("✅ Connected to Elasticsearch")
        else:
            raise ValueError("❌ Could not connect to Elasticsearch")

    def push_dataframe(self, df: pd.DataFrame):
        """
        Push pandas DataFrame to Elasticsearch index
        """
        # Bulk push for efficiency
        actions = [
            {"_index": self.index_name, "_source": row.dropna().to_dict()}
            for _, row in df.iterrows()
        ]
        if actions:
            helpers.bulk(self.es, actions)
            print(f"✅ Loaded {len(df)} documents into index '{self.index_name}'")
        else:
            print("⚠️ No data to push")

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        
        # Only process files in specific prefix
        if not object_key.endswith(".csv"):
            print(f"Skipping file: {object_key}")
            continue
        
        print(f"Processing file: s3://{bucket_name}/{object_key}")

        # 2. Read the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(file_content))
        sf = SnowflakeConnector(config)
        sf.connect()
        df = column_rename(df)
        df = change_status(df)
        df = parse_name(df)
        df = transform_open_house(df)
        df = generate_full_address(df)
        df = split_emails(df)
        df = generate_transaction_id(df)
        df = clean_phone_numbers(df, "presented_by_mobile")
        es= ElasticClient()
        df = clean_columns(df)
        sf.load_dataframe(df)
        es.push_dataframe(df)
