import os
import time
from dotenv import load_dotenv
import boto3
from redshift_connector import connect
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL

# Load environment variables from .env
load_dotenv()

def create_redshift_cluster(redshift_client, cluster_identifier, master_username, master_password):
    response = redshift_client.create_cluster(
        ClusterIdentifier=cluster_identifier,
        NodeType='dc2.large',
        MasterUsername=master_username,
        MasterUserPassword=master_password,
        NumberOfNodes=2,
        PubliclyAccessible=True,
    )
    return response['Cluster']['ClusterStatus']

def wait_for_cluster_available(redshift_client, cluster_identifier):
    while True:
        cluster_description = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        cluster_status = cluster_description['ClusterStatus']

        if cluster_status == 'available':
            break
        elif cluster_status == 'unavailable':
            print("Cluster is currently unavailable. Waiting for it to be ready...")
        else:
            print(f"Cluster status: {cluster_status}. Waiting for it to be ready...")

        time.sleep(60)

def create_redshift_database(redshift_client, cluster_identifier, database_name, master_username, master_password):
    cluster_description = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    endpoint = cluster_description['Endpoint']['Address']

    # Create the SQLAlchemy URL
    url = URL.create(
        drivername='redshift+redshift_connector',
        host=endpoint,
        database=database_name,
        username=master_username,
        password=master_password,
    )

    # Print the Database Connection String
    print(f"Database connection string: {url}")

    return str(url)  # Convert URL object to string

def upload_to_s3(local_file_path, bucket_name, object_key, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client('s3',
         aws_access_key_id=aws_access_key_id,
         aws_secret_access_key= aws_secret_access_key)
    s3.upload_file(local_file_path, bucket_name, object_key)

def create_table_and_load_data(connection_string):
    # Create an SQLAlchemy engine with connect_args
    engine = create_engine(connection_string)

    # Drop the table if it exists
    drop_table_query = text("DROP TABLE IF EXISTS sales;")
    with engine.connect() as connection:
        connection.execute(drop_table_query)

        # Create the table
        create_table_query = """
        CREATE TABLE sales (
            order_id INT,
            product_id INT,
            quantity INT,
            price DECIMAL(10, 2)
        );
        """
        connection.execute(create_table_query)

        # Upload local CSV to S3
        s3_bucket = 'jessie-redshift-bucket'
        s3_object_key = 'sales.csv'
        local_csv_path = '/Users/jessiebaron/Documents/Redshift-Demo/sales.csv'
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
        aws_secret_access_key = os.environ.get('AWS_SECRET')
        upload_to_s3(local_csv_path, s3_bucket, s3_object_key, aws_access_key_id, aws_secret_access_key)

        # Load data into the table from S3
        copy_data_query = f"""
        COPY sales FROM 's3://{s3_bucket}/{s3_object_key}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        CSV IGNOREHEADER 1;
        """
        connection.execute(copy_data_query)

        # Query all rows in the table
        select_query = "SELECT * FROM sales;"
        rows = connection.execute(select_query)

        print("Table created, data loaded, and all rows queried:")
        for row in rows:
            print(row)

        print("Table created and data loaded successfully.")

def get_redshift_load_errors(connection_string, table_name):
    try:
        # Connect to the Redshift cluster
        with connect(connection_string) as connection:
            # Build and execute the SQL query to get load errors
            query = f"""
                SELECT *
                FROM stl_load_errors
                WHERE table_name = '{table_name}'
                ORDER BY starttime DESC
            """
            errors = connection.execute(query).fetchall()
            return errors

    except Exception as e:
        print(f"Error: {e}")

def main():
    # AWS credentials and region
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
    aws_secret_access_key = os.environ.get('AWS_SECRET')
    aws_region = 'us-east-1'

    # Redshift cluster details
    cluster_identifier = os.environ.get('CLUSTER_IDENTIFIER')
    master_username = os.environ.get('CLUSTER_NAME')
    master_password = os.environ.get('CLUSTER_PASSWORD')

    # Create Redshift client
    redshift_client = boto3.client('redshift', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    # Create Redshift cluster
    cluster_status = create_redshift_cluster(redshift_client, cluster_identifier, master_username, master_password)
    print(f"Redshift cluster creation in progress. Current status: {cluster_status}")

    # Wait for the cluster to be available
    print("Waiting for the Redshift cluster to be available...")
    wait_for_cluster_available(redshift_client, cluster_identifier)
    print("Redshift cluster is now available.")

    # Create Redshift database and get connection string
    connection_string = create_redshift_database(redshift_client, cluster_identifier, 'dev', master_username, master_password)
    load_errors = get_redshift_load_errors(connection_string, 'sales')

    # Print or process the load errors as needed
    print(load_errors)

    create_table_and_load_data(connection_string)


if __name__ == "__main__":
    main()
