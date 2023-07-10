import json, gzip, os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select, cast, JSON, inspect, func, text

# Step 1: READ the JSONL file
json_file_path = './raw_data/athlete_events_2006_2016.jsonl.gz'
with gzip.open(json_file_path, 'rt') as file:
    json_data = file.readlines()

# Step 2: Parse the JSONL data
parsed_data = [json.loads(line) for line in json_data]

# Step 3: Transform the data (if needed)

# Step 4: Connect to the PostgreSQL database
host = os.environ.get('pghost')
port = os.environ.get('pgport')
database = os.environ.get('pgdatabase')
user = os.environ.get('pguser')
password = os.environ.get('pgpassword')

connection_params = {
    'host': host,
    'port': port,
    'database': database,
    'user': user,
    'password': password
}

# Create the connection string
connection_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user=user, password=password, host=host, port=port, database=database
)
engine = create_engine(connection_string)
connection = engine.connect()

# Step 5: Import/Copy the transformed data into the database
metadata = MetaData()
metadata.reflect(bind=engine)
inspector = inspect(engine)

# Step 6: Create the table schema for temp_data_table
table_name = 'temp_data_table'

# Step 7: Create the table if it doesn't exist
if not inspector.has_table(table_name):
    table = Table(
        table_name,
        metadata,
        Column('data', JSON)
    )
    table.create(bind=engine, checkfirst=True)

# Step 8: Insert the transformed data into the table
table = Table(table_name, metadata, autoload=True, autoload_with=engine)

try:
    connection.execute(table.insert(), [{"data": row} for row in parsed_data])

    # Step 9: Perform any additional optimizations (e.g., creating cluster partitions)

    # Step 10: Create the processed_data_table
    processed_table_name = 'processed_data_table'
    processed_table = Table(
        processed_table_name,
        metadata,
        Column('year', Integer),
        Column('season', String),
        Column('medal', String),
        Column('team', String),
        extend_existing=True  # Enable extending the existing table
    )

    # Step 11: Create the table if it doesn't exist
    if not inspector.has_table(processed_table_name):
        processed_table.create(bind=engine, checkfirst=True)

    # Step 12: Populate the processed_data_table
    insert_query = processed_table.insert().from_select(
        ['year', 'season', 'medal', 'team'],
        select(
            cast(func.json_extract_path_text(table.c.data, 'year'), Integer).label('year'),
            cast(func.json_extract_path_text(table.c.data, 'season'), String).label('season'),
            cast(func.json_extract_path_text(table.c.data, 'medal'), String).label('medal'),
            cast(func.json_extract_path_text(table.c.data, 'team'), String).label('team')
        ).select_from(table)
    )

    connection.execute(insert_query)
    
except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    connection.close()
    engine.dispose()
