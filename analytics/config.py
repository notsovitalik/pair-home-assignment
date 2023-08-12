import os

# path and names
SQL_FOLDER = os.path.join('sql')
QUERY_AGGREGATE = 'calculate_agg.sql'
QUERY_CREATE_TABLE = 'create_agg_table.sql'
QUERY_DELETE_HOUR = 'delete_agg_hour.sql'

TARGET_SCHEMA = 'analytics'
TARGET_TABLE = 'device_aggregate'

# periodicity in seconds
PERIODICITY = 600 

