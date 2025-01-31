import os
import re
import sys
import snowflake.connector

def remove_comments(sql_script):
    # Remove single-line comments
    sql_script = re.sub(r'--.*', '', sql_script)
    
    # Remove multi-line comments
    sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
    
    return sql_script

def check_exists(sql_script):
    connection = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT')
    )
    cursor = connection.cursor()
    
    with open(sql_script, 'r') as file:
        sql_queries = remove_comments(file.read())
        
    tables = re.findall(r'insert into ([A-Z_\.]+)', sql_queries, re.IGNORECASE)
    
    warnings = []
    
    for table in tables:
        database, schema, table_name = table.split('.')
        
        # Check database
        cursor.execute(f"SELECT COUNT(*) FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '{database}'")
        if cursor.fetchone()[0] == 0:
            warnings.append(f"Database {database} does not exist for the query: {table}")
            continue

        # Check schema
        cursor.execute(f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema}'")
        if cursor.fetchone()[0] == 0:
            warnings.append(f"Schema {schema} does not exist in database {database} for the query: {table}")
            continue

        # Check table
        cursor.execute(f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'")
        if cursor.fetchone()[0] == 0:
            warnings.append(f"Table {table_name} does not exist in schema {schema} of database {database} for the query: {table}")
    
    return warnings

if __name__ == "__main__":
    changed_files = sys.argv[1].split()
    warnings = []
    
    for script in changed_files:
        warnings.extend(check_exists(script))
    
    if warnings:
        for warning in warnings:
            print(f"::warning::{warning}")
        exit(1)
    else:
        print("All database, schema, and tables exist. Scripts look good.")
