import re
import snowflake.connector
import os

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Connect to Snowflake
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )

def check_exists(db, schema, table):
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        # Check if database exists
        cursor.execute(f"SHOW DATABASES LIKE '{db}'")
        if not cursor.fetchone():
            return f"Database '{db}' does not exist"
        
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db} LIKE '{schema}'")
        if not cursor.fetchone():
            return f"Schema '{schema}' does not exist in database '{db}'"
        
        # Check if table exists in the schema
        cursor.execute(f"SHOW TABLES IN SCHEMA {db}.{schema} LIKE '{table}'")
        if not cursor.fetchone():
            return f"Table '{table}' does not exist in schema '{schema}' of database '{db}'"
        
    return f"Table '{db}.{schema}.{table}' exists"

# Extract table references (FROM db.schema.table) from SQL file
def extract_table_references(sql_file_path):
    with open(sql_file_path, 'r') as file:
        content = file.read()
    
    # Regular expression to find table references (e.g., FROM db.schema.table)
    pattern = r"FROM\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)"
    return re.findall(pattern, content)

# Main function to check the existence of all tables in the SQL file
def check_sql_file_existence(sql_file_path):
    table_references = extract_table_references(sql_file_path)
    results = []
    
    for db, schema, table in table_references:
        result = check_exists(db, schema, table)
        results.append(result)
    
    return results

if __name__ == "__main__":
    # Get the file path passed as a command-line argument
    if len(sys.argv) < 2:
        print("Please provide the path to the SQL file.")
        sys.exit(1)
    
    sql_file_path = sys.argv[1]  # The path to the SQL file provided as a command-line argument

    results = check_sql_file_existence(sql_file_path)
    for result in results:
        print(result)
