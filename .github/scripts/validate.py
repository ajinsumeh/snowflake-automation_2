import os
import re
import sys
import snowflake.connector
from typing import List, Tuple

def get_snowflake_connection():
    """Establish connection to Snowflake using environment variables."""
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
    )

def remove_comments(sql_content: str) -> str:
    """Remove SQL comments from the content."""
    # Remove inline comments (--) and their content
    sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
    # Remove multi-line comments (/* ... */)
    sql_content = re.sub(r'/\*[\s\S]*?\*/', '', sql_content)
    return sql_content

def split_queries(sql_content: str) -> List[str]:
    """Split SQL content into individual queries based on semicolon."""
    # Split on semicolons and filter out empty queries
    queries = [q.strip() for q in sql_content.split(';')]
    return [q for q in queries if q]

def validate_query(conn, query: str) -> Tuple[bool, str]:
    """Validate a single query using EXPLAIN plan."""
    try:
        cursor = conn.cursor()
        #print("Testing query:"+query)
        explain_query = f"EXPLAIN USING TEXT {query}"
        cursor.execute(explain_query)
        result = cursor.fetchall()
        cursor.close()
        return True, "Query:"+query+ "is valid"
    except Exception as e:
        return False, str(e)

def main():
    # Get list of files to validate from command line arguments
    if len(sys.argv) < 2:
        print("No files to validate")
        return

    files_to_validate = sys.argv[1:]
    
    # Connect to Snowflake
    try:
        conn = get_snowflake_connection()
    except Exception as e:
        print(f"Failed to connect to Snowflake: {str(e)}")
        exit(1)

    # Process each changed SQL file
    has_errors = False
    for file_path in files_to_validate:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
            
        print(f"\nValidating {os.path.basename(file_path)}...")
        
        # Read and process SQL file
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Remove comments and split into individual queries
        clean_content = remove_comments(content)
        queries = split_queries(clean_content)
        
        # Validate each query
        for i, query in enumerate(queries, 1):
            print("Checking query:"+query)
            valid, message = validate_query(conn, query)
            if not valid:
                print(f"Error in query {i}: {message}")
                has_errors = True
            else:
                print(f"Query {i} is valid")

    conn.close()
    
    if has_errors:
        exit(1)
    else:
        print("\nAll queries validated successfully!")

if __name__ == "__main__":
    main()
