import snowflake.connector
import sys
import os
import re

def remove_comments(sql):
    # Remove inline comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    # Remove multi-line comments
    sql = re.sub(r'/\*[\s\S]*?\*/', '', sql)
    return sql

def extract_queries(sql_file):
    with open(sql_file, 'r') as f:
        content = f.read()
    
    # Remove all comments first
    actual_query = remove_comments(content)
    
    # Split into individual queries by semicolon
    queries = []
    current_query = []
    
    for line in actual_query.splitlines():
        line = line.strip()
        if not line:  # Skip empty lines
            continue
            
        current_query.append(line)
        if line.endswith(';'):
            query = ' '.join(current_query)
            queries.append(query)
            current_query = []
    
    # Handle any remaining query without semicolon
    if current_query:
        queries.append(' '.join(current_query))
    
    return queries

def extract_object_reference(query):
    """Extract database, schema, and table from any SQL statement"""
    # Pattern matches both INSERT INTO and FROM clauses
    patterns = [
        r'INSERT\s+INTO\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)',
        r'FROM\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return {
                'database': match.group(1).upper(),
                'schema': match.group(2).upper(),
                'table': match.group(3).upper(),
                'full_name': f"{match.group(1)}.{match.group(2)}.{match.group(3)}",
                'query': query.strip()
            }
    return None

def validate_objects(conn, query_info):
    """Validate if database, schema, and table exist"""
    cursor = conn.cursor()
    validation_messages = []
    
    try:
        # Check database
        cursor.execute("""
        SELECT COUNT(*) 
        FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES 
        WHERE DATABASE_NAME = %s
        """, (query_info['database'],))
        if cursor.fetchone()[0] == 0:
            validation_messages.append(f"Database '{query_info['database']}' does not exist")
            return validation_messages
        
        # Check schema
        cursor.execute(f"""
        SELECT COUNT(*) 
        FROM {query_info['database']}.INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME = %s
        """, (query_info['schema'],))
        if cursor.fetchone()[0] == 0:
            validation_messages.append(f" Schema '{query_info['schema']}' does not exist in database '{query_info['database']}'")
            return validation_messages
        
        # Check table
        cursor.execute(f"""
        SELECT COUNT(*) 
        FROM {query_info['database']}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s
        """, (query_info['schema'], query_info['table']))
        if cursor.fetchone()[0] == 0:
            validation_messages.append(f"Table '{query_info['table']}' does not exist in {query_info['database']}.{query_info['schema']}")
    
    finally:
        cursor.close()
    
    return validation_messages

def format_validation_results(query_results):
    """Format validation results for GitHub comment"""
    message = []
    for result in query_results:
        message.append(f"Query: ```sql\n{result['query']}\n```")
        if result['errors']:
            message.extend(result['errors'])
        else:
            message.append("All objects exist")
        message.append("---")
    return "\n".join(message)

def main():
    if len(sys.argv) != 2:
        print("Usage: python validate_sql.py <sql_file>")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    queries = extract_queries(sql_file)
    
    if not queries:
        print("No valid SQL queries found in file")
        sys.exit(0)
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT']
    )
    
    validation_results = []
    has_errors = False
    
    try:
        for query in queries:
            result = {'query': query, 'errors': []}
            query_info = extract_object_reference(query)
            
            if query_info:
                validation_messages = validate_objects(conn, query_info)
                if validation_messages:
                    result['errors'] = validation_messages
                    has_errors = True
            
            validation_results.append(result)
    
    finally:
        conn.close()
    
    # Format and print results
    print(format_validation_results(validation_results))
    
    if has_errors:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
