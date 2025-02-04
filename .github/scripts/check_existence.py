import os
import sys
import re
import sqlparse

def remove_comments(sql):
    """Removes -- and /* */ comments from SQL script."""
    sql = re.sub(r'--.*', '', sql)  # Remove single-line comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)  # Remove multi-line comments
    return sql

def split_queries(sql):
    """Splits SQL script into individual queries using ; as a separator."""
    return [q.strip() for q in sqlparse.split(sql) if q.strip()]

def extract_table_names(queries):
    """Extracts table names based on 'FROM db.schema.table' pattern."""
    table_pattern = re.compile(r'from\s+([\w.]+)', re.IGNORECASE)
    table_list = []

    for query in queries:
        matches = table_pattern.findall(query)
        if matches:
            table_list.extend(matches)

    return list(set(table_list))  # Remove duplicates

def process_sql_file(file_path):
    """Reads and processes a single SQL file."""
    with open(file_path, "r") as f:
        sql_script = f.read()

    cleaned_sql = remove_comments(sql_script)
    queries = split_queries(cleaned_sql)
    table_names = extract_table_names(queries)

    return file_path, table_names

def main():
    if len(sys.argv) < 2:
        print("No changed SQL files provided.")
        return

    changed_files = sys.argv[1].split()  # Read files from command-line args
    
    all_tables = {}

    for sql_file in changed_files:
        if os.path.exists(sql_file):  # Ensure file exists before processing
            file_path, tables = process_sql_file(sql_file)
            all_tables[file_path] = tables
        else:
            print(f"Warning: {sql_file} not found.")

    print("Extracted Table Names from Changed Files:")
    for file, tables in all_tables.items():
        print(f"{file}: {tables}")

if __name__ == "__main__":
    main()
