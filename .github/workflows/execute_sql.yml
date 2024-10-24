name: Execute SQL Scripts
on:
  push: # trigger a workflow when a push is made to the main branch for commits to sql_scripts directory
    branches:
      - main 
    paths:
      - '.github/sql_scripts/**'
  pull_request: # trigger when a pull request is made to merge changes to main branch for sql_scripts directory
      branches:
      - main
      paths:
      - '.github/sql_scripts/**'
      
jobs:
  execute_sql_scripts:
    runs-on: ubuntu-latest # I am specifying the OS
    steps:
    # We provide access to the code,branches in GitHub to the workflow. Allows access to SQL scripts
    - uses: actions/checkout@v2 
      with:
      #I specify how much git history I want to retrieve
        fetch-depth: 2  # To get the previous 2 commits

    - name: Use Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        # python interacts with snowflake through below extension
        pip install snowflake-connector-python

    - name: Execute SQL Scripts
      env:
        SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
        SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
        SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
        SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
        SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
      run: |
        # Get list of changed files in the push
        #Compares files between the previous commit and current commit
        #scope of change only limited to sql_scripts directory
        CHANGED_FILES=$(git diff --name-only ${{ github.event.before }} ${{ github.sha }} | grep -E '\.github/sql_scripts/.*\.(sql|txt)$' || true)

        #to check if the changed files are not empty
        if [ ! -z "$CHANGED_FILES" ]; then
          for file in $CHANGED_FILES; do
            if [ -f "$file" ]; then # I check if the file exists
              echo "Processing changed file: $file"
              # call the python function
              python .github/scripts/execute_snowflake_script.py "$file"
            fi
          done
        else
          echo "No SQL files were changed in this push"
        fi
