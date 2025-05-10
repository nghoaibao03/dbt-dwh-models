import duckdb

# Connect
con = duckdb.connect("D:/Workspace/datawarehouse/dbt-dimmensional-modeling/target/adventureworks.duckdb")

# Get all table names
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main';
""").fetchall()

# Check duplicates for each table
for (table,) in tables:
    print(f"\nChecking duplicates in table: {table}")
    query = f"""
        SELECT *, COUNT(*) 
        FROM "{table}"
        GROUP BY ALL
        HAVING COUNT(*) > 1
        LIMIT 5
    """
    try:
        result = con.execute(query).fetchdf()
        if not result.empty:
            print(result)
        else:
            print("✅ No duplicates found.")
    except Exception as e:
        print(f"❌ Error checking table {table}: {e}")

con.close()