import sqlite3

sqlite_db = "Membership.db"
table_name = "Member_details"


# Function to create the database and table if they don't exist
def create_db_and_table():
    connection = sqlite3.connect(sqlite_db)
    cursor = connection.cursor()
    # Create table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            first_name TEXT,
            last_name TEXT,
            dob DATE,
            last_active DATE,
            score INTEGER,
            member_since INTEGER,
            state TEXT,
            company_name TEXT,
            id INTEGER
        )"""
        )

    connection.commit()
    connection.close()


# Function to write a partition to SQLite
def write_partition_to_sqlite(iterator):
    # Connect to SQLite database
    connection = sqlite3.connect(sqlite_db)
    cursor = connection.cursor()

    # Insert rows in bulk for efficiency
    rows = list(iterator)  # Collect partition rows as a list of tuples
    cursor.executemany(
        f"INSERT INTO {table_name} (first_name, last_name, dob, last_active, score, member_since, state, company_name, id)  \
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    # Commit and close connection
    connection.commit()
    connection.close()
