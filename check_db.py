import sqlite3

DB_PATH = 'stroke_drugs.db'

def check_usernames_in_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check if the username column exists
        cursor.execute("PRAGMA table_info(annotations)")
        columns = cursor.fetchall()
        username_column_exists = any(column[1] == 'username' for column in columns)
        
        if not username_column_exists:
            print("The 'username' column does not exist in the 'annotations' table.")
            return

        # Query to check for non-null usernames
        cursor.execute("""
            SELECT username, COUNT(*) as count
            FROM annotations
            WHERE username IS NOT NULL AND username != ''
            GROUP BY username
        """)
        results = cursor.fetchall()
        
        if results:
            print("\nUsernames found in the database:")
            for username, count in results:
                print(f"  {username}: {count}")
        else:
            print("\nNo non-empty usernames found in the database.")
        
        # Check for null or empty usernames
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM annotations
            WHERE username IS NULL OR username = ''
        """)
        null_count = cursor.fetchone()[0]
        
        print(f"\nNumber of rows with null or empty usernames: {null_count}")
        
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_usernames_in_db()