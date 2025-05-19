import mysql.connector
from mysql.connector import Error
import csv
import uuid


def connect_db():
    """Connects to the MySQL server (not to a specific database)."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1292",  # Change this to your actual MySQL password
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None
def create_database(connection):
    """Creates the ALX_prodev database if it does not exist."""
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev checked/created successfully")
        cursor.close()
    except Error as e:
        print(f"Error creating database: {e}")
        
def connect_to_prodev():
    """Connects to the ALX_prodev database."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='1292',  # Change this to your actual MySQL password
            database='ALX_prodev'
        )
        return connection
    except Error as e:
        print(f"Error connecting to ALX_prodev: {e}")
        return None
    
def create_table(connection):
    """Creates the user_data table if it does not exist."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age DECIMAL NOT NULL,
                INDEX(user_id)
            );
        """)
        print("Table user_data created successfully")
        cursor.close()
    except Error as e:
        print(f"Error creating table: {e}")
        
def insert_data(connection, csv_file):
    """Inserts data into user_data table from a CSV file."""
    try:
        cursor = connection.cursor()

        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Check for duplicate before inserting
                cursor.execute("SELECT * FROM user_data WHERE email = %s", (row['email'],))
                if cursor.fetchone():
                    continue  # Skip if email already exists
                user_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO user_data (user_id, name, email, age)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, row['name'], row['email'], row['age']))
        connection.commit()
        print("Data inserted successfully from CSV.")
        cursor.close()
    except Error as e:
        print(f"Error inserting data: {e}")
