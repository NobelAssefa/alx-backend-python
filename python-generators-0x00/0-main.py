
seed = __import__('seed')

connection = seed.connect_db()
if connection:
    seed.create_database(connection)  # ✅ creates ALX_prodev
    connection.close()

    connection = seed.connect_to_prodev()  # ✅ connects to ALX_prodev

    if connection:
        seed.create_table(connection)     # ✅ creates user_data table
        seed.insert_data(connection, 'user_data.csv')  # ✅ inserts CSV data
