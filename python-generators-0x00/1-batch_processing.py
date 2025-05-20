seed = __import__('seed')


def stream_users_in_batches(batch_size):
    connection1 = seed.connect_db()
    connection = seed.connect_to_prodev()
    
    batch =[]
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data")
    
    for row in cursor:
        batch.append(row)
        for row in cursor:
            if len(batch) == batch_size:
                yield batch
                batch = []

    # yield remaining rows if any
    if batch:
        yield batch

    cursor.close()
    connection.close()
    
    
def batch_processing(batch_size):
    """
    Generator function that filters users over age 25 from batches
    """
    for batch in stream_users_in_batches(batch_size):
        filtered = [user for user in batch if user["age"] > 25]
        print('filterd',filtered)
        yield filtered
