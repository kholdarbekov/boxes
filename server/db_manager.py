import os
from pymongo import MongoClient

DB_USERNAME = os.environ.get("DB_USERNAME")
DB_USER_PASSWORD = os.environ.get("DB_USER_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")


def get_database():

    if DB_USERNAME and DB_USER_PASSWORD and DB_HOST and DB_PORT:
        CONNECTION_STRING = f"mongodb://{DB_USERNAME}:{DB_USER_PASSWORD}@{DB_HOST}:{DB_PORT}"
    else:
        # If no env variables are found then use local setup
        # with localhost:27017 which is default
        CONNECTION_STRING = None

    # Create a connection using MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database and return it
    boxes_db = client.boxes
    boxes_indexes_dict = boxes_db.boxes.index_information()
    if 'category_index' not in boxes_indexes_dict:
        boxes_db.boxes.create_index('category', name='category_index')
    
    if 'created_at_index' not in boxes_indexes_dict:
       boxes_db.boxes.create_index('created_at', name='created_at_index')

    return boxes_db
