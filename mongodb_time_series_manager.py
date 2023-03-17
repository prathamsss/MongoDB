#!/usr/bin/env python3
""" Utility to manage timestamp data in MongoDB """

from pymongo import MongoClient
import bson
import datetime
from pymongo import errors



class MongoDB:
    """
    A class for interacting with a MongoDB database and its collections.
    """

    def __init__(self, db_name):
        """
        Constructor method for MongoDB class.

        :param db_name: the name of the database to connect with.
        """
        self.client = MongoClient()
        self.db = self.client[db_name]

    def create_timeseries_collection(self, collection_name, time_field, meta_field):
        """
        Create a new time-series collection with specified time_field and meta_field.
        If collection is existing it would return that collection.

        :param collection_name: the name of the collection to create
        :param time_field: the name of the time field in the collection documents
        :param meta_field: the name of the meta field in the collection documents
        :return: the created collection
        """
        try:
            timeseries_options = {
                'timeseries': {
                    'timeField': time_field,
                    'metaField': meta_field
                }
            }
            codec_options = bson.codec_options.CodecOptions(tz_aware=True)

            collection = self.db.create_collection(collection_name, **timeseries_options).with_options(
                codec_options=codec_options)

        except errors.CollectionInvalid as e:
            collection = self.db['collection_name']
            print(f"{e}. Continuing")

        return collection

    def insert_timeseries_document(self, collection, timestamp, value, tags):
        """
        Insert a document into a time-series collection.

        :param collection: the collection to insert the document into
        :param timestamp: the timestamp value for the document
        :param value: the value for the document
        :param tags: the tags for the document
        """

        document = {
            'timestamp': timestamp,
            'value': value,
            'tags': tags
        }
        collection.insert_one(document)

    def print_collection_data(self, collection):
        """
        Print all documents in a collection.

        :param collection: the collection to print the documents from
        """
        for document in collection.find():
            print(document)

    def delete_document_by_timestamp(self, collection, timestamp):
        """
        Delete a document from a time-series collection by timestamp.

        :param collection: the collection to delete the document from
        :param timestamp: the timestamp of the document to delete
        """

        query = {'timestamp': timestamp}
        result = collection.delete_one(query)
        print(f'Deleted {result.deleted_count} document(s).')

    def get_documents_by_timestamp(self, collection, start_time, end_time):
        """
        Get all documents from a time-series collection between specified start and end times.

        :param collection: the collection to get documents from
        :param start_time: the start time of the documents to retrieve
        :param end_time: the end time of the documents to retrieve
        :return: a cursor with the documents matching the query
        """

        query = {'timestamp': {'$gte': start_time, '$lte': end_time}}
        return collection.find(query)

    def get_documents_by_tag(self, collection, tag_name, tag_value):
        """
        Get all documents from a time-series collection that have a specified tag value.

        :param collection: the collection to get documents from
        :param tag_name: the name of the tag to search for
        :param tag_value: the value of the tag to search for
        :return: a cursor with the documents matching the query
        """
        query = {'tags.' + tag_name: tag_value}
        return collection.find(query)

    def remove_duplicates(self,collection,field_to_check):
        """
        Remove all documents from the collection that have a duplicate value
        for the specified field.

        :param collection: The collection to remove duplicates from.
        :type collection: pymongo.collection.Collection
        :param field_to_check: The field to check for duplicates.
        :type field_to_check: str
        """

        # Get a list of unique values for the specified field
        unique_values = collection.distinct(field_to_check)
        # Remove all documents that have a duplicate value for the specified field
        for value in unique_values:
            filter = {field_to_check: value}
            result = collection.delete_many(filter)
            print(f"Deleted {result.deleted_count} documents with value {value}")



# Example about how you can access the above utility:

mongo = MongoDB("Warehouse_1")
collection = mongo.create_timeseries_collection('myt3', 'timestamp', 'tags')

# insert some documents into the collection
timestamp1 = datetime.datetime.utcnow() - datetime.timedelta(days=1)
value1 = 12345
tags1 = {'bounding_box_count': 1, 'location': 'xyz',"value": 1223}
mongo.insert_timeseries_document(collection, timestamp1, value1, tags1)

timestamp2 = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
value2 = 67890
tags2 = {'bounding_box_count': 2, 'location': 'abc', 'value': 5657}
mongo.insert_timeseries_document(collection, timestamp2, value2, tags2)


# print the data in the collection
print('All documents:')
mongo.print_collection_data(collection)

# delete a document by timestamp
print('\nDeleting document with timestamp', timestamp1)
mongo.delete_document_by_timestamp(collection, timestamp1)

# get documents by timestamp range
print('\nDocuments with timestamps between', timestamp2, 'and', datetime.datetime.utcnow())
for document in mongo.get_documents_by_timestamp(collection, timestamp2, datetime.datetime.utcnow()):
    print(document)

# get documents by tag
print('\nDocuments with value=5657:')
for document in mongo.get_documents_by_tag(collection, 'value', 5657):
    print(document)
