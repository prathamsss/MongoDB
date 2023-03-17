#!/usr/bin/env python3

import os
from pymongo import MongoClient
import urllib.parse
import argparse
import warnings

warnings.filterwarnings("ignore")


class MyMongoDB:
    """
     Utility to handle mongoDB database.
     In this task, I've used MongoDB database to scale.
     When you have new camera, just insert RTSP and ID in this database.
     This would help to store rtsp link into our database.
    """

    def __init__(self,  db_name, collection_name):
        """
        :param db_name: Name for database
        :param collection_name: Collection name for database
        """
        self.client = MongoClient()
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_data(self, data):
        """
        This would add entries of new camera into database.
        :param data: dictionary to insert into database
        """
        if self.collection.find_one(data) is None:
            self.collection.insert_one(data)
        else:
            print('Document already exists in the collection.')

    def retrieve_data(self, query=None):
        """
        Method to retrieve all data.
        :param query: If you have custom collection to
                      retrieve, pass into this param
        :return: list of data
        """
        if query is None:
            results = self.collection.find()
        else:
            results = self.collection.find(query)
        return list(results)

    def print_data(self, data):
        """
        Method to print all data.
        :param data: data from above retrive function.
        """
        for item in data:
            print(item)

    def delete_all_data(self):
        """
        Method to delete all data
        """
        result = self.collection.delete_many({})
        print(f"{result.deleted_count} documents deleted.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MongoDB database creation tool')
    parser.add_argument('--delete', default=False, required=False,type=bool,
                        help='Argument to delete all data in database.')

    args = parser.parse_args()


    mongo = MyMongoDB("camera_devices", "factory_1")

    if args.delete:
        # delete all data
        mongo.delete_all_data()
    else:
        # Add data to database
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        rtsp_link_1 = os.path.join(directory, "video/1.mp4")
        id_1 = 1

        rtsp_link_2 = os.path.join(directory ,"video/2.mp4")
        id_2 = 2


        print("=====>",rtsp_link_1)
        # Here we Insert two video rtsp link / video file path.
        mongo.insert_data({"rtsp_link": (rtsp_link_1), "cam_id": id_1})
        mongo.insert_data({"rtsp_link": (rtsp_link_2), "cam_id": id_2})

        #  Retrieve all data and print it
        data = mongo.retrieve_data()
        mongo.print_data(data)

        # data = mongo.retrieve_data({"cam_id": 2})
        # mongo.print_data(data)

        # for each in data:
        #     print("each ==>",each["id"])

