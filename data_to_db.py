from telnetlib import EC
import pandas as pd
import pymongo
import json
from datetime import datetime


class DataDB():

    def __init__(self):

        with open("configs.json", 'r') as config_data:
            configs = json.load(config_data)

        self.client = pymongo.MongoClient(
            configs['host'], int(configs['port']))
        self.csv_path = configs['csvPath']
        self.mydb = self.client[configs['db_name']]
        self.mycol = self.mydb[configs['collectName']]

    def read_and_update(self):
        """
        This will take the data, and saves it in the database after processing and return a successful response.
        """
        try:
            print('--- Reading and updating database ---')

            data_from_csv = pd.read_csv(self.csv_path)
            data_from_csv.drop_duplicates(inplace=True)

            data_from_csv[['TXN',
                           'CONST_NUM',
                           'RRN',
                           'ACC_NUM',
                           'BANK',
                           'CUST_NAM',
                           'TRANS_TYPE']] = data_from_csv['NARRATION'].str.split('/',
                                                                                 expand=True)
            data_inserted = data_from_csv.to_dict(orient='records')
            x = self.mycol.insert_many(data_inserted)

            print(data_from_csv.head())
            return "success"

        except Exception as e:
            print(f'--- Exception in reading and updating db: {e}---')
            return "failed"

    def records_count(self):
        """
        This api call should return number of rows present
        """
        try:
            print('--- Fetching records count ---')
            record_count = self.mycol.find().count()
            return record_count

        except Exception as e:

            print(f'--- Exception in fetching records count: {e}---')
            return "failed"

    def unique_banknames(self):
        """
        This api call should return number of unique banks present in your db in json format.
        """
        try:
            print('--- Fetching unique bank names ---')
            unique_bnames = self.mycol.find().distinct('BANK')
            return unique_bnames

        except Exception as e:
            print(f'--- Exception in fetching unique bank names: {e}---')
            return "failed"

    def dateswise_data(self, start_date, end_date):
        """This api call will take 2 query parameters as input
i.e, from date and to date and will return number of
transactions occurred during that interval."""
        try:
            print('--- Fetching datewise data ---')
            coll_list = []

            for coll in self.mycol.find(
                    {}, {'_id': 0, 'TXN DATE': 1, 'TXN': 1}):

                coll_list.append(coll)

            df_read = pd.DataFrame(coll_list)
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            df_read['TXN DATE'] = pd.to_datetime(df_read['TXN DATE'])

            num_trans = len(df_read[(df_read['TXN DATE'] > start_date) & (
                df_read['TXN DATE'] <= end_date)]['TXN'])

            return num_trans

        except Exception as e:
            print(f'--- Exception in fetching datewise data: {e}---')
            return "failed"

    def fetch_cust_names(self):
        """This api should return names of all customers in Camel Case
format. ( e.g, Ram Mishra )"""
        try:
            print('--- Fetching customer names ---')
            coll_list = []
            for coll in self.mycol.find({}, {'_id': 0, 'CUST_NAM': 1}):

                coll_list.append(coll)

            df_read = pd.DataFrame(coll_list)
            res_cust_name = {'CamelCase': [], 'TitleCase': []}
            for names in df_read['CUST_NAM'].unique():
                output = ''.join(x for x in names.title() if x.isalnum())
                res_cust_name['CamelCase'].append(
                    output[0].lower() + output[1:])
                res_cust_name['TitleCase'].append(names.title())

            return res_cust_name

        except Exception as e:
            print(f'--- Exception in fetching customer names: {e}---')
            return "failed"

    def fetch_trans_summary(self):
        """This api call should return number of transactions based on its type.E.g, { ‘IMPS’ : 10, ‘NEFT’ : 15 }"""
        try:
            print('--- Fetching transactions summary ---')
            coll_list = []
            for coll in self.mycol.find({}, {'_id': 0, 'TRANS_TYPE': 1}):

                coll_list.append(coll)

            df_read = pd.DataFrame(coll_list)

            summ_data = df_read.groupby('TRANS_TYPE').size().to_dict()

            return summ_data

        except Exception as e:
            print(f'--- Exception in fetching transactions summary: {e}---')
            return "failed"

    def fetch_trans_amount_summary(self):
        """This api call should return total amount of transactions based on its type.E.g, { ‘IMPS’ : 12,265, ‘NEFT’ : 10,560 }"""
        try:
            print('--- Fetching transactions amount summary ---')
            coll_list = []
            for coll in self.mycol.find(
                    {}, {'_id': 0, 'TRANS_TYPE': 1, 'AMOUNT': 1}):

                coll_list.append(coll)

            df_read = pd.DataFrame(coll_list)

            summ_data = df_read.groupby('TRANS_TYPE').sum().to_dict()

            return summ_data

        except Exception as e:
            print(
                f'--- Exception in fetching transactions amount summary: {e}---')
            return "failed"

    def fetch_total_amount(self):
        """This api call should return total transaction amount."""
        try:
            print('--- Fetching total amount ---')
            coll_list = []
            for coll in self.mycol.find({}, {'_id': 0, 'AMOUNT': 1}):

                coll_list.append(coll)

            df_read = pd.DataFrame(coll_list)

            summ_data = df_read.sum()

            return summ_data

        except Exception as e:
            print(f'--- Exception in fetching transactions summary: {e}---')
            return "failed"
