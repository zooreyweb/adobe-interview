"""
Healper module corresponding to
"""
import boto3
import io
import os
import pandas as pd
import s3fs
import time
from datetime import datetime
import numpy as np
from urllib.parse import urlparse
import re


class ModelDataPreparation:

    def __init__(self, config):
        """
        Class constructor.
        This is the class constructor that received the config object.

        :param config: dict
            Config required for the data
        :return None
        """

        self.config = config
        print(self.config)
        self.fs = s3fs.S3FileSystem(default_cache_type='none',
                                    use_listings_cache=False)  # This exposes a filesystem-like API (ls, cp, open, etc.) on top of S3 storage.
        self.data_bucket = os.environ["DATA_BUCKET"]
        print(f"Bucket Name: {self.data_bucket}")
        self.s3 = boto3.client('s3')
        self.today = datetime.now().strftime('%Y-%m-%d')
        return None

    def get_hitdata_set(self):
        """
        This function will perform below actions
            1. Copy the file to processing directory
            2. Read the files into pandas dataframe
            3. Return the dataframe to main function

        :return: input_data_set dataframe
            Read the data from S3 bucket and returns pandas DF
        """
        data_ingest_start = time.time()
        print(data_ingest_start)
        print('inside hitdataset')
        input_path = os.path.join(self.config['s3']['INPUT_DATA_PATH'], 'hit_data.txt')
        processing_path = os.path.join(self.config['s3']['PROCESSING_DATA_PATH'], self.today, 'hit_data.txt')
        print(input_path, processing_path)

        #Copy the files to processing directory
        status = self.copy_s3_data(input_path, processing_path)
        status =200
        if status == 200:
            hit_data_set_bucket = os.path.join('s3://', self.data_bucket, processing_path)
            print(hit_data_set_bucket)
            bucket_keys = sorted(self.fs.listdir(hit_data_set_bucket), key=lambda x: x['LastModified'])
            print(bucket_keys)

        try:
            print('inside read_csv')
            bucket_name = 'adobe-hitdata'
            print(bucket_name)
            #s3://adobe-hitdata/inputs/hit_data/data_org.txt

            obj = self.s3.get_object(Bucket=bucket_name, Key=processing_path)
            input_data_set = pd.read_csv(io.BytesIO(obj['Body'].read()), sep='\t', engine='python')
            input_data_set.head(3)
            '''
            print(os.path.join('s3://', bucket_keys[-1]['key']))
            input_data_set = pd.read_csv(os.path.join('s3://', bucket_keys[-1]['key']), sep='\t', engine='python')
            '''
            print('inside read_csv 2')
        except Exception as e:
            print(f"Error reading input dataset st S3 location: {processing_path}.Please check and rerun the "
                  f"process {e}!")

        # This variables will be useful to build a Job Execution Audit table and create various DQ check
        data_ingest_time = time.time() - data_ingest_start
        record_count = input_data_set.shape[0]
        print(record_count)
        #total_bytes = int(input_data_set.memory_usuage().sum())

        if record_count == 0:
            error_messaage = f'File is empty. Please check and try again!'
            raise Exception(error_messaage)

        return input_data_set

    def copy_s3_data(self, input_path, output_path):
        """
        This function copy and delete a file btw folders within an s3 bucket 
        :param input_path: String
        :param output_path: String
        :return: status
        """
        print('inside s3 copy function')
        s3_resource = boto3.resource('s3')
        response = s3_resource.Object(self.data_bucket, output_path ).copy_from(CopySource=input_path)
        print(response)
        #status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        s3_resource.Object(self.data_bucket, input_path).delete()
        '''
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
            s3_resource.Object(self.data_bucket, input_path).delete()
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
        '''


    def write_dataframe(self, df):
        """
        :param df:
        :return:
        """
        print("inside write")
        
        export_data_path = f"{self.config['s3']['OUTPUT_DATA_PATH']}/{self.today}_{self.config['s3']['OUTPUT_DATA_FILE']}"
        print(export_data_path)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            response = self.s3.put_object(
                Bucket=self.data_bucket, Key=export_data_path, Body=csv_buffer.getvalue())

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(status)
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")

    def get_referrer_data(self, referrer):
        """
        This function get an URL and returns domain name, query and search term from Search engine.
        Search term is derived based on regex and currently supports google, yahoo, bing and esshopzilla domain only

        :param referrer: Input Url
        :return: domain_name, query and search_term
        """
        r = urlparse(referrer)
        search_term = None
        domain_name = '.'.join(r.netloc.split('.')[1:])
        query = r.query
        regex = f"( ?q=|p=|pid=|k=)([^&#]+)"
        s = re.search(regex, query, re.IGNORECASE)
        if s:
            search_term = s[2]
        return domain_name, query, search_term

    def cols_rename_format(self, hit_enriched_data_df):
        """
        Rename columns from product based on product attributes
        :param hit_enriched_data_df:
        :return: dataframe
        """

        col_rename_d = {
            0: 'category',
            1: 'product_name',
            2: 'num_of_items',
            3: 'tot_revenue',
            4: 'custom_event',
            5: 'merchandising_evar'
        }
        hit_enriched_data_df.rename(col_rename_d, axis=1, inplace=True)
