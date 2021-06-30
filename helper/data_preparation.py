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
import sys

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
                                    use_listings_cache=False)
        self.data_bucket = os.environ["DATA_BUCKET"]
        print(f"Bucket Name: {self.data_bucket}")
        self.s3 = boto3.client('s3')
        self.today = datetime.now().strftime('%Y-%m-%d_%H%M%S')
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
        input_path = os.path.join(self.config['s3']['INPUT_DATA_PATH'], 'hit_data.txt')
        processing_path = os.path.join(self.config['s3']['PROCESSING_DATA_PATH'], self.today, 'hit_data.txt')
        print(input_path, processing_path)

        #Copy the files to processing directory
        status = self.copy_s3_data(input_path, processing_path)

        if status == 200:
            hit_data_set_bucket = os.path.join('s3://', self.data_bucket, processing_path)
            print(hit_data_set_bucket)
            bucket_keys = sorted(self.fs.listdir(hit_data_set_bucket), key=lambda x: x['LastModified'])
            print(bucket_keys)

        try:

            obj = self.s3.get_object(Bucket=self.data_bucket, Key=processing_path)
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

    def preprocess_data(self, input_df):
        """
        Preprocessing, data cleansing and derive new features

        :param input_df:
        :return:
        """

        #Filter ipaddress without Purchase event = 1.0
        input_df = input_df[input_df.ip.isin(input_df.loc[input_df.event_list == 1.0, 'ip'])]

        #Derive Domain name, search term and query
        input_df['page_domain'] = input_df['page_url'].apply(lambda x: urlparse(x).netloc)

        input_df['search_domain'], input_df['search_query'], input_df['search_term'] = \
            zip(*input_df['referrer'].map(self.get_referrer_data))

        #Explode data by product
        df_product_list_explode = input_df.assign(product_list=input_df['product_list'].str.split(',')).explode('product_list')

        #Split rows to create Product attributes
        df_product_list = df_product_list_explode['product_list'].str.split(';', expand=True)
        hit_enriched_data_df = pd.concat([df_product_list_explode, df_product_list], axis=1)
        self.cols_rename_format(hit_enriched_data_df)

        #Partition data by ip, hit_time and sort data by ip and partition key
        hit_enriched_data_df['partition_key'] = hit_enriched_data_df.groupby('ip')['hit_time_gmt'].rank(method='first', ascending=False)

        #Drop unwanted columns
        dataprep_df = hit_enriched_data_df.drop(['pagename','referrer', 'page_domain', 'search_query', 'category', \
                                                 'custom_event', 'user_agent', 'geo_city', 'geo_region', 'geo_country', 'page_url', 'product_list', \
                                                 'product_name','num_of_items'], \
                                                axis=1).sort_values(by=['ip', 'partition_key'], ascending=True).reset_index(drop=True)

        #Fillna for empty records
        dataprep_df = dataprep_df.replace(r'^\s*$', np.nan, regex=True)

        return dataprep_df

    def copy_s3_data(self, input_path, output_path):
        """
        This function copy and delete a file btw folders within s3 bucket
        :param input_path: String
        :param output_path: String
        :return: status
        """
        s3_resource = boto3.resource('s3')
        num_of_file = 0

        for obj in s3_resource.Bucket(self.data_bucket).objects.filter(Prefix=input_path):
            print("test")
            print(obj)
            num_of_file +=1
            if obj.key[-1] != '/':
                response = s3_resource.Object(self.data_bucket, output_path ).copy_from(CopySource= os.path.join(self.data_bucket,input_path))
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                print(status)
                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                    s3_resource.Object(self.data_bucket, input_path).delete()
                else:
                    print(f"Unsuccessful S3 put_object response. Status - {status}")
                    sys.exit(1)
        if num_of_file == 0:
            print(f"No files availabe in this bucket: {input_path}")
            sys.exit(1)


    def write_dataframe(self, result_df):
        """
        This function will write a pandas datafram into S3 bucket as a tab delimited file
        :param result_df: dataframe
        :return Status: int
        """
        #Archieve the last run files
        #Currently not implemented but can be easily achieved by calling copy_s3_data function

        today = datetime.now().strftime('%Y-%m-%d')
        export_data_path = f"{self.config['s3']['OUTPUT_DATA_PATH']}/{today}_{self.config['s3']['OUTPUT_DATA_FILE']}"
        with io.StringIO() as csv_buffer:
            result_df.to_csv(csv_buffer, index=False, sep='\t')
            response = self.s3.put_object(
                Bucket=self.data_bucket, Key=export_data_path, Body=csv_buffer.getvalue())

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(status)
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
            exit(1)

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

    def apply_business_logic(self, dataPrep_df):

        """
        This function will create simulate revenue by forward fill the revenue by ipaddress and generated the required business output
        :param datapre_df:
        :return results_df: dataframe
        """

        dataPrep_df['Revenue_Simulated'] = dataPrep_df.groupby(['ip']).tot_revenue.ffill()
        #Filter to get external website only
        result_df = dataPrep_df[dataPrep_df['search_domain'] != 'esshopzilla.com']
        result_df = result_df.loc[result_df.groupby(["ip", "Revenue_Simulated"])["partition_key"].idxmin()]

        #Drop all unnessary columns
        result_df = result_df.drop(['hit_time_gmt', 'date_time', 'ip', 'event_list', 'partition_key', 'tot_revenue'], axis=1) \
            .sort_values(by='Revenue_Simulated', ascending=False).reset_index(drop=True)


        result_df = result_df.groupby(['search_domain', 'search_term'], as_index = False)['Revenue_Simulated'].agg('sum')

        col_rename_d = {
            'search_domain': 'Search Engine Domain',
            'search_term': 'Search Keyword',
            'Revenue_Simulated': 'Revenue'
        }
        result_df.rename(col_rename_d, axis=1, inplace=True)
        print(result_df.shape[0])

        return result_df
