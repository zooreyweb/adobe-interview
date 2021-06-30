import boto3
import pandas as pd
import yaml
from helper.data_preparation import ModelDataPreparation
from urllib.parse import urlparse
import re

def main():
	"""

	This is the main function that performs the data preparation and derive revenue
	 generated by search keywords and search engine domain

	Parameters: Triggered as a first entry point when a file is ingested in S3 bucket
	-----------

	:return:Exists after transforming the data to build revenue generated per search keyword and engine.
	results are persisted in S3 bucket

	Notes:
	-----

	1. Read the config YAML file for inputs
	2. Instantiate the support classes that perform different activities including,
		- Data Ingestion into the pipeline
		- Data Preparation like Cleansing, validation and enrichment of data
		- Persisting the outputs
	3. Prepare the data before applying transformation logic
	4. Apply business logic
	5. Generated output and persist the data in S3 bucket
	6. Exit

	"""

	##Reac Config yaml
	with open('config.yml', 'r') as f:
		config = yaml.safe_load(f)
		f.close

	print(config)
	##Instantiate class objects
	dataPrep = ModelDataPreparation(config)

	## Read data into Pandas dataframe
	input_df = dataPrep.get_hitdata_set()

	#Datapreparation
	dataPrep_df = dataPrep.preprocess_data(input_df)

	#Apply business logic
	result_df = dataPrep.apply_business_logic(dataPrep_df)

	#Read into S3 bucket
	status = dataPrep.write_dataframe(result_df)
	print("Completed")

if __name__ == '__main__':
	main()