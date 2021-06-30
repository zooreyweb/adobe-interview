## Adobe Analytics - Hit Data Analysis ##

<b> Situation: </b> How much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?
<br>
<p>
<b> Task: </b> The Objective of this is to analyze Analytics hit event data, calculate revenue earned by search engine 
and generate tab delimited file with the following data points:
<br>1. Search Engine Domain (i.e. google.com).
<br>2.Search Keyword (i.e. "Laffy Taffy")
<br>3.Revenue (i.e. 12.95)
<p>
<b> Action :</b> This model uses Adobe Analytics dataset. The analysis will load the data from S3 bucket, preprocess and cleanse data. 
We will use the pre-processed data to apply business logic and generate the final output.

<br>
<b> Results: </b> Revenue output file is generated per the requirement above.
<b>Please note, enriched the data only for future usecases like verify revenue by search term and product.
<br>
<b> Scenarios not covered: </b>

##Other Possible scenarios which are not covered part of this Transformation

1. Not verifying if a customer search term and order completed items were matching.

2. A customer can searched multiple time and placed an order.
   Based on Use case, either we can assign the revenue to the last search or 1st search in the same file or have a timeshift.
2. User can made multiple products, but searched with one search term, in this case, we can assign the revenue only by Total_revenue from purchase - (product * num of items)
3. Searched within our website directly.

How to Deploy and Run the code:

##Technology Stack

<b>Compute </b>: AWS Batch </br>
<b>Storage</b>: S3</br>
<b>Repository:</b> Docker container deployed in ECR
<b>CICD Pipeline</b>: Codebuild</br>

#Prerequisite:
<b>Step 1:</b> Create New ECR Repository <br>
<b>Step 2:</b> Create New Compute Environment in AWS Batch. For simplicity, I created On-demand env<br>
<b>Step 3:</b> Create New Job Queues based on this compute env<br>
<b>Step 4:</b> Create an AWS Batch job based on ECR Image tag and env variable for Data Bucket<br>
<b>Step 5:</b> Create Job definition in ECR<br>

#Create Docker Image and deploy to AWS

<b>Step 1: </b>Build Docker image by executing this command from your Terminal
docker build -t <image_name> <br>
<b>Step 2:</b> Push the image by executing this command<br>
<b>Step 3:</b> Create Lambda function to trigger the job based on S3 event<br>
<b>Step 4:</b> Upload a S3 file(naming convention: hit_data.txt)<br>
<b>Step 4:</b> S3 event will trigger  Lambda function and inturn batch job will be invoked from this function. Please upload python file and config yml file from Lambda folder <br>
<b>Step 5: </b> Output will be generated and placed in the S3 bucket <br>
