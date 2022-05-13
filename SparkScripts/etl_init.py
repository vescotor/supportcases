import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import concat_ws, regexp_replace, udf
import pyspark.pandas as pd
import re
#import pandas as pd

# spark-submit --master yarn --deploy-mode client s3://soportedataset/scripts/etl_init.py soportedataset

def run_transform(bucket_name):	
	# Create a spark session
    spark = SparkSession.builder.appName('init etl job').getOrCreate()
    # Setup the logger to write to spark logs
    # noinspection PyProtectedMember
    logger = spark._jvm.org.apache.log4j.Logger.getLogger('TRANSFORM')
    logger.info('Spark session created')
    logger.info('Trying to read data now.')

    # Read the Historical Data CSV
    historical_path = 's3://{}/historical/'.format(bucket_name)
    support_pd = (spark.read.option('multiline', 'true').option('quote', '"').option("header", 'true').option("escape", '\\').option('escape', '"').csv(historical_path))
    support_pd = support_pd.withColumnRenamed("Case Age (days)", "Case Age (days)".replace( "(days)" , "days"))
    support_pd = support_pd.withColumnRenamed("Time to FR (Minutes)", "Time to FR (Minutes)".replace( "(Minutes)" , "Minutes"))
    support_pd = support_pd.withColumnRenamed("Date/Time Opened", "Date/Time Opened".replace( "/" , " "))
    support_pd = support_pd.withColumnRenamed("Date/Time Closed", "Date/Time Closed".replace( "/" , " "))
    for c in support_pd.columns:
        support_pd = support_pd.withColumnRenamed(c, c.replace( " " , "_"))
    

    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], '[^a-zA-Z0-9." \n\.]', ''))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], '[\n\r]', ''))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], '  ', ' '))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], '"', ''))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], '"', ''))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], "'", ''))
    support_pd = support_pd.withColumn('Description', regexp_replace(support_pd['Description'], ',', ''))

    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], '[^a-zA-Z0-9." \n\.]', ''))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], '[\n\r]', ''))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], '  ', ' '))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], '"', ''))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], '"', ''))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], "'", ''))
    support_pd = support_pd.withColumn('Subject', regexp_replace(support_pd['Subject'], ',', ''))

	# Bucket the data by activity type and write the
	# results to S3 in overwrite mode
    writer = support_pd.write
    writer.format('csv')
    writer.mode('overwrite')
    write_path = 's3://{}/data/historical/'.format(bucket_name)
    writer.option('path', write_path)
    writer.option("header", "true")
    writer.option("quote", '"')
    writer.option("escape", "\\")
    writer.option('escape', '"')
    writer.save()

    support_pd.toPandas().to_csv('s3://{}/data/'.format(bucket_name))

    # Prepare the data for model
    #model_pd = support_pd.filter((support_pd.Language == "English")&(support_pd.Case_Record_Type == "Customer Support Case")&(support_pd.Type == "Issue")&(support_pd.Case_Reason == "Account - General")) 
    model_pd = support_pd.drop('Case_Owner', 'Case_Number', 'Case_Origin', 'Priority', 'Root_Cause', 'Case_Triage_Tier', 'Time_in_Hours', 'Status', 'Date_Time_Opened', 'Date_Time_Closed', 'Case_Date/Time_Last_Modified', 'Case_Age_days', 'Time_to_FR_Minutes', 'XX_Met_SLA', 'Account_Name', 'JIRA_Key', 'JIRA_Status')
    model_pd = model_pd.dropna()
    model_pd = model_pd[model_pd['Language'] == 'English']
    model_pd = model_pd[model_pd['Case_Record_Type'] == 'Customer Support Cases']
    model_pd = model_pd[model_pd['Type'] == 'Issue']
    model_pd = model_pd[model_pd['Case_Reason'] == 'Account - General']
    model_pd = model_pd.drop('Case_Sub_Reason', 'Language', 'Case_Record_Type', 'Type', 'Case_Reason')
   
    # Prepare the text field
    model_pd = model_pd.withColumn('Text', concat_ws(" ", model_pd['Subject'], model_pd['Description']))
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], '[^a-zA-Z0-9." \n\.]', ''))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], '[\n\r]', ''))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], '  ', ' '))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], '"', ''))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], '"', ''))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], "'", ''))
    model_pd.show()
    model_pd = model_pd.withColumn('Text', regexp_replace(model_pd['Text'], ',', ''))
    model_pd.show()
    model_pd = model_pd.drop('Description', 'Subject')

    # Bucket the data by activity type and write the
	# results to S3 in overwrite mode
    model_pd = model_pd.filter(model_pd.Text.isNotNull())
    model_pd.groupBy('Product').count().show()
    writer = model_pd.write
    writer.format('csv')
    writer.mode('overwrite')
    write_path = 's3://{}/data/processed/'.format(bucket_name)
    writer.option('path', write_path)
    writer.option("header", "true")
    writer.save()

    #model_pd.toPandas().to_csv('s3://{}/'.format(bucket_name))

	# Stop Spark
    #spark.stop()


def main():
	# Accept bucket name from the arguments passed.
	# TODO: Error handling when there are no arguments passed.
	bucket_name = sys.argv[1]
	# Run the transform method
	run_transform(bucket_name)


if __name__ == '__main__':
	main()



