import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import concat_ws, regexp_replace, udf
import pyspark.pandas as pd
import re
#import pandas as pd

# spark-submit --master yarn --deploy-mode client s3://soportedataset/scripts/etl_init.py soportedataset

def run_transform(bucket_name, input_data):	
	# Create a spark session
    spark = SparkSession.builder.appName('init etl job').getOrCreate()
    # Setup the logger to write to spark logs
    # noinspection PyProtectedMember
    logger = spark._jvm.org.apache.log4j.Logger.getLogger('TRANSFORM')
    logger.info('Spark session created')
    logger.info('Trying to read data now.')

    # Read the Historical Data CSV
    historical_path = 's3://{}/data/historical/'.format(bucket_name)
    support_pd = (spark.read.option('multiline', 'true').option('quote', '"').option("header", 'true').option("escape", '\\').option('escape', '"').csv(historical_path))
    model_path = 's3://{}/data/processed/'.format(bucket_name)
    model_pd = (spark.read.option('multiline', 'true').option('quote', '"').option("header", 'true').option("escape", '\\').option('escape', '"').csv(historical_path))
    
    emp_RDD = spark.sparkContext.emptyRDD()
    newRow = spark.createDataFrame(data = emp_RDD,
                           schema = support_pd.printSchema())

    newRow = newRow.withColumn('Subject', input_data[0])
    newRow = newRow.withColumn('Description', input_data[1])
    newRow = newRow.withColumn('Product', input_data[2])
    newRow = newRow.withColumn('BC_Case_Reason', input_data[3])

    support_pd = support_pd.union(newRow)

	# Bucket the data by activity type and write the
	# results to S3 in overwrite mode
    writer = support_pd.write
    writer.format('csv')
    writer.mode('overwrite')
    write_path = 's3://{}/data/historical/'.format(bucket_name)
    writer.option('path', write_path)
    writer.option("header", "true")
    writer.save()

   
    # Prepare the text field
    newRow = newRow.withColumn('Text', concat_ws(" ", newRow['Subject'], newRow['Description']))
    print("############### Testing 1: ", newRow.select('Text').collect()[88][0])
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], '[^a-zA-Z0-9." \n\.]', ''))
    print("############### Testing 2: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], '[\n\r]', ''))
    print("############### Testing 3: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], '  ', ' '))
    print("############### Testing 4: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], '"', ''))
    print("############### Testing 5: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], '"', ''))
    print("############### Testing 5: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], "'", ''))
    print("############### Testing 5: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.withColumn('Text', regexp_replace(newRow['Text'], ',', ''))
    print("############### Testing 5: ", newRow.select('Text').collect()[88][0])
    newRow.show()
    newRow = newRow.drop('Description', 'Subject')

    # Bucket the data by activity type and write the
	# results to S3 in overwrite mode
    model_pd = model_pd.union(newRow)
    model_pd = model_pd.filter(model_pd.Text.isNotNull())
    model_pd.groupBy('Product').count().show()
    writer = model_pd.write
    writer.format('csv')
    writer.mode('overwrite')
    write_path = 's3://{}/data/processed/'.format(bucket_name)
    writer.option('path', write_path)
    writer.option("header", "true")
    writer.save()

	# Stop Spark
    #spark.stop()


def main():
	# Accept bucket name from the arguments passed.
	# TODO: Error handling when there are no arguments passed.
    bucket_name = sys.argv[1]
    input_data = sys.argv[2]
	# Run the transform method
    run_transform(bucket_name)


if __name__ == '__main__':
	main()



