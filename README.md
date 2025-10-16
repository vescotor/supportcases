# supportcases

Spark scripts:
- Script for the initial process and transform of the historic dataset. It will create two new datasets, one with the historic data which will be processed by the AWS Glue service and the other dataset will contain only the data that the deep learning model requires for training.
- Script to train the classification model.
- Script to process and transform new data.

Jupyter Notebook:
- Contains relevant information about the dataset.
- Contains the models used to classify the support cases in Product and Sub-Product.

Función Lambda:
- Script used to setup the Lambda function that will connect the SageMaker endpoint of the model to an API Gateway service.

