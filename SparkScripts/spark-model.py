import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import concat_ws, regexp_replace, udf
import pyspark.pandas as pd
import tensorflow as tf
import tensorflow.keras as keras
import tensorflow.keras. layers as layers
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.layers import Embedding, LSTM, Dense, Bidirectional
from tensorflow.keras.models import Sequential
from sklearn.preprocessing import LabelEncoder, LabelBinarizer
from sklearn.model_selection import train_test_split
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from spark_tensorflow_distributor import MirroredStrategyRunner
import pandas as pd
import numpy as np

def run_training(bucket_name):	
	# Create a spark session
    spark = SparkSession.builder.appName('training job').getOrCreate()
    # Setup the logger to write to spark logs
    # noinspection PyProtectedMember
    logger = spark._jvm.org.apache.log4j.Logger.getLogger('TRANSFORM')
    logger.info('Spark session created')
    logger.info('Trying to read data now.')

    # Read the Historical Data CSV
    modeldata_path = 's3://{}/data/processed/'.format(bucket_name)
    model_path = 's3://{}/'.format(bucket_name)
    model_pd = (spark.read.option('multiline', 'true').option('quote', '"').option("header", 'true').option("escape", '\\').option('escape', '"').csv(modeldata_path))

    model_pd = model_pd.filter(model_pd.Text.isNotNull())

    X = model_pd.select('Text').toPandas().to_numpy().reshape(-1)
    prod_array = model_pd.select('Product').toPandas().to_numpy().reshape(-1)
    reason_array = model_pd.select('BC_Case_Reason').toPandas().to_numpy().reshape(-1)

    label_encoder = LabelBinarizer()
    Y1 = label_encoder.fit_transform(prod_array)

    label_encoder2 = LabelBinarizer()
    Y2 = label_encoder2.fit_transform(reason_array

    X_train, X_val, Y_train, Y_val = train_test_split(X, Y1, test_size=0.2, random_state=24, shuffle=True)
    X2_train, X2_val, Y2_train, Y2_val = train_test_split(X, Y2, test_size=0.2, random_state=24, shuffle=True)

    maxlen = 2000

    tokenizer = Tokenizer(num_words=6000, oov_token="<OOV>", lower=True)
    tokenizer.fit_on_texts(X_train)
    word_index = tokenizer.word_index

    sequences = tokenizer.texts_to_sequences(X_train)
    X_train_padded = pad_sequences(sequences, maxlen, padding='post', truncating='post')
    sequences = tokenizer.texts_to_sequences(X_val)
    X_val_padded = pad_sequences(sequences, maxlen, padding='post', truncating='post')

    tokenizer = Tokenizer(num_words=6000, oov_token="<OOV>", lower=True)
    tokenizer.fit_on_texts(X2_train)    
    word_index = tokenizer.word_index

    sequences = tokenizer.texts_to_sequences(X2_train)
    X2_train_padded = pad_sequences(sequences, maxlen, padding='post', truncating='post')
    sequences = tokenizer.texts_to_sequences(X2_val)
    X2_val_padded = pad_sequences(sequences, maxlen, padding='post', truncating='post')

    def train():
        vocab_size = 6000
        embedding_dim = 16 #dimensión de los vectores densos (word embeddings)
        input_layer= layers.Input(maxlen)

        prod_layer = layers.Embedding(vocab_size, embedding_dim)(input_layer)
        subprod_layer = layers.Embedding(vocab_size, embedding_dim)(input_layer)

        prod_layer = layers.Bidirectional(layers.LSTM(64, return_sequences=True))(prod_layer)
        prod_layer = layers.Bidirectional(layers.LSTM(64))(prod_layer)
        prod_layer = layers.Dense(12, activation="sigmoid", name="prod")(prod_layer)

        subprod_layer = layers.Bidirectional(layers.LSTM(64, return_sequences=True))(subprod_layer)
        subprod_layer = layers.Bidirectional(layers.LSTM(64))(subprod_layer)
        subprod_layer = layers.Dense(94, activation="sigmoid", name="subprod")(subprod_layer)

        model3 = keras.Model(inputs=input_layer, outputs=[prod_layer,subprod_layer], name="SupportModel")
        model3.summary()

        model3.compile(loss="categorical_crossentropy", optimizer="adam", metrics=['accuracy'])

        metrics = {
        "box": ['accuracy'],
        "class": ['accuracy']
        }

        h3 = model3.fit(
        x = X2_train_padded, 
        y= {
            "prod": Y_train,
            "subprod": Y2_train
        }, 
        epochs=5, 
        validation_data=(
            X2_val_padded, 
            {"prod": Y_val,
            "subprod": Y2_val
        }),
        verbose=1
        )

        product3 = model3.evaluate(x= X_val_padded, y={"prod": Y_val, "subprod":Y2_val}, verbose=1)    

        log = []
        log.append(h3)
        log.append(product3)

        model3.save(model_path)
        writer = log.write
        writer.format('csv')
        writer.mode('overwrite')
        write_path = 's3://{}/data/logs/'.format(bucket_name)
        writer.option('path', write_path)
        writer.option("header", "true")
        writer.save()

    train()
	# Stop Spark
    spark.stop()


def main():
	# Accept bucket name from the arguments passed.
	# TODO: Error handling when there are no arguments passed.
    bucket_name = sys.argv[1]
	# Run the transform method
    run_training(bucket_name)


if __name__ == '__main__':
	main()


