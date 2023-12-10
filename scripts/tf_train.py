
import os
import argparse

import boto3
import tensorflow as tf
from tensorflow.keras.experimental import LinearModel, WideDeepModel
from tensorflow import keras
from sagemaker.experiments import load_run
from sagemaker.session import Session



class SageMakerExperimentCallback(keras.callbacks.Callback):
    def __init__(self, run):
        super().__init__()
        self.run = run
    
    def on_epoch_end(self, epoch, logs=None):
        self.run.log_metric(name="loss", value=logs["loss"], step=epoch)
        self.run.log_metric(name="mse", value=logs["mse"], step=epoch)


def parse_args():

    parser = argparse.ArgumentParser()

    # hyperparameters sent by the client are passed as command-line arguments to the script
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch_size", type=int, default=64)
    parser.add_argument("--learning_rate", type=float, default=0.1)

    # data directories
    parser.add_argument("--training", type=str, default=os.environ["SM_CHANNEL_TRAINING"])
    parser.add_argument("--testing", type=str, default=os.environ["SM_CHANNEL_TESTING"])

    # model directory: we will use the default set by SageMaker, /opt/ml/model
    parser.add_argument("--model_dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--sagemaker_region", type=str, default='us-east-1')


    return parser.parse_known_args()

def get_train_data(train_dir, batch_size):

    def pack(features, label):
        linear_features = [tf.cast(features['day_of_week'], tf.float32), tf.cast(features['month'], tf.float32),
                           tf.cast(features['hour'], tf.float32), features["trip_distance"]]
        
        dnn_features = [tf.cast(features["pickup_location_id"], tf.float32), tf.cast(features["dropoff_location_id"], tf.float32), features["trip_distance"]]
        return (tf.stack(linear_features, axis=-1), tf.stack(dnn_features, axis=-1)), label

    
    column_headers = ["day_of_week","month","hour","pickup_location_id","dropoff_location_id","trip_distance","fare_amount"]

    ds = tf.data.experimental.make_csv_dataset(tf.io.gfile.glob(train_dir + '/*.csv'),
                                               batch_size=batch_size,
                                               column_names=column_headers,
                                               num_epochs=1,
                                               shuffle=True,
                                               label_name="fare_amount")
    ds = ds.map(pack)
    return ds


if __name__ == "__main__":
    args, _ = parse_args()
    
    batch_size = args.batch_size
    epochs = args.epochs
    learning_rate = args.learning_rate
    train_dir = args.training
    region = args.sagemaker_region
    ds = get_train_data(train_dir, batch_size)
    
    boto_session = boto3.session.Session(region_name=region)
    sagemaker_session = Session(boto_session=boto_session)
    
    with load_run(sagemaker_session=sagemaker_session) as run:
        linear_model = LinearModel()
        dnn_model = keras.Sequential([
            keras.layers.Flatten(),
            keras.layers.Dense(128, activation='elu'),  
            keras.layers.Dense(64, activation='elu'), 
            keras.layers.Dense(32, activation='elu'), 
            keras.layers.Dense(1,activation='sigmoid') 
        ])
        combined_model = WideDeepModel(linear_model, dnn_model)
        combined_model.compile(optimizer="Adam", loss="mse", metrics=["mse"])

        combined_model.fit(ds, epochs=epochs, callbacks=SageMakerExperimentCallback(run))   
