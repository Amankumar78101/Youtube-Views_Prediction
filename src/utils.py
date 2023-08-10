import os
import sys
import pickle

from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

from src.exception import CustomException
from src.logger import logging

def save_object(file_path, obj):
    """The save_object function takes a file path and an object as inputs, creates a directory 
    for the file if it doesn't exist, then saves the object to the file using pickle."""
    try:
        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)

    except Exception as e:
        raise CustomException(e, sys)
    
def evaluate_model(X_train,y_train,X_test,y_test,models):
    """The evaluate_model function takes X_train, y_train, X_test, y_test, and a dictionary of models as inputs, 
        fits each model on the training data, predicts the testing data, and calculates R2 scores for each model on 
        the test data, then returns a dictionary containing the R2 scores for each model."""
    try:
        report = {}
        for i in range(len(models)):
            model = list(models.values())[i]
            # Train model
            model.fit(X_train,y_train)

            

            # Predict Testing data
            y_test_pred =model.predict(X_test)

            # Get R2 scores for train and test data
            #train_model_score = r2_score(ytrain,y_train_pred)
            test_model_score = r2_score(y_test,y_test_pred)

            report[list(models.keys())[i]] =  test_model_score

        return report

    except Exception as e:
        logging.info('Exception occured during model training')
        raise CustomException(e,sys)
    
def load_object(file_path):
    """The load_object function takes a file path as input, opens the file in binary mode, loads the 
        object from the file using pickle, and returns the loaded object."""
    try:
        with open(file_path,'rb') as file_obj:
            return pickle.load(file_obj)
    except Exception as e:
        logging.info('Exception Occured in load_object function utils')
        raise CustomException(e,sys)

    







