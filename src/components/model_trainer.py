# Basic Import
import os
import sys
from src.logger import logging
from src.utils import save_object
from dataclasses import dataclass
from src.utils import evaluate_model
from src.exception import CustomException
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge,Lasso,ElasticNet


@dataclass 
class ModelTrainerConfig:
    # seving the model file
    trained_model_file_path = os.path.join('artifacts','model.pkl')


class ModelTrainer:
    def __init__(self):
        self.model_trainer_config = ModelTrainerConfig()

    def initate_model_training(self,train_array,test_array):
        try:
            # This code is splitting the train and test data arrays into input features and target features for both train and test datasets
            logging.info('Splitting Dependent and Independent variables from train and test data')
            X_train, y_train, X_test, y_test = (
                train_array[:,:-1],
                train_array[:,-1],
                test_array[:,:-1],
                test_array[:,-1]
            )
            #This code is creating a dictionary models containing instances of the LinearRegression, Lasso, Ridge, and ElasticNet regression models.
            models={
            'LinearRegression':LinearRegression(),
            'Lasso':Lasso(),
            'Ridge':Ridge(),
            'Elasticnet':ElasticNet(),
            'RandomForest': RandomForestRegressor()
            }
            # This code is printing the evaluation report for the models and formatting it into a string that shows the percentage accuracy of each model.
            print('\n====================================================================================\n')
            model_report:dict=evaluate_model(X_train,y_train,X_test,y_test,models)
            result = [f"{key}: {model_report[key] * 100}%" for key in model_report]
            print(' '.join(result))


            print('\n====================================================================================\n')            
            logging.info(f'Model Report :{model_report}')

            # This code identifies the best model based on the highest R2 score and prints the name of the best model and its corresponding R2 score.
            best_model_score = max(sorted(model_report.values()))

            best_model_name = list(model_report.keys())[
                list(model_report.values()).index(best_model_score)
            ]
            
            best_model = models[best_model_name]

            print(f'Best Model Name : {best_model_name} , R2 Score : {best_model_score*100}')
            print('\n====================================================================================\n')
            logging.info(f'Best Model Name : {best_model_name} , R2 Score : {best_model_score}')

            save_object(
                 file_path=self.model_trainer_config.trained_model_file_path,
                 obj=best_model
            )
          

        except Exception as e:
            logging.info('Exception occured at Model Training')
            raise CustomException(e,sys)
        

