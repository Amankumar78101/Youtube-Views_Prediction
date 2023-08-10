## End To End ML Project

### created a environment
```
conda create -p zomato python==3.8

conda activate zomato/
```
### Install all necessary libraries
```
pip install -r requirements.txt
```

### To create a README file
```
README.md
```

### Here are the initial steps for performing EDA: create a folder named 'notebooks' and a file named 'EDA'."

## Data Ingestions step
```
df=pd.read_csv('data/finalTrain.csv')
df.head()

```

```
df.isnull().sum()
```
### Yes missing values present in the data lists of missing values Column name
```
null_cols = df.columns[df.isnull().any()].tolist()
null_cols
```

```
df.info()
```

### Lets drop the id column
```
df=df.drop(labels=['ID'],axis=1)
df.head()
```

### check for duplicated records
```
df.duplicated().sum()
```

### Splitting the  numerical and categorical columns
```
numerical_columns=df.columns[df.dtypes!='object']
categorical_columns=df.columns[df.dtypes=='object']
```

### Let's describe the categorical_columns
```
df[categorical_columns].describe()
```

### To analyze the data, we should compute the correlation of the numerical columns.
```
# Select only numerical columns
numerical_columns = df1.select_dtypes(include=[np.number])

# Set figure size
plt.figure(figsize=(10, 8))

# Compute the correlation heatmap
sns.heatmap(numerical_columns.corr(), annot=True)

# Save the plot
plt.savefig('correlation_plot.png')

```


### To Checking  unique data in columns
```
for i in categorical_columns:
    print(i,"=",df[i].unique(),'\n\n')
```

### Here are the initial steps for performing Model Training: create a  file named "Model Training".

### Data Ingestions step 
```
df = pd.read_csv('./data/finalTrain.csv')
df.head()
```
 
### Selecting the data to drop. This data is not correlated with what I want to predict
```
df=df.drop(labels=['ID','Delivery_person_ID','Order_Date','Time_Orderd','Time_Order_picked',"Restaurant_latitude",'Restaurant_longitude','Delivery_location_latitude','Delivery_location_longitude'],axis=1)
df.head()
```
 
## Splitting Independent and dependent features
```
X = df.drop(labels=['Time_taken (min)'],axis=1)
Y = df[['Time_taken (min)']]

```
 
# Define which columns should be ordinal-encoded and which should be scaled
```
categorical_cols = X.select_dtypes(include='object').columns
numerical_cols = X.select_dtypes(exclude='object').columns
```
 


# Define the custom ranking for each ordinal variable
```
weather_ranking = ['nan', 'Sunny', 'Cloudy', 'Windy', 'Sandstorms', 'Stormy', 'Fog']
Road_traffic_densitys_ranking=['nan','Low','Medium','High','Jam']
Type_of_orders_ranking=['Snack','Buffet', 'Meal', 'Drinks']
type_of_vehicle_ranking = ['electric_scooter', 'motorcycle', 'scooter', 'bicycle']
festival_ranking = ['nan', 'No','Yes' ]
city_ranking = ['nan','Semi-Urban','Urban','Metropolitian' ]
```

## Creating the Numerical Pipeline
```
num_pipeline=Pipeline(steps=[
                            ('imputer',SimpleImputer(strategy='median')),
                            ('scaler',StandardScaler())])
```
 
# Creating the Categorigal Pipeline
```
cat_pipeline=Pipeline(steps=[
                            ('imputer',SimpleImputer(strategy='most_frequent')),
                            ('ordinalencoder',OrdinalEncoder(categories=[weather_ranking,Road_traffic_densitys_ranking,
                                                                        Type_of_orders_ranking,
                                                                        festival_ranking,city_ranking])),
                            ('scaler',StandardScaler())
                                        ])
```
 

 # Creating the preprocessor
```
preprocessor=ColumnTransformer([
                                ('num_pipeline',num_pipeline,numerical_cols),
                                ('cat_pipeline',cat_pipeline,categorical_cols)
                                ])
```
 

## To Split Train and test data
```
X_train,X_test,y_train,y_test=train_test_split(X,Y,test_size=0.30,random_state=42)
```
 

# To fit and transform the X_train data

```
X_train=pd.DataFrame(preprocessor.fit_transform(X_train),columns=preprocessor.get_feature_names_out())
```

### To transform the X_test data
```
X_test=pd.DataFrame(preprocessor.transform(X_test),columns=preprocessor.get_feature_names_out())
```
 
 ### To Training Model
```
regression=LinearRegression()
regression.fit(X_train,y_train)
regression.coef_
regression.intercept_
```
 

 ### The evaluate_model function calculates mean absolute error (mae), mean squared error (mse), root mean squared error (rmse), and R2 score (r2_square) between true and predicted values, and returns them as a tuple.
```
def evaluate_model(true, predicted):
    mae = mean_absolute_error(true, predicted)
    mse = mean_squared_error(true, predicted)
    rmse = np.sqrt(mean_squared_error(true, predicted))
    r2_square = r2_score(true, predicted)
    return mae, rmse, r2_square
```

## To Train multiple models
```

models={
    'LinearRegression':LinearRegression(),
    'Lasso':Lasso(),
    'Ridge':Ridge(),
    'Elasticnet':ElasticNet()
}
trained_model_list=[]
model_list=[]
r2_list=[]

for i in range(len(list(models))):
    model=list(models.values())[i]
    model.fit(X_train,y_train)

    #Make Predictions
    y_pred=model.predict(X_test)

    mae, rmse, r2_square=evaluate_model(y_test,y_pred)

    print(list(models.keys())[i])
    model_list.append(list(models.keys())[i])

    print('Model Training Performance')
    print("RMSE:",rmse)
    print("MAE:",mae)
    print("R2 score",r2_square*100)

    r2_list.append(r2_square)
    
    print('='*35)
    print('\n')
```


### Build Docker Image 
```
docker build -t <image_name:<tagname>>
```
>Note:Image name for docker must be lowercase

###To list see docker image
```
docker image
```


### To Run the docker image
```
docker run -p 5000:5000 <image_name>
```

### To Container is running
```
docker ps
```

### To Stop Container to run 
```
docker stop
```

### To remove the docker image
```
docker image rm -f <image_name>
```


### To push docker image
```
docker push <image_name>:tag
```



