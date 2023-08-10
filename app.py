from flask import Flask,request,render_template,jsonify
from src.pipeline.prediction_pipeline import CustomData,PredictPipeline


application=Flask(__name__)

app=application


@app.route('/')
def home_page():
    return render_template('index.html')

@app.route('/predict',methods=['GET','POST'])

def predict_datapoint():
    if request.method=='GET':
        return render_template('form.html')
    
    else:
        data=CustomData(
            Delivery_person_Age=float(request.form.get('Age')),
            Delivery_person_Ratings = float(request.form.get('Ratings')),
            Weather_conditions = str(request.form.get('Weather conditions')),
            Vehicle_condition = int(request.form.get('Vehicle condition')),
            Type_of_order = str(request.form.get('Type of order')),
            Type_of_vehicle=str(request.form.get('Type of vehicle')),
            multiple_deliveries =float( request.form.get('Multiple deliveries')),
            Road_traffic_density =request.form.get('Road traffic density'),
            Festival= str(request.form.get('Festival')),
            City =str(request.form.get('City')),
            Displacement =float(request.form.get('Displacement'))
            
        )
        final_new_data=data.get_data_as_dataframe()
        predict_pipeline=PredictPipeline()
        pred=predict_pipeline.predict(final_new_data)

        results=round(pred[0],2)

        return render_template('results.html',final_result=results)






if __name__=="__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)



