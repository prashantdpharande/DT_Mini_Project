from flask import Flask, render_template, request
import pickle
import json
import numpy as np

model= pickle.load(open('artifacts/model.pkl','rb'))

with open('artifacts/columns_name.json','r') as json_file:
    col_name=json.load(json_file)
col_name_list=col_name['Columns']

app=Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict',methods=['GET','POST'])
def user_info():
    data=request.form
    user_data=np.zeros(len(col_name_list))
    user_data[0] = data['age']
    user_data[1] = data['sex']
    user_data[2] = data['cp']
    user_data[3] = data['trestbps']
    user_data[4] = data['chol']
    user_data[5] = data['fbs']
    user_data[6] = data['restecg']
    user_data[7] = data['thalach']
    user_data[8] = data['exang']
    user_data[9] = data['oldpeak']
    user_data[10] = data['slope']
    user_data[11] = data['ca']
    user_data[12] = data['thal']
    
    result = model.predict([user_data])
    if result[0] == 0:
        modelling_result = "Heart Disease Exist"
    else: 
        modelling_result = "Heart Disease Does Not Exist"
    
    print(modelling_result)


    return render_template("result.html",prediction =modelling_result)


if __name__=='__main__':
    app.run(host='0.0.0.0',port='8585',debug=True)
    
