import pandas as pd
import sys
import numpy as np
import os
from google.cloud import bigquery
from datetime import datetime
from flask import Flask, render_template, request, escape
import google.cloud.logging

app = Flask(__name__)
CREDS = "project-395518-3924a7001029.json"
bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS,project='project-395518')

if int(os.environ.get("PRODUCTION", 0)) == 1:
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

@app.route('/set_base_model')
def index():
    return render_template('form_data.html')

@app.route('/data', methods = ['POST', 'GET'])
def data():
    if request.method == 'POST':
        form_data = request.form
        year = form_data['year']
        month = form_data['month']
        day = form_data['day']
    
        date = datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        
        # do the predict
        query = f"""
        SELECT 
        A.forecast_timestamp AS Forecast_Date, 
        A.forecast_value AS Predicted_Transaction_Value, 
        CEIL(B.forecast_value) AS Predicted_Transaction_Count
        FROM 
        (SELECT forecast_timestamp, forecast_value 
        FROM ML.FORECAST(MODEL `project-395518.bitcoin_transaction.value_model`, STRUCT(1000 AS horizon))
        WHERE forecast_timestamp = "{date}") AS A
        JOIN
        (SELECT forecast_timestamp, forecast_value
        FROM ML.FORECAST(MODEL `project-395518.bitcoin_transaction.transaction_count_model`, STRUCT(1000 AS horizon))
        WHERE forecast_timestamp = "{date}") AS B
        ON 
        A.forecast_timestamp = B.forecast_timestamp
        """

        df = bigquery_client.query(query).to_dataframe()
        return render_template('data.html', tables=[df.to_html(index = False, max_rows=20, classes='data')], titles=['predictions'])
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 80)))
