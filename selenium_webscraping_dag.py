from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")

driver = webdriver.Chrome(options=options)

URL = "https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions"

default_args = {
    'owner': 'mulyo',
    'tries': 5,
    'try_delay': timedelta(minutes=2)
}

@dag(
    dag_id = 'selenium_webscraping_dag',
    description = 'DAG for web scraping with selenium',
    default_args = default_args,
    start_date = datetime(2025, 4, 23),
    schedule_interval = '@daily',
    catchup = False
)

def selenium_webscrapring():

    @task()
    def dataExtraction():

        driver.get(URL)
        time.sleep(3)

        country = driver.find_elements(By.XPATH, '//*[@id="mw-content-text"]/div[1]/table[1]')

        datas = list()

        for cnt in country:
            rows = cnt.find_elements(By.TAG_NAME, "tr")
            datas = [row.text for row in rows]

        new_data = [item.split() for item in datas]
        driver.quit()

        return new_data

    @task()
    def copyOfData(new_data):

        countries = new_data[3:214]
        for i, j in enumerate(countries):
            if(len(j) > 5):
                x = len(j) - 4
                countries[i] = [' '.join(countries[i][:x])] + [countries[i][x:]]

        return countries

    @task()
    def rowName(new_data):

        if len(new_data) > 1:
            des = new_data[0] + new_data[1]
            description = [des[0], ''.join(des[1:5]), ' '.join(des[5:7])+' '+des[15], ''.join(des[5:7])+' '+des[16], ' '.join(des[11:15])]
        else:
            description = []
        return description

    @task()
    def toCsv(description, countries):

        df = pd.DataFrame(countries, columns=description)
        df.to_csv("/home/mulyo/airflow_venv/dags/CO2 Emission by countries.csv", index=False)


    new_data = dataExtraction()
    countries = copyOfData(new_data)
    description = rowName(new_data)
    toCsv(description, countries)

selenium_webscrapring = selenium_webscrapring()

