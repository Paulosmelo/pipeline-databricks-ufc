# Databricks notebook source
import requests
from bs4 import BeautifulSoup
import pandas as pd
import string
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

def fetch_page(requests_session, url):
    response = requests_session.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch page. Status code: {response.status_code}")
        return None

# COMMAND ----------

def extract_fighter_data_from_table(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    table = soup.select_one('.b-statistics__table') 
    fighters = []

    headers = []
    th =  table.find_all('th')
    for i in th:
        title = i.text
        headers.append(title.strip(' \n\t'))

    headers.insert(0, 'id')
    df = pd.DataFrame(columns = headers)
        
    rows = table.find_all('tr')[1:]
    del rows[:1]

    for j in rows:
        row_data = j.find_all('td')
        a = j.select('.b-link')[0]
        url  = a.get('href').strip()
        id = url.rsplit('/', 1)[-1]
        row = [i.text for i in row_data]
        row.insert(0, id)
        length = len(df)
        df.loc[length] = row

    return df


# COMMAND ----------

alphabet = list(string.ascii_lowercase)
dataframes = []
requests_session = requests.Session()

for letter in alphabet:    
    base_url = f'http://ufcstats.com/statistics/fighters?char={letter}&page=all'
    page_content = fetch_page(requests_session, base_url)

    if page_content:
        fighters_data = extract_fighter_data_from_table(page_content)
        dataframes.append(fighters_data)


df = pd.concat(dataframes)
display(df)

# COMMAND ----------

def extract_fighter_details(id):
    url = f'http://ufcstats.com/fighter-details/{id}'
    html_content = fetch_page(url)
    soup = BeautifulSoup(html_content, 'lxml')
    details = soup.select('.b-list__box-list-item') 
    fighter_data = {}

    for item in details:
        stat = item.text.split(':')
        if len(stat) > 1:
            tittle = stat[0].strip(' \n\t"')
            value = stat[1].strip(' \n\t"')          
            fighter_data[tittle] = value
            fighter_data['id'] = id

    return fighter_data

# COMMAND ----------

fighters_id = df['id']
details = []
total = len(df)
done = 0
requests_session = requests.Session()

for id in fighters_id:
    base_url = f'http://ufcstats.com/fighter-details/{id}'
    page_content = fetch_page(requests_session, base_url)
    
    if page_content:
        fighter_data = extract_fighter_details(page_content)
        fighter_data['id'] = id
        details.append(fighter_data)
        done+=1
        print(done/total*100)

df_details=  pd.DataFrame(details)


# COMMAND ----------

df_final = pd.merge(df,df_details, how="left", on="id")

# COMMAND ----------

df_spark =spark.createDataFrame(df_final) 

# COMMAND ----------

df_cols =  [col.lower() for col in list(df_spark.columns)]

# COMMAND ----------

duplicate_col_index = [idx for idx, 
  val in enumerate(df_cols) if val in df_cols[:idx]] 
  
for i in duplicate_col_index: 
    df_cols[i] = df_cols[i] + '_duplicate'

# COMMAND ----------

df_spark = df_spark.toDF(*df_cols) 

# COMMAND ----------

path = 'dbfs:/mnt/data/raw/fighters'

df_spark.write\
        .mode('Overwrite')\
        .format('parquet')\
        .save(path)
