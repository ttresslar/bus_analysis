#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime


# In[2]:


thing = pd.read_html("http://www.dar-express.co.tz/website/search?")
thing=thing[0]
thing["origin_searched"] = "blank"
thing["destination_searched"] = "blank"
thing["date_searched"] = "blank"
base_df = thing.truncate(after=-1)


# In[3]:


base_url = "http://www.dar-express.co.tz/website/search?start_point={}&end_point={}&date={}&fleet_type=1"
arusha = "1"
karatu = "9"
dar = "4"
moshi = "5"
nairobi = "3"
rombo = "2"
cities = [arusha,karatu,dar,moshi,nairobi,rombo]


# In[4]:


base_time = datetime.datetime.today()
base_time = base_time.replace(hour=0, second=0, minute=0, microsecond=0)
date_list = [base_time + datetime.timedelta(days=x) for x in range(1, 31)]
date_list = [date.strftime("%Y-%m-%d") for date in date_list]
date_list


# In[5]:

print("Scraping Dar Express")
for origin in cities:
    for destination in cities:
        for date in date_list:
            url = base_url.format(origin, destination, date)
            data = pd.read_html(url)
            data = data[0] #ticket data is in table 0, the next table is a legend I think
            #data["origin_searched"] = origin
            #data["destination_searched"] = destination
            data["date_searched"] = date
            base_df = base_df.append(data, ignore_index=True, sort=True)


# In[6]:


base_df = base_df[base_df['Adult Fare'] !='No trip avaiable']


# In[7]:


#base_df.Departure = base_df.Departure.str.extract('(\d+)')
base_df.Duration = base_df.Duration.str.extract('(\d+)')
base_df.Distance = base_df.Distance.str.extract('(\d+)')
base_df.Departure = base_df.Departure.str.extract('(\d{2}:\d{2}:\d{2})')
base_df["Adult Fare"] = base_df["Adult Fare"].str.extract('(\d+)')
base_df["Children Fare"] = base_df["Children Fare"].str.extract('(\d+)')
base_df["Special Price"] = base_df["Special Price"].str.extract('(\d+)')
base_df["seats_remaining"] = base_df.Operator.str.extract('(\d+)')
base_df.Arrival = base_df.Arrival.str.extract('(\d{2}:\d{2}:\d{2})')


# In[9]:


temp = base_df.Route.str.split(" to ", n=1, expand=True)
base_df['origin'] = temp[0]
base_df['destination'] =temp[1]
base_df['created_at'] = datetime.datetime.today()

# In[10]:


base_df.columns = map(str.lower, base_df.columns)

# In[9]:


base_df.to_csv('/home/tyler/bus_scraper/dar-express_routes/dar-express_routes'+datetime.datetime.today().strftime("%Y-%m-%d")+'.csv')
print("Wrote results to csv")




# In[11]:
"""

from sqlalchemy import create_engine
import sqlalchemy.pool as pool
import psycopg2, os, time

def getconn():
    conn = psycopg2.connect(dbname=*redacted*, user=*redacted*,
                            password=*redacted*,
                            host=*redacted*,
                            port=*redacted*, sslmode='require')
    return conn


# In[13]:


engine = create_engine('postgresql+psycopg2://', creator=getconn)
base_df.to_sql('darexpress', engine, if_exists='append', index=False)
engine.dispose()

print("Saved in DB")

"""
