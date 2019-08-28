#!/home/tyler/miniconda3/bin/python3

print("starting")
import pandas as pd
print("pandas")

import requests, re, datetime
print("requests")
print("imported libraries")


def get_datelist():
    base_time = datetime.datetime.today()
    base_time = base_time.replace(hour=0, second=0, minute=0, microsecond=0)
    date_list = [base_time + datetime.timedelta(days=x) for x in range(1, 31)]
    date_list = [unformatted.strftime("%Y-%m-%d") for unformatted in date_list]
    return date_list

print("getting date list")
date_list = get_datelist()
date_list

from bs4 import BeautifulSoup as bs
print("bs")

print("get soup")
def get_soup(origin, destination, date):
    base_url = "https://www.shabiby.co.tz/schedule?from={}&to={}&date={}"
    c = requests.get(base_url.format(origin, destination, date), "html.parse")
    c = c.content
    soup = bs(c,features="lxml")
    return soup

print("get places")
def get_places(soup):
    places = soup.find_all("option")
    places = [place.get_text() for place in places]
    places = list(dict.fromkeys(places))
    return places

print("get headers")
def get_headers_and_dates(soup):
    stuff = soup.findAll("div", {"class": "schedule-item-card"})
    headers = [thing.find_all('small') for thing in stuff]
    things, dates = [[],[]]
    for header in headers:
        new_thing = [item.get_text().replace("\n", '').strip() for item in header]
        dates.append(new_thing[0])
        new_thing = [new_thing[i] for i in [1,2,4]]
        things.append(new_thing)
    return [things, dates]

print("data and routes")
def get_data_and_routes(soup):
    stuff = soup.findAll("div", {"class": "schedule-item-card"})
    data = [thing.find_all("p", {"class":"small"}) for thing in stuff]
    my_data,routes = [[],[]]

    for datum in data:
        new_data = [atom.get_text().replace("\n", '').strip() for atom in datum]
        routes.append(new_data[0])
        new_data = new_data[1:4]
        my_data.append(new_data)
    return [my_data, routes]


print("progress_bar")
def get_progress_bar(soup):
    stuff = soup.findAll("div", {"class": "schedule-item-card"})
    progress = [sub.findAll("div", {"class":"progress-bar progress-bar-danger"}) for sub in stuff]
    progress = [re.findall('\d+', pro.get_text()) for prog in progress if prog for pro in prog ]
    progress = [int(pro)/100 for prog in progress for pro in prog]
    return progress


print("assemble_dicts")
def assemble_dicts(things, my_data, dates, routes, progress):
    b = []
    for (thing, my_datum, date, route, prog) in zip(things, my_data, dates, routes, progress):
        a = dict(zip(thing,my_datum))
        a["Date"] = date
        a["Time"], a["Weekday"] = re.split(r'\s{2,}', a["DEPARTURE"])
        a["Origin"], a["Destination"] = route.split(" - ")
        a["FARE"] = re.sub("\D", "", a["FARE"])
        a["passengers"] = prog
        b.append(a)
    return b


# In[10]:

print("Scraping Shabiby")
soup = get_soup("Dodoma", "Dar es Salaam", date_list[0])
places = get_places(soup)
places


# In[11]:


things, dates = get_headers_and_dates(soup)
data, routes = get_data_and_routes(soup)
progress = get_progress_bar(soup)
df = pd.DataFrame(assemble_dicts(things, data, dates, routes, progress))
df["search_origin"] = "Dodoma"
df["search_destination"] = "Dar es Salaam"
df["search_date"] = date_list[0]
df


# In[12]:


for origin in places:
    for destination in places:
        for date in date_list:
            soup = get_soup(origin,destination,date)
            things, dates = get_headers_and_dates(soup)
            progress = get_progress_bar(soup)
            if things == []:
                continue
            data, routes = get_data_and_routes(soup)
            new_df = pd.DataFrame(assemble_dicts(things, data, dates, routes, progress))
            new_df["search_origin"] = origin
            new_df["search_destination"] = destination
            new_df["search_date"] = date
            df = df.append(new_df, ignore_index=True, sort=True)
df


# In[13]:


df.drop_duplicates(keep="first", inplace=True)
df["created_at"] = datetime.datetime.today()
#df.drop("DEPARTURE", axis=1, inplace=True)
df.columns = map(str.lower, df.columns)


# In[14]:


df.to_csv("/home/tyler/bus_analysis/shabiby/shabiby"+datetime.datetime.today().strftime("%Y-%m-%d")+".csv")
print("Saved to csv")

# In[15]:

"""

from sqlalchemy import create_engine
import sqlalchemy.pool as pool
import psycopg2, os, time

# In[16]:


def getconn():
    conn = psycopg2.connect(dbname=*redacted*, user=*redacted*,
                            password=*redacted*,
                            host=*redacted*,
                            port=*redacted*, sslmode='require')
    return conn



engine = create_engine('postgresql+psycopg2://', creator=getconn)
df.to_sql('shabiby', engine, if_exists='append', index=False)
engine.dispose()

print("Saved to DB")
"""