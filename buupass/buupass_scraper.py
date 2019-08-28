#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests, re, datetime, urllib.parse
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
import psycopg2, os, time
from multiprocessing.pool import ThreadPool

"""
def getconn():
    conn = psycopg2.connect(dbname=*redacted*, user=*redacted*,
                            password=*redacted*,
                            host=*redacted*,
                            port=*redacted*, sslmode='require')
    return conn



engine = create_engine('postgresql+psycopg2://', creator=getconn)
"""
def get_datelist():
    base_time = datetime.datetime.today()
    base_time = base_time.replace(hour=0, second=0, minute=0, microsecond=0)
    date_list = [base_time + datetime.timedelta(days=x) for x in range(1, 15)]
    date_list = [unformatted.strftime("%Y-%m-%d") for unformatted in date_list]
    return date_list

print("getting date list")
date_list = get_datelist()
date_list


def get_soup(origin, destination, date):
    base_url = "https://buupass.com/Booking/search?from={}&to={}&departure_date={}"
    url = base_url.format(origin, destination, date)
    c = requests.get(url, "html.parse")
    c = c.content
    soup = bs(c,features="lxml")
    return soup


def get_ticket_info(soup):
    stuff = soup.findAll("article")
    bus_co = [thing.h4.get_text().split(" - ",3) for thing in stuff]
    #print(bus_co)
    price = [re.findall('\d+',thing.find("span", {"class":"price listprice"}).get_text())[0] for thing in stuff]
    #print(price)
    seats = [re.findall('\d+',thing.find("div", {"class":"action"}).get_text().strip().replace("\n","").replace("SOLD OUT","0")) for thing in stuff if thing]
    seats = [int(seat[0]) if len(seat)>0 else 0 for seat in seats]
    #print(seats)
    route_info = [{"bus_co":bus, "origin":origin, "destination":dest} for origin, dest, bus in bus_co]
    #print(route_info)
    meta = [{"price":price, "seats_remaining":seat, "created_at": datetime.datetime.today()} for price, seat in zip(price, seats)]
    #print(meta)
    info = [thing.find("div", {"class":"time"}) for thing in stuff]
    #print(info)
    headers = [thing.findAll("span", {"class":"skin-color"}) for thing in info]
    headers = [[head.get_text().strip() for head in header] for header in headers]
    #print(headers)
    datas = [thing.findAll("span", {"class":"search_data_values"}) for thing in info]
    datas = [[datum.get_text().strip() for datum in data] for data in datas]
    #print(datas)
    finale = [dict(zip(header, data)) for header, data in zip(headers, datas)]
    #print(finale)
    ready = [{**route, **meta, **finale} for route, meta, finale in zip(route_info, meta, finale)]
    #print(ready)
    return ready

print("First Iteration")
df = pd.DataFrame(
    get_ticket_info(
        get_soup("Nairobi", "Arusha", date_list[0])
    )
)
df["search_origin"] = "Nairobi"
df["search_destination"] = "Arusha"
df["search_date"] = date_list[0]
df

#no longer using this function
def get_places(soup):
    places = soup.find_all("select")
    _from = places[0].findAll("option")
    _from = [place['value'] for place in _from]
    _to = places[1].findAll("option")
    _to = [place['value'] for place in _to]
    _from = [urllib.parse.quote(fro) for fro in _from if fro]
    _to = [urllib.parse.quote(to) for to in _to if to]
    return [_from, _to]

print("Getting from/to pairs from CSV")
_from_to = pd.read_csv("home/tyler/buupass/buupass_places.csv")
_from_to_date = []
for date in date_list:
    temp = _from_to
    temp['date'] = date
    _from_to_date.extend(temp.values.tolist())

#engine.dispose()


def df_loops(_list):
    origin, destination, date = _list
    soup = get_soup(origin,destination,date)
    new_df = pd.DataFrame(get_ticket_info(soup))
    new_df["search_origin"] = origin
    new_df["search_destination"] = destination
    new_df["search_date"] = date
    return new_df

csv_name = "/home/tyler/bus_analysis/buupass/buupass"+datetime.datetime.today().strftime("%Y-%m-%d")+".csv"
startTime = datetime.datetime.now()

print("Starting loop at " + str(startTime)) 

with ThreadPool(10) as pool:
    for result in pool.map(df_loops, _from_to_date):
        df = df.append(result, ignore_index=True, sort=True)
        df.to_csv(csv_name)

print("It took "+str(datetime.datetime.now() - startTime)+" to run this script")
df


df.drop_duplicates(keep="first", inplace=True)
#df["created_at"] = datetime.datetime.today()
df.columns = map(str.lower, df.columns)

df.to_csv(csv_name)
print("Final write of csv")

#df.to_sql('buupass', engine, if_exists='append', index=False)
#engine.dispose()

#print("Saved to DB")
print("Finished")