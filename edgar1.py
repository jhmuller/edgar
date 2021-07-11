import sys
import os
import datetime

def get_year(dt):
    return dt.year

def get_quarter(dt):
    qtr = int(dt.month / 3) + 1
    return qtr

def get_daily_forms(dt, verbosity=0):
    import urllib.request
    import requests
    year = get_year(dt)
    qtr = get_quarter(dt)
    datestr = dt.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{datestr}.idx"
    try:
        r = requests.get(url)
    except Exception as e:
        print(sys.exc_info())
        raise RuntimeError(str(e))
    print(f"{type(r)}")
    print(r)
    return r

if __name__ == "__main__":
    verbosity = 2
    dt = datetime.datetime(2021, 7, 1)
    res = get_daily_forms(dt, verbosity=verbosity)