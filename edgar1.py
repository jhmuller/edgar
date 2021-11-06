import sys
import os
import datetime
import re
import urllib.request
import requests
from collections import namedtuple
import pandas as pd

def get_year(dt):
    return dt.year

def get_quarter(dt):
    qtr = int(dt.month / 3) + 1
    return qtr

def get_url_resp(url):
    try:
        headers = {"User-Agent": "Enter The Data john@enterthedata.com",
                   "Accept-Encoding": "gzip, deflate",
                   "Host": "www.sec.gov"}

        resp = requests.get(url, headers = headers)
    except Exception as e:
        print(sys.exc_info())
        raise RuntimeError(str(e))
    return resp


def get_daily_forms(dt, form_type='13', verbosity=0):
    year = get_year(dt)
    qtr = get_quarter(dt)
    datestr = dt.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{datestr}.idx"
    resp = get_url_resp(url)
    return resp


def filter_forms(resp, form_type='13'):
    txt = resp.text
    print(type(txt))
    lines = txt.split("\n")
    for li, line in enumerate(lines):
        if li == 8:
            print(line)
            pass
        if line.startswith("Form"):
            break
    colnames = line.split()
    data = []
    for line in lines[li:]:
        #line = line[1:-2]
        if line.startswith("13"):
            flds = re.split("\s\s+", line)
            flds = [f for f in flds if len(f) > 0]

            data.append(flds)

        df = pd.DataFrame(data, columns=["form", "company", "CIK", "date", "url"])
    return df

def get_forms(df):
    for idx, ser in df.iterrows():
        url = f"https://www.sec.gov/Archives/" + ser["url"]
        resp = get_url_resp(url)
        print(resp.text)
        res = read_html(resp.text)
        return resp.text

def read_html(html):
    import re
    import pandas as pd
    import xml.etree.ElementTree as ET
    import io
    try:
        match = re.search(r"<\?xml", html)
        start, end = match.span()
        xmlstr = html[start:]
        if False:
            lines = html.split("\n")
            for li, line in enumerate(lines):
                if re.search("xml", line):
                    break
            xmllines = lines[li:]
            xmlstr = '\n'.join(xmllines)


        def iter_docs(author):
            author_attr = author.attrib
            for doc in author.iter('document'):
                doc_dict = author_attr.copy()
                doc_dict.update(doc.attrib)
                doc_dict['data'] = doc.text
                yield doc_dict

        xml_data = io.StringIO(xmlstr)

        etree = ET.parse(xml_data)  # create an ElementTree object
        doc_df = pd.DataFrame(list(iter_docs(etree.getroot())))
        x = pd.read_xml(xml)

        DF_13F = pd.read_html(html)
        DF_13F = DF_13F[-1]
        DF_13F = DF_13F.iloc[2:]
        new_header = DF_13F.iloc[0]

        new_header = DF_13F.iloc[0]
        DF_13F.columns = new_header
        DF_13F = DF_13F.iloc[1:]
        DF_13F['date_reported'] = datetime.now().strftime("%Y%m")
        DF_13F['cik_company'] = cik_company
        value_to_store_as_index = DF_13F['CUSIP'] + cik_company + datetime.now().strftime("%Y%m")
        DF_13F['indice'] = value_to_store_as_index
        DF_13F = DF_13F[
            ['indice', 'NAME OF ISSUER', 'TITLE OF CLASS', 'CUSIP', '(x$1000)', 'PRN AMT', 'PRN', 'date_reported',
             'cik_company']]
        print('new SEC13 report added to DataFrame')
        print(DF_13F)
    except Exception as e:
        print(sys.exc_info())
        raise RuntimeException(e)

    def get_filings(urls):
        for url in urls:
            url = f"https://www.sec.gov/Archives/"+url

if __name__ == "__main__":
    verbosity = 2
    dt = datetime.datetime(2021, 7, 1)
    resp = get_daily_forms(dt, verbosity=verbosity)
    forms_df = filter_forms(resp)
    get_forms(forms_df)
