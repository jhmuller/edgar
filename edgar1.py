import sys
import os
import datetime
import re
import requests
import copy
import pandas as pd
import logging


def err_info():
    etype, err, tb = sys.exc_info()
    frame = tb.tb_frame
    fstr = str(frame).split(",")
    res = f"{fstr[1]} {fstr[3]} line {tb.tb_lineno}, {etype}, {err}"
    return res

def get_fname(i=1):
    return sys._getframe(i).f_code.co_name

def get_month(dt):
    return dt.month

def get_year(dt):
    return dt.year

def get_quarter(dt):
    qtr = int((dt.month-1) / 3) + 1
    return qtr

def get_dt_str(dt, fmt="%Y%m%d"):
    return dt.strftime(fmt)

def log_msg(msg, loggers=["main", "forms"], level="WARNING"):
    try:
        for lname in loggers:
            logger = logging.getLogger(lname)
            logger.log(msg, level=level)
    except:
        msg = err_info()
        logger = logging.getLogger("main")
        logger.error(msg)
    return

def get_url_resp(url):
    try:
        headers = {"User-Agent": "Enter The Data john@enterthedata.com",
                   "Accept-Encoding": "gzip, deflate",
                   "Host": "www.sec.gov"}

        resp = requests.get(url, headers = headers)
    except Exception as e:
        msg = get_fname() + " "
        msg += err_info()
        log_msg(msg, loggers=["main", "forms"], level="ERROR")
    return resp


def filter_forms_df(lines, form_filter=None, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger("main")
        logger.info("{0}".format(get_fname()))
    for li, line in enumerate(lines):
        if verbosity > 1:
            logger = logging.getLogger("main")
            logger.info("skippling line {0}: {1}".format(li, line))
        if line.startswith("Form"):
            break
    data = []
    for i, line in enumerate(lines[li+1:]):
        if verbosity > 1:
            print(f"line {i}:  {line}")
        if line.startswith(form_filter):
            flds = re.split("\s\s+", line)
            flds = [f.strip() for f in flds if len(f) > 0]
            data.append(flds)
    df = pd.DataFrame(data, columns=["form", "company", "CIK", "date", "url"])
    return df

def get_daily_forms(dt, form_filter=None, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger("main")
        logger.info(f"{get_fname()}  ddir: {dt}  form_filter {form_filter}")
    year = get_year(dt)
    qtr = get_quarter(dt)
    datestr = dt.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{datestr}.idx"
    resp = get_url_resp(url)
    lines = resp.text.split("\n")
    if len(lines) == 0:
        msg = f"0 lines downloaded year: {year} qtr: {qtr} dt: {datestr} {url}"
        log_msg(msg, loggers=["main", "forms"])
        return pd.DataFrame()

    for li, line in enumerate(lines):
        if verbosity > 2:
            print("skippling line {0}: {1}".format(li, line))
        if line.startswith("Form"):
            break
    data = []
    for i, line in enumerate(lines[li+1:]):
        if verbosity > 2:
            print(f"line {i}:  {line}")
        if form_filter and line.startswith(form_filter):
            flds = re.split("\s\s+", line)
            flds = [f.strip() for f in flds if len(f) > 0]
            data.append(flds)
    df = pd.DataFrame(data, columns=["form", "company", "CIK", "date", "url"])
    df.to_csv(f"dailyForms_{form_filter}_{datestr}.csv")
    if df.shape[0] == 0:
        msg = f"empty df for year: {year} qtr: {qtr} dt: {datestr} {url}"
        log_msg(msg, loggers=["main", "forms"])
    return df


def download_forms(formsdf, ddir, verbosity=0):
    import time
    if verbosity > 0:
        logger = logging.getLogger("main")
        logger.info(f"{get_fname()}  ddir: {ddir}")

    if not os.path.isdir(ddir):
        os.mkdir(ddir)

    for i, (idx, ser) in enumerate(formsdf.iterrows()):
        time.sleep(0.5)
        url = f"https://www.sec.gov/Archives/" + ser["url"]
        resp = get_url_resp(url)
        try:
            cname = ser["company"].replace(" ","-")
            cname = cname.replace("\\","_")
            cname = cname.replace("/","_")
            if verbosity > 0:
                if i % 20 == 0:
                    print(f"<{i}, {cname}>")
            fpath = os.path.join(ddir, cname+".txt")
            try:
                with open(fpath, 'wt') as fp:
                    fp.write(resp.text)
            except Exception as e1:
                msg = get_fname() + " "
                msg += ser["company"]
                msg += err_info()
                for lname in ["main", "forms"]:
                    logger = logging.getLogger(lname)
                    logger.error(msg)
        except:
            msg = get_fname() + " "
            msg += ser["company"]
            msg += err_info()
            log_msg(msg, loggers=["main", "forms"], level="ERROR")
    return

def holdings_to_pandas(etree, fname, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger("main")
        logger.info("{0} {1}".format(get_fname(), fname))
    table = False
    tups = []
    tup = {}
    for p in etree.iter() :
        if re.search("infoTable", p.tag):
          table = True
          if len(tup.keys()) > 0:
            tups.append(tup)
            tup = {}
        elif table:
          tag = re.sub("{.*}", "", p.tag).strip()
          tag.replace("\n","")
          tag.replace("\t","")
          if len(tag) > 0:
            tup[tag] = p.text

    tups.append(tup)

    df = pd.DataFrame(tups)
    if df.empty:
        msg = f"{fname}, {get_fname()} empty dataframe"
        for lname in ["main", "forms"]:
            logger = logging.getLogger(lname)
            logger.warning(msg)
        return None
    try:
        ncols = ["value", "sshPrnamt", "Sole", "Shared", "None"]
        for ncol in ncols:
            df[ncol] = df[ncol].astype(int)
        df["value"] = df["value"] * 1000
        sumval = float(df["value"].sum())
        df["wt"] = df["value"]/sumval
        return df
    except:
        msg = f"{fname}, {get_fname()}  error processing holdings2pandas"
        msg += err_info()
        log_msg(msg, loggers=["main", "forms"], level="ERROR")

def parse_forms(ddir, verbosity):
    if verbosity > 0:
        logger = logging.getLogger("main")
        logger.info(f"{get_fname()}  ddir: {ddir}")
    if not os.path.isdir(ddir):
        raise(RuntimeError("ERR: no directory {ddir}"))
    txtfiles = [f for f in os.listdir(ddir) if f.endswith(".txt")]
    for ti, fname in enumerate(txtfiles):
        if verbosity > 1:
            print(f"{ti}, {fname}")
        fpath = os.path.join(ddir, fname)
        try:
            with open(fpath, "r") as fp:
                data = fp.read()
            hdf = parse_form(data, fname=fname, verbosity=verbosity)
            if not isinstance(hdf, pd.DataFrame):
                logger = logging.getLogger("forms")
                logger.warning(f"{fname}, None from parse_form")
                continue
            elif hdf.shape[0] == 0:
                logger = logging.getLogger("forms")
                logger.warning(f"{fname}, empty dataframe from parse_form ")
                continue
            else:
                csvname = os.path.splitext(fname)[0] + ".csv"
                csvpath = os.path.join(ddir, csvname)
                hdf.to_csv(csvpath)
        except Exception as e:
            msg = f"{fname}, {get_fname()}  error parsing"
            msg += err_info()
            log_msg(msg, loggers=["main", "forms"], level="ERROR")
    return

def fixup(html, fname, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger("main")
        logger.info("{0} {1}".format(get_fname(), fname))

    try:
        # this string does not have closing tag
        if verbosity > 1:
            logger.info("  {0} replacing texts".format(get_fname()))
        html = html.replace("<ACCEPTANCE-DATETIME>", "ACCEPTANCE-DATETIME")
        html = re.sub("<\?xml.*\?>", "", html)
        html = re.sub("&", "and", html)
        html = re.sub("<ACCEPTANCE-DATETIME>", "ACCEPTANCE-DATETIME: ", html)

        # now fixup knows issues with missing tags

        noends = ["TYPE", "SEQUENCE", "FILENAME", "DESCRIPTION"]
        tups = []
        for ne in noends:
            ms = [*re.finditer(f"<({ne})>([^<]*)(<.*>)", html)]
            for i, m in enumerate(ms):
                one = f"</{m.group(1)}>"
                three = m.group(3)
                tups.append((m.start(), m.end(), m.group(1), one,  m.group(3)))
        rdf = pd.DataFrame(tups, columns=["start", "end", "grp1", "one", "grp3"])
        rdf.sort_values(by="start", inplace=True)
        newhtml = ''
        laststart = 0
        for idx, row in rdf.iterrows():
            if row["one"] != row["grp3"]:
                pos = row["end"] - len(row["grp3"])
                newstr = row["one"] + "\n"
                newhtml += html[laststart:pos] + newstr
                laststart = pos
        newhtml += html[laststart:]  # m.group(3).start()] + f"</{m.group(1)}>"
        return newhtml
    except:
        msg = f"{get_fname()}  error parsing"
        msg += err_info()
        log_msg(msg, loggers=["main", "forms"], level="ERROR")
        return None

def notused():
    html = ''
    try:
        if verbosity > 0:
            msg = "  {0} getting tags".format(get_fname())
            logging.info(msg)
        tags = [*re.finditer("<([^<]*)>", html)]
        tups = []
        for ti, tag in enumerate(tags):
            start = tag.start()
            end = tag.end()
            txt = html[start:end]
            if re.search("ns1", txt):
                continue
            tups.append((start, end, txt))
        df = pd.DataFrame(tups, columns=["start", "end", "txt"])
        if df.shape[0] > 10000:
            logger = logging.getLogger("main")
            logger.info("df.shape really big")
        df["next"] = "ok"
        if verbosity > 0:
            msg = "  {0} df shape {1}".format(get_fname(), df.shape)
            msg += "  {0} iterating df rows".format(get_fname())
            logger = logging.getLogger("main")
            logger.info(msg)
        for idx, row in df.iterrows():
            if row["txt"][1] == "/":
                df.at[idx, "next"] = "OK"
                continue
            endtag = "</" + row["txt"][1:]
            es = df.loc[df['txt'] == endtag]["start"]
            if es.shape[0] == 0:
                df.at[idx, "next"] = "**"

        data = copy.copy(html)

        if verbosity > 0:
            print("  {0} replacing texts {1}".format(get_fname(), df.shape))
        newdata = ''
        nn_txt = None
        last_start = 0
        for idx, row in df.iterrows():
            if nn_txt is not None:
                pos = row["start"]
                if verbosity > 1:
                    print(f"adding {nn_txt} at start {last_start} to  {pos} before {row['txt']}  idx {idx}")
                newdata += data[last_start:pos]
                newdata += nn_txt + "\n"
                last_start = pos
            if row["next"] == "**":
                if row["txt"].startswith("<?"):
                    continue
                if row["txt"].startswith("<edgarSub"):
                    continue
                if row["txt"].startswith("<ns1"):
                    continue
                if row["txt"].startswith("<informationTab"):
                    continue
                nn_txt = "</" + row["txt"][1:]
            else:
                nn_txt = None
        newdata += data[last_start:]
        return newdata
    except:
        msg = f"{get_fname()}  error parsing"
        msg += err_info()
        for lname in ["main", "forms"]:
            logger = logging.getLogger(lname)
            logger.error(msg)
        return None

def parse_form(html, fname, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger("main")
        logger.info("{0} {1}".format(get_fname(), fname))
    import xml.etree.ElementTree as ET
    import io
    html2 = fixup(html, fname, verbosity=verbosity)
    try:
        lines = html2.split("\n")
        xml_data = io.StringIO(html2)
        etree = ET.parse(xml_data)  # create an ElementTree object
        df = holdings_to_pandas(etree, fname, verbosity=verbosity)
        return df
    except Exception as e:
        with open("html2.txt", 'w') as fp:
            fp.write(html2)
        msg = f"{get_fname()}  error iterparse"
        msg += err_info()
        log_msg(msg, loggers=["main", "forms"], level="ERROR")
        return None


def get_filings(urls):
    for url in urls:
        url = f"https://www.sec.gov/Archives/"+url

def setup_logging(mainName="main", formsName="forms"):

    dt = datetime.datetime.now()
    mainlogfilename = os.path.splitext(__file__)[0] + "_" + dt.strftime("%Y%m%d") + ".log"
    formslogfilename = "forms_" + dt.strftime("%Y%m%d") + ".log"
    mainLogger = logging.getLogger(mainName)
    formsLogger = logging.getLogger(formsName)

    mainLogger.setLevel(logging.DEBUG)
    formsLogger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    formsFh = logging.FileHandler(formslogfilename)
    formsFh.setLevel(logging.DEBUG)

    mainFh = logging.FileHandler(mainlogfilename)
    mainFh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    formsFh.setFormatter(formatter)
    mainFh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    mainLogger.addHandler(mainFh)
    formsLogger.addHandler(formsFh)

    mainLogger.addHandler(ch)


def test_logging():
    for logname in ["main", "forms"]:
        logger = logging.getLogger(logname)
        logger.info("info")
        logger.debug("debug")
        logger.warning("warning")
        logger.error("error")

if __name__ == "__main__":
    verbosity = 1
    setup_logging()
    #test_logging()
    base = datetime.datetime.today()
    #base = datetime.datetime(2021, 10,2)
    numdays = 45
    date_list = [base - datetime.timedelta(days=x) for x in range(numdays)]
    for dt in date_list:
        weekday = dt.weekday()
        if weekday >= 5:
            continue
        ddir = dt.strftime("%Y%m%d")
        for lname in ["main", "forms"]:
            logger = logging.getLogger(lname)
            logger.info(f"--{ddir}--")
        try:
            formsdf = get_daily_forms(dt, form_filter='13', verbosity=verbosity)
            download_forms(formsdf, ddir=ddir, verbosity=verbosity)
            parse_forms(ddir=ddir, verbosity=1)
        except Exception as e:
            print(err_info())
    print("done {0}".format(datetime.datetime.now()))

