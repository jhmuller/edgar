import sys
import os
import datetime
import re
import requests
import copy
import psutil
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import threading
from time import time
from http.client import HTTPSConnection

def processor_intensive(arg):
    def fib(n): # recursive, processor intensive calculation (avoid n > 36)
        return fib(n-1) + fib(n-2) if n > 1 else n
    start = time()
    result = fib(arg)
    return time() - start, result

def io_bound(arg):
    start = time()
    con = HTTPSConnection(arg)
    con.request('GET', '/')
    result = con.getresponse().getcode()
    return time() - start, result

def manager(PoolExecutor, func, *args, **kwargs):
    timings, results = list(), list()
    start = time()
    with PoolExecutor(max_workers=2) as executor:
        try:
            future =  executor.submit(func, *args, **kwargs)
            print(type(future))
            print(future.result())
            pass
            # put results into correct output list:
            #timings.append(timing), results.append(result)
        except:
            msg = " "
            msg += err_info()
            log_msg(level=logging.ERROR, msg=msg)
    finish = time()
    print(f'{func.__name__}, {PoolExecutor.__name__}')
    print(f'wall time to execute: {finish-start}')
    #print(f'total of timings for each call: {sum(timings)}')
    #print(f'time saved by parallelizing: {sum(timings) - (finish-start)}')
    #print(dict(zip(inputs, results)), end = '\n\n')


def now():
    import datetime
    return datetime.datetime.now()

def err_info():
    try:
        etype, err, tb = sys.exc_info()
        frame = tb.tb_frame
        fstr = str(frame).split(",")
        res = f"{fstr[1]} {fstr[3]} line {tb.tb_lineno}, {etype}, {err}"
        return res
    except:
        msg = str(sys.exc_info())
        raise(RuntimeError("Error getting err_info"+msg))

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

def log_msg(msg, loggers=[], level=logging.WARNING):
    try:
        for lname in loggers:
            logger = logging.getLogger(lname)
            logger.log(level, msg)
    except:
        msg = err_info()
        raise(RuntimeError("Error writing to loggers"+msg))
    return

def get_url_resp(url, outLogName, errLogName):
    try:
        headers = {"User-Agent": "Enter The Data john@enterthedata.com",
                   "Accept-Encoding": "gzip, deflate",
                   "Host": "www.sec.gov"}
        res = requests.get(url, headers = headers, timeout=2)
        return res
    except requests.exceptions.ConnectionError:
        msg = "ConnectionError "
        msg += get_fname()
        msg += err_info()
        log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return msg
    except requests.exceptions.ReadTimeout:
        msg = "Timeout Error, maybe try again "
        msg += get_fname()
        msg += err_info()
        log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return None
    except Exception as e:
        msg = "Error "
        msg += get_fname()
        if "res" in locals():
            pass
            #msg += f"res: {str(res)}\n"
        msg += err_info()
        log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return msg


def filter_forms_df(lines,  outLogName, errLogName, form_filter=None, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
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
            pass
            #print(f"line {i}:  {line}")
        if line.startswith(form_filter):
            flds = re.split("\s\s+", line)
            flds = [f.strip() for f in flds if len(f) > 0]
            data.append(flds)
    df = pd.DataFrame(data, columns=["form", "company", "CIK", "date", "url"])
    return df

def get_daily_forms(dt, basedir, outLogName, errLogName, maxTries=4, form_filter=None, verbosity=0):
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        logger.info(f"{get_fname()}  ddir: {dt}  form_filter {form_filter}")
    try:
        year = get_year(dt)
        qtr = get_quarter(dt)
        datestr = dt.strftime("%Y%m%d")
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{qtr}/form.{datestr}.idx"
        resp = None
        cnt = 0
        while resp == None:
            if cnt >= maxTries:
                break
            cnt += 1
            resp = get_url_resp(url, outLogName=outLogName, errLogName=errLogName)
        lines = resp.text.split("\n")
    except:
        msg = get_fname() + " "
        msg += err_info()
        log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)


    if len(lines) == 0:
        msg = f"0 lines downloaded year: {year} qtr: {qtr} dt: {datestr} {url}"
        log_msg(msg, loggers=["main", "forms"])
        return pd.DataFrame()
    try:
        for li, line in enumerate(lines):
            if verbosity > 2:
                print("skippling line {0}: {1}".format(li, line))
            if line.startswith("Form"):
                break
        data = []
        segments = [(0,12), (12,74), (74,86), (86,98), (98,142)]
        for i, line in enumerate(lines[li+1:]):
            if verbosity > 2:
                print(f"line {i}:  {line}")
            if form_filter and line.startswith(form_filter):
                flds = []
                for start, end in segments:
                    fld = line[start:end]
                    flds.append(fld.strip())
                #flds = re.split("\s\s+", line)
                #flds = [f.strip() for f in flds if len(f) > 0]
                data.append(flds)
        df = pd.DataFrame(data, columns=["form", "company", "CIK", "date", "url"])
        if df.shape[0] == 0:
            msg = "Empty dataframe"
            log_msg(msg=msg, level=logging.WARNING, loggers=[outLogName, errLogName])
            return df
        df["fid"] = df["url"].apply(lambda x: os.path.splitext(x)[0].split("/")[-1])
        fname = f"dailyForms_{form_filter}_{datestr}.csv"
        if not os.path.isdir(basedir):
            os.mkdir(basedir)
        fpath = os.path.join(basedir, fname)
        df.to_csv(fpath, index=None)
        if df.shape[0] == 0:
            msg = f"empty df for year: {year} qtr: {qtr} dt: {datestr} {url}"
            log_msg(msg=msg, level=logging.WARNING, loggers=[outLogName, errLogName])
        return df
    except:
        msg = get_fname() + " "
        msg += err_info()
        log_msg(msg=msg, level=logging.ERROR, loggers=[outLogName, errLogName])

def parallel_download(formsdf, basedir, ddir, outLogName, errLogName, ncpu=None,
                      atATime=300,
                      verbosity=0):
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        logger.info(f"{get_fname()}  ddir: {ddir}  atATime {atATime}")
    if ncpu is None:
        ncpu = psutil.cpu_count()
    nrows = atATime
    start = 0
    end = nrows
    futures = []
    ranges = []
    pool_executor = ThreadPoolExecutor
    fi = 0
    with pool_executor(max_workers=ncpu) as executor:
        while start < formsdf.shape[0]:
            try:
                print(start)
                subdf = formsdf.iloc[start:end]
                future =  executor.submit(download_forms, formsdf=subdf, basedir=basedir, ddir=ddir,
                                          outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
                futures.append(future)
                ranges.append((start, end))
                print(len(futures), start, end)
                start = end
                end = min(end+nrows, formsdf.shape[0])
            except:
                msg = err_info()
                log_msg(level=logging.ERROR, msg=msg)
    ndone = 0
    while ndone < len(futures):
        ndone = 0
        for i, f in enumerate(futures):
            if not f.done():
                msg = f"not done with batch {i}"
            else:
                ndone += 1
    return

def download_forms(formsdf, basedir,  ddir, outLogName, errLogName, verbosity=0):
    import time
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        logger.info(f"<{get_fname()}>  ddir: {ddir} shape formsdf {formsdf.shape}  {now()}")
        logger.info(f"  pid: {os.getpid()}  thread id {threading.get_ident()}")
    if formsdf.shape[0] == 0:
        log_msg(msg=f"no forms for {ddir}", level=logging.INFO)
        return

    savedir = os.path.join(basedir, ddir)
    if not os.path.isdir(savedir):
        os.mkdir(savedir)

    for i, (idx, ser) in enumerate(formsdf.iterrows()):
        time.sleep(0.5)
        url = f"https://www.sec.gov/Archives/" + ser["url"]
        try:
            resp = get_url_resp(url, outLogName=outLogName, errLogName=errLogName)
        except requests.exceptions.ConnectionError:
            msg = "ConnectionError: "
            msg += err_info()
            log_msg(level=logging.ERROR, msg=msg, loggers=[outLogName, errLogName])
            return None
        except Exception as e:
            msg = "other error "
            msg += err_info()
            log_msg(level=logging.ERROR, msg=msg, loggers=[outLogName, errLogName])
            return None
        CIK = ser["CIK"]
        uparts = os.path.splitext(url)[0].split("/")
        fid = uparts[len(uparts)-1]
        try:
            cname = ser["company"].replace(" ","-")
            cname = cname.replace("\\","_")
            cname = cname.replace("/","_")
            cname = cname.replace(",","_")
            if verbosity > 0:
                if i % 20 == 0:
                    print(f"<{i}, {cname}>")
            fname = f"{cname}_{CIK}_{fid}.txt"
            fpath = os.path.join(savedir, fname)
            try:
                with open(fpath, 'wt') as fp:
                    fp.write(resp.text)
            except Exception as e1:
                msg = get_fname() + " "
                msg += fname
                msg += err_info()
                log_msg(level=logging.ERROR, msg=msg, loggers=[outLogName, errLogName])
        except:
            msg = get_fname() + " "
            msg += ser["company"]
            msg += err_info()
            log_msg(level=logging.ERROR, msg=msg, loggers=[outLogName, errLogName])
    return

def get_filings(urls):
    for url in urls:
        url = f"https://www.sec.gov/Archives/"+url

def setup_logging(outLogName="dnldOut", errLogName="dnldErr"):

    dt = datetime.datetime.now()
    outLogfilename = outLogName + "_" + dt.strftime("%Y%m%d") + ".log"
    errLogfilename = errLogName + dt.strftime("%Y%m%d") + ".log"
    outLogger = logging.getLogger(outLogName)
    errLogger = logging.getLogger(errLogName)

    outLogger.setLevel(logging.DEBUG)
    errLogger.setLevel(logging.ERROR)

    # create file handler which logs even debug messages
    errFh = logging.FileHandler(errLogfilename)
    errFh.setLevel(logging.DEBUG)

    outFh = logging.FileHandler(outLogfilename)
    outFh.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    errFh.setFormatter(formatter)
    outFh.setFormatter(formatter)

    # add the handlers to the logger
    outLogger.addHandler(outFh)
    errLogger.addHandler(errFh)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    outLogger.addHandler(ch)

if __name__ == "__main__":
    verbosity = 1
    outLogName = "downloadOut"
    errLogName = "downloadErr"
    setup_logging(outLogName=outLogName, errLogName=errLogName)
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    base = datetime.datetime.today()
    #base = datetime.datetime(2021, 3, 4)

    numdays = 3
    basedir = "data"
    date_list = [base - datetime.timedelta(days=x) for x in range(numdays)]
    for dt in date_list:
        weekday = dt.weekday()
        if weekday >= 5:
            pass
        ddir = dt.strftime("%Y%m%d")
        for lname in ["main", "forms"]:
            logger = logging.getLogger(lname)
            logger.info(f"--{ddir}--")
        try:
            formsdf = get_daily_forms(dt, basedir=basedir, form_filter='13',
                                      outLogName= outLogName, errLogName=errLogName, verbosity=verbosity)
            if not isinstance(formsdf, pd.DataFrame):
                msg = f" dt {dt} formsdf not a dataframe"
                log_msg(msg=msg, level=logging.WARNING, loggers = [outLogName, errLogName])
            if formsdf.shape[0] > 0:
                download_forms(formsdf, basedir, ddir,
                               outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
        except Exception as e:
            print(err_info())
            print("")
    print("done {0}".format(datetime.datetime.now()))

