import os
import datetime
import re
import psutil
from pathlib import PurePath
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor
import threading
import xml.etree.ElementTree as ET
import io
from utilities import Utilities

def holdings_to_pandas(etree, fname, outLogName, errLogName, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
        logger.info("{0} {1}".format(Utilities.get_fname(), fname))
        logger.info(f"  pid: {os.getpid()}")
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
        msg = f"{fname}, {Utilities.get_fname()} empty dataframe"
        for lname in [outLogName, errLogName]:
            logger = logging.getLogger(lname)
            logger.warning(msg)
        return df
    try:
        ncols = ["value", "sshPrnamt", "Sole", "Shared", "None"]
        for ncol in ncols:
            df[ncol] = df[ncol].astype(int)
        df["value"] = df["value"] * 1000
        sumval = float(df["value"].sum())
        df["wt"] = df["value"]/sumval
        df["perSh"] = df["value"] / df["sshPrnamt"]
        return df
    except:
        msg = f"{fname}, {Utilities.get_fname()}  error processing holdings2pandas"
        msg += Utilities.err_info()
        Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)


def parallel_parse(sdir, outLogName, errLogName, ncpu=None, verbosity=0):
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        logger.info(f"{Utilities.get_fname()}  ddir: {sdir}  {Utilities.now()}")

    if ncpu is None:
        ncpu = psutil.cpu_count()

    txtfiles = [f for f in os.listdir(sdir) if f.endswith(".txt")]
    nrows = 20
    start = 0
    end = nrows
    futures = []
    ranges = []
    func = parse_forms
    pool_executor = ProcessPoolExecutor
    fi = 0
    files = {}
    with pool_executor(max_workers=ncpu) as executor:
        while start < len(txtfiles):
            try:
                #print(start)
                files[fi] = txtfiles[start:end]
                future =  executor.submit(func, txtfiles=files[fi], sdir=sdir, verbosity=verbosity,
                                          outLogName=outLogName, errLogName=errLogName)
                futures.append(future)
                ranges.append((start, end))
                #print(len(futures), start, end)
                start = end
                end = min(end+nrows, len(txtfiles))
                fi += 1
            except:
                msg = Utilities.err_info()
                Utilities.log_msg(level=logging.ERROR, loggers=[executor, outLogName], msg=msg)
    ndone = 0
    while ndone < len(futures):
        ndone = 0
        for i, f in enumerate(futures):
            if not f.done():
                msg = f"not done with batch {i}"
            else:
                ndone += 1
    return

def parse_forms(sdir, outLogName, errLogName,
                txtfiles, verbosity=0, files=None):
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        msg = f"{Utilities.get_fname()}  ddir: {sdir}  {Utilities.now()}"
        msg += f"  pid: {os.getpid()}  threadid: {threading.get_ident()}"
        msg += txtfiles[0]
        print(msg)

    # the daily file is in basedir
    # but the txt files are in basedir/ddir or fdir
    # confusing!
    ppath = PurePath(sdir)
    pparts = ppath.parts
    year, month, day = pparts[1:4]
    month = month.zfill(2)
    day = day.zfill(2)
    tparts = list(pparts[:3]) + [f"secFilings_{year}{month}{day}.csv"]
    tpath = PurePath(*tparts)
    if not os.path.isfile(tpath):
        msg = f"can't find {tpath}"
        Utilities.log_msg(msg=msg, loggers=[errLogName, outLogName], level=logging.INFO)
        return -1
    dailydf = pd.read_csv(tpath)
    for ti, fname in enumerate(txtfiles):
        if verbosity > 1:
            msg = f"{ti}, {fname}"
            Utilities.log_msg(msg=msg, loggers=[errLogName, outLogName], level=logging.INFO)
        try:
            fparts = os.path.splitext(fname)[0].split("_")
            CIK = fparts[len(fparts)-2][3:]
            CIK = int(CIK)
            fid = fparts[len(fparts)-1][3:]
            cdf = dailydf.loc[dailydf["CIK"] == CIK]
            entry = cdf.loc[cdf["fid"] == fid]
        except:
            msg = f"{fname}, {Utilities.get_fname()}  error parsing"
            msg += Utilities.err_info()
            Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        fpath = os.path.join(sdir, fname)
        try:
            with open(fpath, "r") as fp:
                html_orig = fp.read()
            html_fixed = fixup(html_orig, outLogName, errLogName, verbosity=verbosity)
            outname = os.path.splitext(fname)[0]+ "_fixed.txt"
            outpath = os.path.join(sdir,  outname)
            with open(outpath, "w") as fp:
                fp.write(html_fixed)
            def extract_key_values(text, keys=None, sep=":"):
                res = {}
                for key in keys:
                    realkey = "("+key+sep+")"+"(.*)(\n)"
                    match = re.search(realkey, text)
                    if match:
                        value = match.group(2).strip()
                        res[key] = value
                return res
            hdf = parse_form(html_fixed, fname=fname, outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
            if not isinstance(hdf, pd.DataFrame) or hdf.shape[0] == 0:
                continue

            keys = ["ACCEPTANCE-DATETIME", "STATE", "CITY"]
            keyvals = extract_key_values(html_fixed, keys=keys)
            hdf["year"] = int(year)
            hdf["month"] = int(month)
            hdf["day"] = int(day)
            hdf["filingDt"] = datetime.datetime(int(year), int(month), int(day))
            try:
                key = "ACCEPTANCE-DATETIME"
                filingDt = datetime.datetime.strptime(keyvals[key], "%Y%m%d%H%M%S")
                hdf["filingDt"] = filingDt
            except:
                key = "ACCEPTANCE-DATETIME"
                msg = f" bad Acceptance-datetime {keyvals[key]}"
                Utilities.log_msg(msg=msg, loggers=[errLogName, outLogName], level=logging.WARNING)

            if not isinstance(hdf, pd.DataFrame):
                logger = logging.getLogger("forms")
                logger.warning(f"{fname}, None from parse_form")
                continue
            elif hdf.shape[0] == 0:
                logger = logging.getLogger("forms")
                logger.warning(f"{fname}, empty dataframe from parse_form ")
                continue
            else:
                csvname = os.path.splitext(fname)[0] + "_" + str(CIK) + "_" + str(fid) + ".csv"
                csvpath = os.path.join(sdir, csvname)
                if verbosity > 0:
                    print(csvpath)
                hdf.to_csv(csvpath, index=None)
        except Exception as e:
            msg = f"{fname}, {Utilities.get_fname()}  error parsing"
            msg += Utilities.err_info()
            Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
    return

def fixup(html, outLogName, errLogName, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
        logger.info("{0} {1}".format(Utilities.get_fname()))

    try:
        # this string does not have closing tag
        if verbosity > 1:
            logger.info("  {0} replacing texts".format(Utilities.get_fname()))
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
        msg = f"{Utilities.get_fname()}  error parsing"
        msg += Utilities.err_info()
        Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return None

def parse_form(html, fname, outLogName, errLogName, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
        logger.info("{0} {1}".format(Utilities.get_fname()))

    try:
        lines = html.split("\n")
        xml_data = io.StringIO(html)
        etree = ET.parse(xml_data)  # create an ElementTree object
        df = holdings_to_pandas(etree, fname, outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
        return df
    except Exception as e:
        with open("html2.txt", 'w') as fp:
            fp.write(html)
        msg = f"{Utilities.get_fname()}  error iterparse"
        msg += Utilities.err_info()
        Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return None

if __name__ == "__main__":
    verbosity = 1
    outLogName = "parseOut"
    errLogName = "parseErr"
    Utilities.setup_logging(outLogName=outLogName, errLogName=errLogName)
    basedir = "./data"
    sdirs = Utilities.sub_dirs_with_files(basedir, fname_incl=".txt")
    # for now only 13F files
    sdirs = [x for x in sdirs if re.search("13F", x)]
    sdirs = sorted(sdirs)
    for sdir in sdirs:
        ppath = PurePath(sdir)
        pparts = ppath.parts
        print(pparts)
        for lname in [outLogName, errLogName]:
            logger = logging.getLogger(lname)
            logger.info(f"--{sdir}--")
        try:
            parallel_parse(sdir=sdir, outLogName=outLogName,
                           errLogName=errLogName, verbosity=1, ncpu=None)
        except Exception as e:
            print(Utilities.err_info())
            print("")
    print("done {0}".format(datetime.datetime.now()))

