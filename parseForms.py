
import os
import datetime
import re
import psutil
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import threading
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
        return None
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


def parallel_parse(basedir, ddir,  outLogName, errLogName, ncpu=None, verbosity=0):
    if ncpu is None:
        ncpu = psutil.cpu_count()
    fdir = os.path.join(basedir, ddir)
    if not os.path.isdir(fdir):
        os.makedirs(fdir)
    txtfiles = [f for f in os.listdir(fdir) if f.endswith(".txt")]
    nrows = 20
    start = 0
    end = nrows
    futures = []
    ranges = []
    func = parse_forms
    pool_executor = ProcessPoolExecutor
    fi = 0
    with pool_executor(max_workers=ncpu) as executor:
        while start < len(txtfiles):
            try:
                print(start)
                files = txtfiles[start:end]
                future =  executor.submit(func, txtfiles=txtfiles[start:end], basedir=basedir, ddir=ddir, verbosity=verbosity,
                                          outLogName=outLogName, errLogName=errLogName)
                futures.append(future)
                ranges.append((start, end))
                print(len(futures), start, end)
                start = end
                end = min(end+nrows, len(txtfiles))
            except:
                msg = Utilities.err_info()
                Utilities.log_msg(level=logging.ERROR, msg=msg)
    ndone = 0
    while ndone < len(futures):
        ndone = 0
        for i, f in enumerate(futures):
            if not f.done():
                msg = f"not done with batch {i}"
            else:
                ndone += 1
    return

def parse_forms(basedir, ddir, outLogName, errLogName,
                txtfiles=None, verbosity=0, files=None):
    if verbosity > 0:
        logger = logging.getLogger(outLogName)
        logger.info(f"{Utilities.get_fname()}  ddir: {ddir}  basedir {basedir} {Utilities.now()}")
        logger.info(f"  pid: {os.getpid()}  threadid: {threading.get_ident()}")
    fdir = os.path.join(basedir, ddir)
    if not os.path.isdir(fdir):
        os.makedirs(fdir)
    if txtfiles is None:
        txtfiles = [f for f in os.listdir(fdir) if f.endswith(".txt")]
    # the daily file is in basedir
    # but the txt files are in basedir/ddir or fdir
    # confusing!
    dailydf = pd.read_csv(os.path.join(basedir, f"dailyForms_13_{ddir}.csv"))

    for ti, fname in enumerate(txtfiles):
        if verbosity > 1:
            msg = f"{ti}, {fname}"
            Utilities.log_msg(msg=msg, level=logging.INFO)
        try:
            fparts = os.path.splitext(fname)[0].split("_")
            CIK = fparts[len(fparts)-2]
            CIK = int(CIK)
            fid = fparts[len(fparts)-1]
            cdf = dailydf.loc[dailydf["CIK"] == CIK]
            entry = cdf.loc[cdf["fid"] == fid]
        except:
            msg = f"{fname}, {Utilities.get_fname()}  error parsing"
            msg += Utilities.err_info()
            Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        fpath = os.path.join(basedir, ddir, fname)
        try:
            with open(fpath, "r") as fp:
                data = fp.read()
            hdf = parse_form(data, fname=fname, outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
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
                csvpath = os.path.join(basedir, ddir, csvname)
                hdf.to_csv(csvpath, index=None)
        except Exception as e:
            msg = f"{fname}, {Utilities.get_fname()}  error parsing"
            msg += Utilities.err_info()
            Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
    return

def fixup(html, fname, outLogName, errLogName, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
        logger.info("{0} {1}".format(Utilities.get_fname(), fname))

    try:
        # this string does not have closing tag
        if verbosity > 1:
            logger.info("  {0} replacing texts".format(Utilities.get_fname()))
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
        msg = f"{Utilities.get_fname()}  error parsing"
        msg += Utilities.err_info()
        Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return None

def parse_form(html, fname, outLogName, errLogName, verbosity=0):
    if verbosity > 1:
        logger = logging.getLogger(outLogName)
        logger.info("{0} {1}".format(Utilities.get_fname(), fname))
    import xml.etree.ElementTree as ET
    import io
    html2 = fixup(html, fname, outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
    try:
        lines = html2.split("\n")
        xml_data = io.StringIO(html2)
        etree = ET.parse(xml_data)  # create an ElementTree object
        df = holdings_to_pandas(etree, fname, outLogName=outLogName, errLogName=errLogName, verbosity=verbosity)
        return df
    except Exception as e:
        with open("html2.txt", 'w') as fp:
            fp.write(html2)
        msg = f"{Utilities.get_fname()}  error iterparse"
        msg += Utilities.err_info()
        Utilities.log_msg(msg, loggers=[outLogName, errLogName], level=logging.ERROR)
        return None

if __name__ == "__main__":
    verbosity = 1
    outLogName = "dnldOut"
    errLogName = "dnldErr"
    Utilities.setup_logging(outLogName=outLogName, errLogName=errLogName)
    basedir = "./data"
    ddirs = [x for x in os.listdir(basedir) if os.path.isdir(os.path.join(basedir,x))]
    ddirs = sorted(ddirs, reverse=True)
    for ddir in ddirs:
        if ddir < '20211110':
            break
        for lname in [outLogName, errLogName]:
            logger = logging.getLogger(lname)
            logger.info(f"--{ddir}--")
        try:
            parallel_parse(basedir=basedir, ddir=ddir, outLogName=outLogName,
                           errLogName=errLogName, verbosity=1, ncpu=None)
        except Exception as e:
            print(Utilities.err_info())
            print("")
    print("done {0}".format(datetime.datetime.now()))

