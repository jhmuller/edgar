import sys
import os
import re
import collections
import datetime
import logging

class Utilities(object):
    @staticmethod
    def now():
        import datetime
        return datetime.datetime.now()

    @staticmethod
    def err_info():
        try:
            etype, err, tb = sys.exc_info()
            frame = tb.tb_frame
            fstr = str(frame).split(",")
            res = f"{fstr[1]} {fstr[3]} line {tb.tb_lineno}, {etype}, {err}"
            return res
        except:
            msg = str(sys.exc_info())
            raise (RuntimeError("Error getting err_info" + msg))

    @staticmethod
    def get_fname(i=1):
        return sys._getframe(i).f_code.co_name

    @staticmethod
    def get_month(dt):
        return dt.month

    @staticmethod
    def get_year(dt):
        return dt.year

    @staticmethod
    def get_quarter(dt):
        qtr = int((dt.month - 1) / 3) + 1
        return qtr

    @staticmethod
    def get_dt_str(dt, fmt="%Y%m%d"):
        return dt.strftime(fmt)

    @staticmethod
    def sub_dirs_with_files(ddir, fname_incl=None):
        from collections import deque
        import re
        res = []
        que = deque()
        que.appendleft(ddir)
        while len(que) > 0:
            popdir = que.pop()
            alls = [x for x in os.listdir(popdir)]
            files = [f for f in alls if os.path.isfile(os.path.join(popdir, f))]
            if fname_incl:
                files = [x for x in files if re.search(fname_incl, x)]
            if len(files) > 0:
                res.append(popdir)
            dirs = [os.path.join(popdir, x) for x in alls if os.path.isdir(os.path.join(popdir, x))]
            que.extend(dirs)
        return res

    @staticmethod
    def log_msg(msg, loggers, level=logging.WARNING):
        try:
            if not isinstance(loggers, list) or not isinstance(loggers, tuple):
                msg = f"{Utilities.get_fname()} loggers should be a list"
                raise RuntimeError(msg)
            for lname in loggers:
                logger = logging.getLogger(lname)
                logger.log(level, msg)
        except:
            msg = Utilities.err_info()
            raise (RuntimeError("Error writing to loggers" + msg))
        return

    @staticmethod
    def setup_logging(outLogName="dnldOut", errLogName="dnldErr",
                      outLevel=logging.INFO, errLevel=logging.ERROR):
        dt = datetime.datetime.now()
        outLogfilename = outLogName + "_" + dt.strftime("%Y%m%d") + ".log"
        errLogfilename = errLogName + dt.strftime("%Y%m%d") + ".log"
        outLogger = logging.getLogger(outLogName)
        errLogger = logging.getLogger(errLogName)

        outLogger.setLevel(outLevel)
        errLogger.setLevel(errLevel)

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
