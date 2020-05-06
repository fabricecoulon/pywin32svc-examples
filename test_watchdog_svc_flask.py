"""
Example of a Windows service aka 'SVC' implemented in Python for
a watchdog-like type of service. It monitors a directory for
new files and do something with them.

Requires Python install and separate PyWin32 installation:
(https://github.com/mhammond/pywin32/releases)

pywin32 -- To be able to run as a Windows service

Also requires external Python modules, install them with pip:

watchdog -- To provide the ground file notification functionality
filehash -- To provide a checksum hash of the files
sqlite3worker -- To persist info of what was processed 
    and serialize all sql writes from multiple threads
apsw -- sqlite3 api only that works well with multiple threads
dataclasses -- To make it simple to persist Python objects into db
flask -- To provide insight of what the service is doing via REST
waitress -- Web server to serve flask application
"""
import win32serviceutil
import win32event
import win32service
import servicemanager
import time
import logging
import sys
import os
import unittest
import apsw

from sqlite3worker import Sqlite3Worker
from threading import Lock, Thread
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler
from ctypes import windll
from filehash import FileHash
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from flask import Flask, render_template
from datetime import datetime as dt
from waitress import serve
from enum import Enum

VERSION = '0.7'
WATCHEDDIRNAME = "watcheddir"
WATCHEDDIRPARENT = "C:\\Temp\\watchdog"
# Hardcode the absolute path to the watched directory
WATCHEDDIRPATH = os.path.join(WATCHEDDIRPARENT, WATCHEDDIRNAME)

# Abosulte path to the target directory that contains the processed files
TARGETDIRNAME = "archivedir"
TARGETDIRPARENT = "C:\\Temp\\watchdog"
TARGETDIRPATH = os.path.join(TARGETDIRPARENT, TARGETDIRNAME)

DIRSTOCHECK = [WATCHEDDIRPATH, TARGETDIRPATH]

LOGFILENAME = "watchdog.log"
LOGFILEDIR = os.getenv("TMP") or os.getenv("TEMP")
LOGFILEPATH = os.path.join(LOGFILEDIR, LOGFILENAME)

DBFILENAME = "watchdog.db"
DBFILEPATH = os.path.join(TARGETDIRPARENT, DBFILENAME)

NOT_MODIFIED_SINCE_X_SECONDS = 5.0

#DB_API = 'sqlite3worker'
DB_API = 'apsw'

class DbApiException(Exception):
    pass

class DbApiBase:
    def __init__(self, connection_string, db_api_choice):
        self._conn_string = connection_string
        self._db_api = db_api_choice
        self._db = None

    @property
    def db(self):
        return self._db

    def initdb(self):
        raise NotImplementedError

    def execute(self, tid, *args, **kwargs):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def start_from_thread(self, tid):
        return
    
    def end_from_thread(self, tid):
        return

    def execute_select_with_retry(self, tid, klass, sql, sql_params, retry=3):
        if not sql.strip().lower().startswith('select'):
            raise Exception('Must start with select')
        if not isinstance(sql_params, dict):
            raise Exception('sql parameters must be passed as dict')
        loop_count = 0
        res = None
        while (res is None) and loop_count < retry:
            for row in self.execute(tid, sql, sql_params):
                res = klass(*row)
                break
            else:
                time.sleep(1 + loop_count)
                loop_count += 1
        return res


# To avoid being stuck with the default sqlite3 module which is not
# fit for anything that is multithreaded and because we only intend to
# use sqlite only, we have 2 options:
# 1. sqlite3worker that will serialize everything so that only one thread uses sqlite.
#    The problem with that module is that errors are just strings starting with 'Query returned error'
# 2. apsw (another python sqlite wrapper) that will work as expected (but only for sqlite)
#    This is the favorite approach with real exception converted to our DbApiException
class DbApiSqlite3Worker(DbApiBase):
    def __init__(self, connection_string, db_api_choice='sqlite3worker'):
        super().__init__(connection_string, db_api_choice)
        
    def initdb(self):
        self._db = Sqlite3Worker(self._conn_string)

    def execute(self, tid, *args, **kwargs):
        # tid not used here
        if self._db is None:
            raise Exception('Call initdb first')
        return self._db.execute(*args, **kwargs)

    def close(self):
        if self._db is None:
            raise Exception
        self._db.close()
        self._db = None


class DbApiAPSW(DbApiBase):
    def __init__(self, connection_string, db_api_choice='apsw'):
        super().__init__(connection_string, db_api_choice)
        self._cursors = {}
        
    def initdb(self):
        self._db = apsw.Connection(self._conn_string)

    def close(self):
        if self._db is None:
            raise Exception
        self._db.close()
        self._db = None

    def start_from_thread(self, tid):
        if tid not in self._cursors:
            self._cursors[tid] = self._db.cursor()
        else:
            raise RuntimeError("%s has already an open cursor" % tid)

    def end_from_thread(self, tid):
        if tid in self._cursors:
            self._cursors[tid].close()
        else:
            raise RuntimeError("%s does not have an open cursor" % tid)

    def execute(self, tid, *args, **kwargs):
        if self._db is None:
            raise Exception('Call initdb first')
        if tid not in self._cursors:
            raise RuntimeError("%s does not have an open cursor, call start_from_thread first" % tid)
        try:
            res = self._cursors[tid].execute(*args, **kwargs)
        except (apsw.SQLError, apsw.ConstraintError) as e:
            raise DbApiException(e)
        return res


class DbApiSqlite:
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._dbapi = None
        if (DB_API == 'sqlite3worker'):
            self._klass = DbApiSqlite3Worker
        elif (DB_API == 'apsw'):
            self._klass = DbApiAPSW
        else:
            raise NotImplementedError('Db API not recognized')
    
    def get_dbapi_obj(self):
        self._dbapi_obj = self._klass(self._connection_string)
        self._dbapi_obj.initdb()
        return self._dbapi_obj


class ThreadSafeDict(dict) :
    """
    A threadsafe dictionary that stores the files for which we received notifications
    """
    def __init__(self, * p_arg, ** n_arg) :
        dict.__init__(self, * p_arg, ** n_arg)
        self._lock = Lock()

    def __enter__(self) :
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback) :
        self._lock.release()

ACTIVE_FILES = ThreadSafeDict()
WORKQUEUE = deque()


def is_file_copy_finished(file_path):
    """
    Used to check if the new file for which we received a new file event is completed
    https://stackoverflow.com/questions/32092645/python-watchdog-windows-wait-till-copy-finishes
    """
    finished = False

    GENERIC_WRITE         = 1 << 30
    FILE_SHARE_READ       = 0x00000001
    OPEN_EXISTING         = 3
    FILE_ATTRIBUTE_NORMAL = 0x80

    h_file = windll.Kernel32.CreateFileW(file_path, GENERIC_WRITE, FILE_SHARE_READ, None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None)

    if h_file != -1:
        windll.Kernel32.CloseHandle(h_file)
        finished = True

    return finished

@dataclass
class FilePropsItem:
    id: int
    filepath: str
    hash_type: str
    hash_value: str
    create_t: int
    last_update_t: int
    status: int

@dataclass
class FilePropsItemCT(FilePropsItem):
    newfilepath: str = ''

TABLE_WQ = "workqueue"
TABLE_CT = "completed_tasks"
TABLE_CT_HIST = "completed_tasks_hist"

SQL_CREATE_WQ = """
    CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY,
            filepath TEXT NOT NULL,
            hash_type TEXT NOT NULL,
            hash_value TEXT NOT NULL UNIQUE,
            create_t INTEGER NOT NULL,  -- nb of seconds since epoch
            last_update_t INTEGER NOT NULL, -- nb of seconds since epoch
            status INTEGER NOT NULL
    )
""" % (TABLE_WQ)

SQL_CREATE_CT_FMT = """
    CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY,
            filepath TEXT NOT NULL,
            hash_type TEXT NOT NULL,
            hash_value TEXT NOT NULL UNIQUE,
            create_t INTEGER NOT NULL,  -- nb of seconds since epoch
            last_update_t INTEGER NOT NULL, -- nb of seconds since epoch
            status INTEGER NOT NULL,
            newfilepath TEXT
    )
"""

SQL_CREATE_CT = SQL_CREATE_CT_FMT % (TABLE_CT)
SQL_CREATE_CT_HIST = SQL_CREATE_CT_FMT % (TABLE_CT_HIST)

TABLE_WQ_COLS = "id, filepath, hash_type, hash_value, create_t, last_update_t, status"
TABLE_CT_COLS = TABLE_WQ_COLS + ", newfilepath"

SQL_SELECT_C_FROM_T_WHERE = """
    SELECT %s FROM %s WHERE hash_value = :hash_value
"""

SQL_SELECT_C_FROM_T_WHERE_ID = """
    SELECT %s FROM %s WHERE id = :id
"""

SQL_INSERT_INTO_T = """
    INSERT INTO %s (filepath, hash_type, hash_value, create_t, last_update_t, status) VALUES (:filepath, :hash_type, :hash_value, :create_t, :last_update_t, :status) 
"""

# T1: TABLE_CT , T2: TABLE_CT_HIST
SQL_COPY_FROM_T2_INTO_T1_WHERE_ID = """
    INSERT INTO %s (filepath, newfilepath, hash_type, hash_value, create_t, last_update_t, status)
        SELECT filepath, newfilepath, hash_type, hash_value, create_t, last_update_t, status FROM %s WHERE id = :id
"""

SQL_DELETE_FROM_T_WHERE = """
    DELETE FROM %s WHERE id = :id
"""

SQL_UPDATE_T_SET_F_WHERE_ID = """
    UPDATE %s SET %s = :value WHERE id = :id
"""

SQL_UPDATE_T_SET_F_WHERE_HASHVAL = """
    UPDATE %s SET %s = :value WHERE hash_value = :hash_value
"""

class Status(Enum):
    NA = -1
    SUCCESS = 0
    FAILURE = 1
    PENDING = 2
    UNKNOWN = 10


class FileProperties:
    FILEHASHERS =  { 'md5': FileHash('md5'), 'sha256': FileHash('sha256') }

    def __init__(self, filepath, hash_type='sha256'):
        self._filepath = filepath
        self._complete = False
        self._modified = False
        self._ts = 0
        self._hash_type = hash_type
        try:
            self._hasher = self.FILEHASHERS[self._hash_type]
        except:
            raise Exception('Unsupported hasher')
        self._hash_value = None

    @property
    def complete(self):
        return self._complete

    @complete.setter
    def complete(self, value):
        self._complete = value
        self._ts = time.time()

    @property
    def modified(self):
        return self._modified

    @modified.setter
    def modified(self, value):
        self._modified = value
        if self._modified:
            self._hash_value = None
        self._ts = time.time()

    @property
    def hash_value(self):
        return self._hash_value

    def hash_file(self):
        self._hash_value = self._hasher.hash_file(self._filepath)
        self._ts = time.time()

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, value):
        self._ts = value

    def __str__(self):
        return 'filepath: %s, complete: %s, modified: %s, %s hash: %s' % (
            self._filepath, self._complete, self._modified , self._hash_type, self._hash_value
        )

    def asdataclass(self):
        dc = FilePropsItem(-1, self._filepath, self._hash_type, self._hash_value, self._ts, self._ts, Status.NA.value)
        return dc


class BaseWinservice(win32serviceutil.ServiceFramework):
    """
    A base class for the windows background SVC/service
    """
    _svc_name_ = ''
    _svc_display_name_ = ''
    _svc_description_ = ''

    @classmethod
    def parse_command_line(cls):
        win32serviceutil.HandleCommandLine(cls)

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_requested = False
        self._old_excepthook = sys.excepthook

    def _log(self, msg):
        servicemanager.LogInfoMsg(str(msg))
        logging.info(str(msg))

    def _log2(self, msg):
        logging.info(str(msg))

    def _log_error(self, msg):
        servicemanager.LogErrorMsg(str(msg))
        logging.error(str(msg))

    def _log_error2(self, msg):
        logging.error(str(msg))

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.stop_requested = True

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def _configure_logging(self):
        raise Exception("Override me")

    def main(self):
        self._configure_logging()
        # Replace to use our exception hook
        sys.excepthook = self._svc_excepthook
        self._main()

    def _main(self):
        raise Exception("Override me")

    def _svc_excepthook(self, type, value, traceback):
        logging.error("Unhandled exception occured", exc_info=(type, value, traceback))
        if self._old_excepthook != sys.__excepthook__:
            self._old_excepthook(type, value, traceback)


class CustomEventHandler(FileSystemEventHandler):
    """
    A custom watchdog file system event handler for catching selected events
    """
    def __init__(self, log_func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._log = log_func

    def on_created(self, event):
        super().on_created(event)

        if os.path.isdir(event.src_path):
            self._log('Ignoring: %s' % event.src_path)
            return

        file_path = event.src_path

        if not file_path in ACTIVE_FILES:
            ACTIVE_FILES[file_path] = FileProperties(file_path)

        # Event is received as soon as the file is created, but
        # we have to wait until it is completely written
        while not is_file_copy_finished(file_path):
            time.sleep(1)

        ACTIVE_FILES[file_path].complete = True
        ACTIVE_FILES[file_path].modified = False

        self._log('File: %s. Created and ready' % file_path)

    def on_modified(self, event):
        super().on_modified(event)
        file_path = event.src_path

        if file_path not in ACTIVE_FILES:
            # ignore
            return

        if ACTIVE_FILES[file_path].ts > 0:
            now_ts = time.time()
            last_ts = ACTIVE_FILES[file_path].ts
            dt = now_ts - last_ts
            if dt > .0:
                ACTIVE_FILES[file_path].modified = True

        self._log('File: %s. Modified' % file_path)

    def on_deleted(self, event):
        super().on_deleted(event)
        file_path = event.src_path
        if file_path in ACTIVE_FILES:
            del ACTIVE_FILES[file_path]

        self._log('File: %s. Removed.' % file_path)

class WatchdogSvc(BaseWinservice):
    """
    The main custom watchdog based Windows SVC class with builtin working logging.
    By the way, this took a lot of time to get logging to work...
    https://pythonhosted.org/watchdog/api.html
    https://github.com/gorakhargosh/watchdog/blob/master/src/watchdog/events.py
    """
    _svc_name_ = "WatchdogSvc1"
    _svc_display_name_ = "Watchdog Service"
    _svc_description_ = "Watchdog Service"

    def _configure_logging(self):
        l = logging.getLogger()
        l.setLevel(logging.DEBUG)
        #l.setLevel(logging.ERROR)
        f = logging.Formatter('%(asctime)s %(process)d:%(thread)d %(name)s %(levelname)-8s %(message)s')
        h=logging.StreamHandler(sys.stdout)
        h.setLevel(logging.NOTSET)
        h.setFormatter(f)
        l.addHandler(h)
        h=RotatingFileHandler(LOGFILEPATH,maxBytes=1024**2,backupCount=1)
        h.setLevel(logging.NOTSET)
        h.setFormatter(f)
        l.addHandler(h)

    def _init_db(self):
        dbapi = DbApiSqlite(DBFILEPATH).get_dbapi_obj()
        dbapi.start_from_thread('main')
        return dbapi

    def _main(self):
        self._dbapi = None
        try:
            self._log("Start v%s at %s" % (VERSION, time.ctime()))
            self._log("Start db worker...")
            self._dbapi = self._init_db()
            self._log("Start background threads...")
            # background thread for processing the work in WORKQUEUE
            background_t = Thread(
                target=background_thread_processing,
                args=({'tid': 'bg-1',
                        'log': self._log2,
                        'logerr': self._log_error2,
                        'dbapi': self._dbapi
                },),
                daemon=True
            )
            background_t.start()
            self._log("Start Flask...")
            flask_t = Thread(
                target=flask_thread_processing,
                args=({'tid': 'ft-1',
                        'log': self._log2,
                        'logerr': self._log_error2,
                        'logexc': self._svc_excepthook,
                        'dbapi': self._dbapi
                },),
                daemon=True
            )
            flask_t.start()

            self._log(LOGFILEPATH)
            self._log("Watching dir: %s" % WATCHEDDIRPATH)
            self._log("Target dir: %s" % TARGETDIRPATH)

            for d in DIRSTOCHECK:
                if (not os.path.exists(d)) or (not os.path.isdir(d)):
                    raise Exception('%s does not exists or is not a directory' % d)

            allfiles = []
            for (dirpath, dirnames, filenames) in os.walk(WATCHEDDIRPATH):
                allfiles.extend(filenames)
                break  # Only level inside watcheddir
            if allfiles:
                _allfiles = [os.path.join(dirpath, fname) for fname in allfiles]
                self._log("Adding existing files: %s" % str(_allfiles))
                for f in _allfiles:
                    ACTIVE_FILES[f] = FileProperties(f)
                    ACTIVE_FILES[f].complete = True
                    ACTIVE_FILES[f].modified = True

            event_handler = CustomEventHandler(self._log)
            observer = Observer()
            observer.schedule(event_handler, WATCHEDDIRPATH, recursive=True)
            observer.start()
            while not self.stop_requested:
                keys = list(ACTIVE_FILES.keys())
                for k in keys:
                    mark4removal = False
                    if not ACTIVE_FILES[k].complete:
                        continue

                    if ACTIVE_FILES[k].modified:
                        now_ts = time.time()
                        modified_last_ts = ACTIVE_FILES[k].ts
                        dt = now_ts - modified_last_ts
                        if dt > NOT_MODIFIED_SINCE_X_SECONDS:  # Assume ok if not modified for the last 10s
                            ACTIVE_FILES[k].modified = False
                    elif ACTIVE_FILES[k].hash_value == None:
                        ACTIVE_FILES[k].hash_file()
                        self._add_to_workqueue(ACTIVE_FILES[k])
                        # mark for removal from pending
                        mark4removal = True

                    self._log(str(ACTIVE_FILES[k]))

                    if mark4removal:
                        del ACTIVE_FILES[k]

                time.sleep(1)

            if observer:
                observer.stop()
                observer.join()

            self._log("Stop at %s. Exiting..." % time.ctime())

        except Exception as e:
            self._svc_excepthook(*sys.exc_info())
        
        finally:
            if self._dbapi:
                self._dbapi.end_from_thread('main')
                self._dbapi.close()

    def _add_to_workqueue(self, fp):
        if not isinstance(fp, FileProperties):
            return
        if self._dbapi is None:
            return
        self._log("[DEBUG] Add to workqueue")
        results = self._dbapi.execute('main', "select * from workqueue where hash_value = ?", (fp.hash_value,))
        # One way to detect an error in execute is to check the type of the returned value
        # it is a string, that is an error message otherwise its a sequence/list, it can be empty
        if isinstance(results, str):
            self._log("[DEBUG] returning a string?!")
            return
        found = 0
        for row in results:
            print('[DEBUG]', row)
            found += 1
        if found == 0:
            self._dbapi.execute('main', SQL_INSERT_INTO_T % (TABLE_WQ), asdict(fp.asdataclass()))
        self._log("[DEBUG] Adding %s to wk" % str(fp))
        WORKQUEUE.append(fp.asdataclass())


class BaseBackgroundTask:
    """
    I am a background task, that will be executed by a background thread. In case
    of error/exception during execute(), i will run _on_error followed by _post_exec
    and return False. Otherwise, True on success.
    """
    def __init__(self, argdict):
        self._item = argdict['item']
        self._id = self._item.id
        self._t = argdict['t']
        self._has_error = False
        self._log = argdict['log']

    def _pre_exec(self):
        pass

    def _post_exec(self):
        pass

    def execute(self):
        """
        Execute a task, handles error(s) and returns True on success, False otherwise
        """
        self._log('[%d] processing %s...' % (self._id, str(self._item)))
        self._pre_exec()
        try:
            time.sleep(self._t)
        except Exception as e:
            self._on_error(e)
        else:
            self._on_sucess()
        finally:
            msg = '[%d] done. (has_error = %s)' % (self._id, self._has_error)
            self._log(msg)
        self._post_exec()
        return (not self._has_error, self._item)

    def _on_error(self, e):
        self._has_error = True

    def _on_sucess(self):
        self._has_error = False

MAX_TOKEN = 5
MAX_SLOT_TIME = 60.0
class TokenGenerator:
    """
    I provide tokens, MAX_TOKEN under MAX_SLOT_TIME seconds in order to
    avoid congestion.
    """
    def __init__(self, max_token=MAX_TOKEN, max_slot_time=MAX_SLOT_TIME):
        self._max_token_per_slot = max_token
        self._max_slot_time = max_slot_time
        self._token = 0
        self._last_get_ts = -1
        
        self._token_counter = 0

    def get_token(self):
        res = -1
        if not self._token_available():
            return res
        res = self._token
        self._token += 1
        if self._token_counter == 0:
            self._last_get_ts = time.time()
        self._token_counter += 1
        return res

    def _token_available(self):
        if self._token_counter == 0:
            return True

        curr_ts = time.time()

        if (curr_ts - self._last_get_ts) >= self._max_slot_time:
            self._token_counter = 0

        if self._token_counter < self._max_token_per_slot:
            return True

        return False

    def __repr__(self):
        return "[self._token = %d, self._token_counter =  %d" % (self._token, self._token_counter)

MAX_WORKERS = 3
def background_thread_processing(args):
    """
    I instanciate X workers and I wait until I have a token to pass the work from the
    workqueue to the workers
    """
    log = args['log']
    dbapi = args['dbapi']
    tid = args['tid']
    dbapi.start_from_thread(tid)

    log('This is a test from background_thread_processing')

    pending_workqueue_items = {}
    background_tasks = []

    tg = TokenGenerator(max_token=2, max_slot_time=60.0)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            pending_workqueue_items.clear()
            background_tasks.clear()

            # When there is work to do and some workers are available
            while (len(WORKQUEUE) > 0) and (len(background_tasks) < MAX_WORKERS):
                # Get a token/integer starts with 0
                token = tg.get_token()
                if not (token >= 0):
                    # We did not get a valid token, wait
                    break
                f = WORKQUEUE.popleft()
                # Get the id from the table workqueue to pass it to the background_tasks
                # 2 possible error cases, (1) nothing was returned by the select (race?)
                # (2) an exception was thrown
                itemWQ = None
                try:
                    itemWQ = dbapi.execute_select_with_retry(tid, FilePropsItem, SQL_SELECT_C_FROM_T_WHERE % (TABLE_WQ_COLS, TABLE_WQ), {'hash_value': f.hash_value}, 3)
                    if not isinstance(itemWQ, FilePropsItem):
                        # Error
                        break
                except DbApiException:
                    # Error
                    break
                # Create a bg task pass it a FilePropsItemCT copied from immutable FilePropsItem
                itemCT = FilePropsItemCT(**asdict(itemWQ))
                background_tasks.append(BaseBackgroundTask({'item': itemCT, 't': 5, 'log': log}))
                # Key: ID int, Value: FilePropsItemCT
                pending_workqueue_items[itemWQ.id] = itemCT

            # Create futures for all background_tasks
            futures = [executor.submit(bt.execute) for bt in background_tasks]
            if futures:
                # Wait until all background tasks are completed and save the results into completed tasks table
                for fut in as_completed(futures):
                    res, itemCT = fut.result()
                    log("fut res = %s" % res)
                    # Insert into completed tasks table (a different identity/id might be used) but the hash must be unique
                    # We use the hash_value to know which row to update with the result,etc.
                    log(str(asdict(itemCT)))
                    try:
                        dbapi.execute(tid, SQL_INSERT_INTO_T % (TABLE_CT), asdict(itemCT))
                    except DbApiException as e:
                        log(e)
                    #item_CT_HIST = None
                    #while itemCT_HIST is None:
                    #    time.sleep(0.5)
                    
                    itemCT.newfilepath = 'filepath_after_processing'
                    
                    dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_HASHVAL % (TABLE_CT, 'filepath'), {'value': itemCT.filepath, 'hash_value': itemCT.hash_value})
                    dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_HASHVAL % (TABLE_CT, 'newfilepath'), {'value': itemCT.newfilepath, 'hash_value': itemCT.hash_value})
                    dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_HASHVAL % (TABLE_CT, 'last_update_t'), {'value': time.time(), 'hash_value': itemCT.hash_value})
                    dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_HASHVAL % (TABLE_CT, 'status'),
                        {'value': (Status.SUCCESS.value if res else Status.FAILURE.value), 'hash_value': itemCT.hash_value})
                    # remove item from workqueue
                    for (key, value) in pending_workqueue_items.items():
                        if value == itemCT:
                            dbapi.execute(tid, SQL_DELETE_FROM_T_WHERE % (TABLE_WQ), {'id': key})
                            break

            # Wait
            time.sleep(1)

    # FIXME: should call these for cleanup
    dbapi.end_from_thread(tid)
    dbapi.close()


def flask_thread_processing(args):
    """
    I serve a Flask application
    """
    tid = args['tid']
    log = args['log']
    #logerr = args['logerr']
    logexc = args['logexc']
    dbapi = args['dbapi']
    dbapi.start_from_thread(tid)

    try:

        log('Hello from Flask thread %s' % tid)

        app = Flask('flask_thread_processing')
        @app.route('/')
        def get_root():
            now_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = "%s current time: %s<br>" % (app.name, now_time)
            return msg

        @app.route('/status')
        def get_status():
            now_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            resp = "%s current time: %s<br>" % (app.name, now_time)
            resp += "len(ACTIVE_FILES): %d" % len(ACTIVE_FILES) + '<br>'
            resp += 'ACTIVE_FILES:<br>'
            for af in ACTIVE_FILES:
                resp += "%s<br>" % str(ACTIVE_FILES[af])
            return resp

        @app.route('/test')
        def get_test():
            now_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            resp = "%s current time: %s<br>" % (app.name, now_time)
            #for row in dbapi.execute(tid, "select * from %s" % (TABLE_CT)):
            sql = "SELECT %s FROM %s WHERE last_update_t > %d LIMIT %d" % (TABLE_CT_COLS, TABLE_CT, time.time() - 600, 10)
            for row in dbapi.execute(tid, sql):
                resp += 'get_test: ' + str(row) + '<br>'
                item = FilePropsItemCT(*row)
                resp += "%s<br>" % str(item)
            return resp

        # Flask has its own dev web server not suitable for production

        log('Calling flask app.run...')
        # This does not work (throw an exception under pywin32)
        # See https://github.com/pallets/flask/issues/3447
        #app.run(debug=True, host="127.0.0.1", port=8080, use_reloader=False)
        # That's why we use serve() from another module instead:
        serve(app, host='127.0.0.1', port=8080)


    except Exception:
        logexc(*sys.exc_info())

    finally:
        log('Exiting...')
        dbapi.end_from_thread(tid)
        dbapi.close()


class ThisIsMyUnitTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_dirs(self):
        for d in DIRSTOCHECK:
            self.assertTrue(os.path.isdir(d), '%s does not exists' % d)

    def test_tokengen(self):
        c_max_token = 5
        c_max_slot_t = 0.1
        i = 0
        res = -1
        last_res = -1
        tg = TokenGenerator(max_token=c_max_token, max_slot_time=c_max_slot_t)
        while 1:
            last_res = res
            res = tg.get_token()
            time.sleep(0.001)
            if (i < (c_max_token - 1) or (i > c_max_token and i <= (2*c_max_token))):
                self.assertTrue((res > last_res), '%d %d' % (res, last_res))
            elif (i == c_max_token):
                self.assertTrue((res == -1), '%d' % res)
                time.sleep(c_max_slot_t)
            elif (res > 2*c_max_token):
                break
            i += 1

    def test_db(self):
        tid = 'main'
        dbapi = DbApiSqlite(DBFILEPATH).get_dbapi_obj()
        dbapi.start_from_thread(tid)
        dbapi.execute(tid, SQL_CREATE_WQ)
        dbapi.execute(tid, SQL_CREATE_CT)
        dbapi.execute(tid, SQL_CREATE_CT_HIST)
        dbapi.end_from_thread(tid)
        dbapi.close()

    def test_fileprops(self):
        file1 = FileProperties(DBFILEPATH)
        file1.hash_file()
        asdict(file1.asdataclass())

    def test_error_in_dpapi_execute(self):
        # This will not raise any error/exception, it will only print
        # print an error message starting wing "Query returned error:..."
        tid = 'main'
        dbapi = DbApiSqlite(':memory:').get_dbapi_obj()
        dbapi.start_from_thread(tid)
        try:
            dbapi.execute(tid, 'insert into bad sql')
        except DbApiException:
            pass
        finally:
            # Check the output for "Query returned error"
            dbapi.end_from_thread(tid)
            dbapi.close()

    def _test_create_insert_select_delete_T(self, T):
        dbapi = DbApiSqlite(':memory:').get_dbapi_obj()
        tid = 'main'
        dbapi.start_from_thread(tid)
        #conn.row_factory = sqlite3.Row  #this for getting the column names in dict(row)
        item1 = FilePropsItem(-1, 'dummy_filepath1', 'dummy_hashtype1', 'dummy_hash_value1', 1, 2, Status.SUCCESS.value)
        item2 = FilePropsItem(-1, 'dummy_filepath2', 'dummy_hashtype2', 'dummy_hash_value2', 0, 0, Status.FAILURE.value)
        try:
            # Create
            if T == TABLE_WQ:
                C = TABLE_WQ_COLS
                dbapi.execute(tid, SQL_CREATE_WQ)
                klass = FilePropsItem
            elif T == TABLE_CT:
                C = TABLE_CT_COLS
                dbapi.execute(tid, SQL_CREATE_CT)
                klass = FilePropsItemCT

            # Insert
            for item in [item1, item2]:
                dbapi.execute(tid, SQL_INSERT_INTO_T % (T), asdict(item))

            # Select
            results = dbapi.execute(tid, SQL_SELECT_C_FROM_T_WHERE % (C, T), {'hash_value': 'dummy_hash_value1'})
            self.assertTrue(len(results) == 1)
            item = None
            for row in results:
                item = klass(*row)
                self.assertTrue(isinstance(item, FilePropsItem))
                self.assertTrue(item.id == 1)
                self.assertTrue(item.filepath == 'dummy_filepath1')
                self.assertTrue(item.hash_type == 'dummy_hashtype1')
                self.assertTrue(item.hash_value == 'dummy_hash_value1')
                self.assertTrue(item.create_t == 1)
                self.assertTrue(item.last_update_t == 1)
                self.assertTrue(item.status == Status.SUCCESS.value)
                break
            else:
                raise Exception("No results found")

            # Update
            dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_ID % (T, 'last_update_t'), {'value': time.time(), 'id': item.id})
            dbapi.execute(tid, SQL_UPDATE_T_SET_F_WHERE_HASHVAL % (T, 'last_update_t'), {'value': time.time(), 'hash_value': item.hash_value})

            # Delete
            dbapi.execute(tid, SQL_DELETE_FROM_T_WHERE % (T), {'id': item.id})
            results = dbapi.execute(tid, SQL_SELECT_C_FROM_T_WHERE % (C, T), {'hash_value': 'dummy_hash_value1'})
            self.assertTrue(len(results) == 0)

        except Exception:
            return False
        finally:
            dbapi.end_from_thread(tid)
            dbapi.close()
        return True

    def test_create_insert_select_delete_CT(self):
        return self._test_create_insert_select_delete_T(TABLE_CT)

    def test_create_insert_select_delete_WQ(self):
        return self._test_create_insert_select_delete_T(TABLE_WQ)

    def test_select_with_retry(self):
        tid = 'main'
        dbapi = DbApiSqlite(':memory:').get_dbapi_obj()
        dbapi.start_from_thread(tid)
        dbapi.execute(tid, SQL_CREATE_CT)
        item1 = FilePropsItemCT(-1, 'dummy_filepath', 'dummy_hashtype', 'dummy_hash_value1', 1, 2, Status.SUCCESS.value)
        dbapi.execute(tid, SQL_INSERT_INTO_T % (TABLE_CT), asdict(item1))
        retry = 3
        itemCT = dbapi.execute_select_with_retry(tid, FilePropsItemCT, SQL_SELECT_C_FROM_T_WHERE_ID % (TABLE_CT_COLS, TABLE_CT), {'id': 1}, retry)
        self.assertTrue(itemCT is not None)
        self.assertEqual(itemCT.id, 1)
        self.assertEqual(itemCT.filepath, 'dummy_filepath')
        self.assertEqual(itemCT.hash_type,'dummy_hashtype')
        self.assertEqual(itemCT.hash_value,'dummy_hash_value1')
        self.assertEqual(itemCT.create_t, 1)
        self.assertEqual(itemCT.last_update_t, 2)
        self.assertEqual(itemCT.status, Status.SUCCESS.value)
        dbapi.end_from_thread(tid)
        dbapi.close()

    def _test_copy_from_T_to_T(self, T):
        tid = 'main'
        dbapi = DbApiSqlite(':memory:').get_dbapi_obj()
        dbapi.start_from_thread(tid)
        dbapi.execute(tid, SQL_CREATE_CT)
        dbapi.execute(tid, SQL_CREATE_CT_HIST)
        item1 = FilePropsItemCT(-1, 'dummy_filepath', 'dummy_newfilepath', 'dummy_hashtype', 'dummy_hash_value1', 0, 0, Status.NA.value)
        dbapi.execute(tid, SQL_INSERT_INTO_T % (TABLE_CT), asdict(item1))
        itemCT = None
        itemCT_HIST = None
        retry = 5
        try:
            # This code construct is not necessary when working directly in memory, however, it is used a lot when
            # using the file-based database. Id must be equal to 1 since there was no other rows in the table before
            while (itemCT is None) and (retry != 0):
                for row in dbapi.execute(tid, SQL_SELECT_C_FROM_T_WHERE_ID % (TABLE_CT_COLS, TABLE_CT), {'id': 1}):
                    itemCT = FilePropsItemCT(*row)
                    break
                else:
                    time.sleep(0.5)
                    retry -= 1
            dbapi.execute(tid, SQL_COPY_FROM_T2_INTO_T1_WHERE_ID % (TABLE_CT_HIST, TABLE_CT), {'id': 1})
            retry = 5
            while (itemCT_HIST is None) and (retry != 0):
                for row in dbapi.execute(tid, SQL_SELECT_C_FROM_T_WHERE_ID % (TABLE_CT_COLS, TABLE_CT_HIST), {'id': 1}):
                    itemCT_HIST = FilePropsItemCT(*row)
                    break
                else:
                    time.sleep(0.5)
                    retry -= 1
            self.assertTrue(itemCT_HIST is not None)
            #print(asdict(itemCT_HIST))
        finally:
            dbapi.end_from_thread(tid)
            dbapi.close()

    def test_copy_from_CT_to_CT(self):
        return self._test_copy_from_T_to_T(TABLE_CT)

    def test_copy_from_WQ_to_WQ(self):
        return self._test_copy_from_T_to_T(TABLE_WQ)


if __name__ == '__main__':
    _cls = WatchdogSvc
    if len(sys.argv) == 1 and \
            sys.argv[0].endswith('.exe') and \
            not sys.argv[0].endswith(r'win32\PythonService.exe'):
        # invoked as non-pywin32-PythonService.exe executable without
        # arguments
        # Initialize the service manager and start our service.
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(_cls)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # invoked with arguments, or without arguments as a regular
        # Python script  
        # We support a "help" command that isn't supported by
        # `win32serviceutil.HandleCommandLine` so there's a way for
        # users who run this script from a PyInstaller executable to see
        # help. `win32serviceutil.HandleCommandLine` shows help when
        # invoked with no arguments
        if len(sys.argv) == 2:
            if sys.argv[1] == 'help':
                sys.argv = sys.argv[:1]
            elif (sys.argv[1] == 'test') or (sys.argv[1] == 'tests'):
                unittest.main(argv=['first-arg-is-ignored'], exit=True)
        _cls.parse_command_line()
