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
sqllite3 -- To persist info of what was processed
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
import sqlite3

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

VERSION = '0.6'
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


class FileProperties:
    """
    Holds the properties of the files that we are interested in
    """
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
        f = logging.Formatter('%(asctime)s %(process)d:%(thread)d %(name)s %(levelname)-8s %(message)s')
        h=logging.StreamHandler(sys.stdout)
        h.setLevel(logging.NOTSET)
        h.setFormatter(f)
        l.addHandler(h)
        h=RotatingFileHandler(LOGFILEPATH,maxBytes=1024**2,backupCount=1)
        h.setLevel(logging.NOTSET)
        h.setFormatter(f)
        l.addHandler(h)

    def _main(self):
        try:
            self._log("Start background threads...")
            # background thread for processing the work in WORKQUEUE
            background_t = Thread(target=background_thread_processing, args=({'tid': 'bg-1', 'log': self._log2, 'logerr': self._log_error2},), daemon=True)
            background_t.start()
            flask_t = Thread(
                target=flask_thread_processing,
                args=({'tid': 'ft-1', 'log': self._log2, 'logerr': self._log_error2, 'logexc': self._svc_excepthook},),
                daemon=True
            )
            flask_t.start()
            self._log("Start background threads DONE")

            self._log("Start v%s at %s" % (VERSION, time.ctime()))
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
                        # apped to workqueue
                        WORKQUEUE.append(k)
                        WORKQUEUE.append(k)
                        WORKQUEUE.append(k)
                        WORKQUEUE.append(k)
                        WORKQUEUE.append(k)
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

class BaseBackgroundTask:
    """
    I am a background task, that will be executed by a background thread. In case
    of error/exception during execute(), i will run _on_error followed by _post_exec
    and return False. Otherwise, True on success.
    """
    def __init__(self, argdict):
        self._id = argdict['id']
        self._f = argdict['f']
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
        self._log('[%d] processing %s...' % (self._id, str(self._f)))
        self._pre_exec()
        try:
            time.sleep(self._t)
        except Exception as e:
            self._on_error(e)
        else:
            self._on_sucess()
        finally:
            msg = '[%d] done. (%s)' % (self._id, self._has_error)
            self._log(msg)
        self._post_exec()
        return not self._has_error

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
    log = args['log'] or None

    log('This is a test from background_thread_processing')

    background_tasks = []
    id = 0

    tg = TokenGenerator(max_token=2, max_slot_time=60.0)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            del background_tasks[:]

            # When there is work to do and some workers are available
            while (len(WORKQUEUE) > 0) and (len(background_tasks) < MAX_WORKERS):
                # Get a token/integer starts with 0
                token = tg.get_token()
                if not (token >= 0):
                    # We did not get a valid token, wait
                    break
                f = WORKQUEUE.popleft()
                background_tasks.append(BaseBackgroundTask({'id': token, 'f': f, 't': 5, 'log': log}))

            # Create futures for all background_tasks
            futures = [executor.submit(bt.execute) for bt in background_tasks]
            if futures:
                # Wait until all background tasks are completed
                for fut in as_completed(futures):
                    res = fut.result()

            # Wait
            time.sleep(1)


def flask_thread_processing(args):
    """
    I serve a Flask application
    """
    tid = args['tid'] or ''
    log = args['log'] or _
    logerr = args['logerr'] or _
    logexc = args['logexc'] or _

    try:

        log('Hello from Flask thread')

        app = Flask('flask_thread_processing')
        @app.route('/')
        def get_root():
            now_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = "%s current time: %s" % (app.name, now_time)
            return msg

        @app.route('/status')
        def get_status():
            resp = ''
            resp += "len(ACTIVE_FILES): %d" % len(ACTIVE_FILES) + '<br>'
            resp += 'ACTIVE_FILES:<br>'
            for af in ACTIVE_FILES:
                resp += "%s<br>" % str(ACTIVE_FILES[af])
            return resp

        # Flask has its own dev web server not suitable for production

        log('Calling flask app.run...')
        # This does not work (throw an exception under pywin32)
        # See https://github.com/pallets/flask/issues/3447
        #app.run(debug=True, host="127.0.0.1", port=8080, use_reloader=False)
        # That's why we use serve() from another module instead:
        serve(app, host='127.0.0.1', port=8080)


    except Exception as e:
        #logerr("[%s] Something fishy just happened... %s" % (tid, str(e)))
        logexc(*sys.exc_info())

    finally:
        log('Exiting...')


def run_tests():
    def test_dirs():
        for d in DIRSTOCHECK:
            assert os.path.isdir(d), '%s does not exists' % d
        print('dirs OK')
        return True

    def test_tokengen():
        c_max_token = 5
        c_max_slot_t = 0.1
        i = 0
        res = -1
        last_res = -1
        tg = TokenGenerator(max_token=c_max_token, max_slot_time=c_max_slot_t)
        while 1:
            last_res = res
            #print(str(tg))
            res = tg.get_token()
            #print(res)
            time.sleep(0.001)
            if (i < (c_max_token - 1) or (i > c_max_token and i <= (2*c_max_token))):
                assert (res > last_res), '%d %d' % (res, last_res)
            elif (i == c_max_token):
                assert (res == -1), '%d' % res
                time.sleep(c_max_slot_t)
            elif (res > 2*c_max_token):
                break
            i += 1
        print('tokengen OK')
        return True

    def test_db():
        conn = None
        try:
            conn = sqlite3.connect(DBFILEPATH)
        except sqlite3.Error as e:
            print(e)
            return False
        finally:
            if conn:
                conn.close()
        print('sqlite3 OK: %s (%s)' % (sqlite3.version, DBFILEPATH))
        return True

    def test_fileprops():
        file1 = FileProperties(DBFILEPATH)
        file1.hash_file()
        print(str(file1), 'OK')
        return True

    def test_dataclasses():
        @dataclass
        class Item:
            id: int
            filepath: str
            checksum: str
            create_t: int
            last_update_t: int
            status: int    
        item1 = Item(-1, 'gdfgd', '54dfd', 0, 0, -1)
        asdict(item1)
        print('dataclasses OK')

    test_tokengen()
    test_dirs()
    test_db()
    test_fileprops()
    test_dataclasses()

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
                run_tests()
                sys.exit(0)
        _cls.parse_command_line()
