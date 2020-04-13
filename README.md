# Pywin32 examples for Python3

This is a project for a Windows service written in Python with the Pywin32 extension.
I also want to be able to use a pyinstaller or pyoxidizer tools to package and distribue this later on.

Tested with python 3.8 (amd64) and pywin32-227.win-amd64-py3.8

Pywin32 services have to be started with these commands under windows:

```
python script.py install
python script.py start
```

To stop and remove the service:

```
python script.py stop
python script.py remove
```

Additionally, once installed, one can use `sc`:

```
sc query <servicename>
sc start <servicename>
sc stop <servicename>
```

## Pywin32 installation notes

  1. Install Python3+ for all users, change system path, system installation.
     e.g 64-bit version: python-3.8.2-amd64.exe

  2. Install PyWin32 (python extensions for windows) also 64-bit version that
     matches the Python3 version you've installed at step #1
     https://github.com/mhammond/pywin32/releases
     e.g pywin32-227.win-amd64-py3.8.exe

  3. Be sure to have the file `%python_install_dir%\Lib\site-packages\win32\pywintypes38.dll`
     (please note that '38' is the version of your Python installation)
     If you don't have this file, take it from `%python_install_dir%\Lib\site-packages\pywin32_system32\pywintypes38.dll`
     and copy it into `%python_install_dir%\Lib\site-packages\win32\`.
     `%python_install_dir%` is the installation directory chosen during step #1 before.

  4. Use 'debug' to start service in debug (console):
     ```
     python script.py debug
     ```
  5. Start a `powershell` and use `pip` to install the dependencies:
     `pip install xyz` where xyz are stated in the imports of `script.py`

