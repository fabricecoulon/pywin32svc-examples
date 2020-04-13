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

