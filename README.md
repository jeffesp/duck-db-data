To run do the following:

```
$ poetry install
$ poetry run uvicorn main:app --reload --port 9933
```

You should see something like this in the terminal:

```
INFO:     Will watch for changes in these directories: ['/home/jeffrey/Code/duck-db-data']
INFO:     Uvicorn running on http://127.0.0.1:9933 (Press CTRL+C to quit)
INFO:     Started reloader process [2533480] using StatReload
INFO:     Started server process [2533534]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

I am using VSCode and the REST Client Extension to test this. If you don't use VSCode, there is a test.sh file with some cURL commands that you could adapt to this.

None of this code is intended to be something that is run in any production type environment. It can serve as an example of what we might do.
