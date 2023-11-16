To run do the following:

```
$ poetry install
$ poetry run uvicorn main:app --reload --port 9933
```

I am using VSCode and the REST Client Extension to test this. If you don"t use VSCode, there is a test.sh file with some cURL commands that you could adapt to this.

To access a file on S3, you can either set the following environment variables:

```
export AWS_ACCESS_KEY_ID="<value>"
export AWS_SECRET_ACCESS_KEY="<value>"
export AWS_SESSION_TOKEN="<value>"
```

or you can provide the following as the "connection_attr" property in the register request:

```
{
    "s3-region": "<value>",
    "s3-access-key": "<value>",
    "s3-secret": "<value>",
    "s3-session": "<value>"
}
```

None of this code is intended to be something that is run in any production type environment. It can serve as an example of what we might do.
