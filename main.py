from contextlib import contextmanager
import os
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, List
import uuid
import time
import duckdb

app = FastAPI()

datasets = {}
views = {}
tasks = {}


class Dataset(BaseModel):
    sources: List[str]
    source_type: str = 'csv'
    connection_attr: Dict[str, str]


class Query(BaseModel):
    dataset_id: str
    cache_result: bool = False
    query: str


class ViewQuery(BaseModel):
    dataset: List[List[str]]
    query: str


@contextmanager
def get_db(application_id='duck-db'):
    db = duckdb.connect()
    try:
        db.sql(f"ATTACH '{application_id}'")
        db.sql(f"USE '{application_id}'")
        db.sql("PRAGMA memory_limit='16GB'")
        db.sql("PRAGMA threads=4")
        yield db
    finally:
        db.close()


def format_source(dataset: Dataset):
    if dataset.source_type == 'csv':
        return f"read_csv_auto({dataset.sources}, header=true)"
    elif dataset.source_type == 'parquet':
        return f"read_parquet({dataset.sources})"
    else:
        raise HTTPException(status_code=400, detail="Must provide a source_type")


def process_dataset(task_id: str, dataset_id: str, dataset: Dataset):
    try:
        tasks[task_id] = "processing"
        with get_db() as db:
            # using region as indication that all are present.
            if 's3-region' in dataset.connection_attr:
                db.sql(f"SET s3_region='{dataset.connection_attr['s3-region']}'")
                db.sql(f"SET s3_access_key_id='{dataset.connection_attr['s3-access-key']}'")
                db.sql(f"SET s3_secret_access_key='{dataset.connection_attr['s3-secret']}'")
                db.sql(f"SET s3_session_token='{dataset.connection_attr['s3-session']}'")
            elif 'AWS_ACCESS_KEY_ID' in os.environ:
                db.sql("SET s3_region='us-east-1'")  # DDB seem to require this, not sure why.
                db.sql(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
                db.sql(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
                db.sql(f"SET s3_session_token='{os.environ.get('AWS_SESSION_TOKEN', None)}'")
            db.sql(f'CREATE TABLE "{dataset_id}" AS SELECT * FROM {format_source(dataset)}')
        datasets[dataset_id] = dataset
        tasks[task_id] = "completed"
    except Exception as e:
        tasks[task_id] = f"failed: {str(e)}"


def process_query(task_id: str, view_id: str, cache_result: bool, query: str):
    try:
        tasks[task_id] = "processing"
        with get_db() as db:
            if cache_result:
                db.sql(f'CREATE TABLE "{view_id}" AS ({query})')
            else:
                db.sql(f'CREATE VIEW "{view_id}" AS ({query})')
        views[view_id] = {}
        tasks[task_id] = "completed"
    except Exception as e:
        tasks[task_id] = f"failed: {str(e)}"


@app.post("/register-dataset")
def register_dataset(dataset: Dataset, background_tasks: BackgroundTasks):
    dataset_id = str(uuid.uuid4().hex)
    task_id = str(uuid.uuid4().hex)
    background_tasks.add_task(process_dataset, task_id, dataset_id, dataset)
    tasks[dataset_id] = "queued"
    return {
        "task_id": task_id,
        "dataset_id": dataset_id,
        "message": "Dataset registration in progress",
    }


@app.post("/execute-query")
def execute_query(query: Query, background_tasks: BackgroundTasks):
    if query.dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    view_id = str(uuid.uuid4().hex)
    task_id = str(uuid.uuid4().hex)

    background_tasks.add_task(process_query, task_id, view_id, query.cache_result, query.query)
    tasks[task_id] = "queued"
    return {"task_id": task_id, "view_id": view_id, "message": "Query execution in progress"}


@app.get("/query-results")
def query_results(
    view_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    page_index: int = 0,
    page_size: int = 100,
):
    if view_id:
        id = view_id
    elif dataset_id:
        id = dataset_id
    else:
        raise HTTPException(status_code=400, detail="View ID or Dataset ID required")

    with get_db() as db:
        db.execute(f"CALL pragma_table_info('{id}')")
        columns = db.fetchall()
        db.execute(f'SELECT * FROM "{id}" OFFSET {page_index * page_size} LIMIT {page_size}')
        data = db.fetchall()
        db.execute(f'SELECT count(*) FROM "{id}"')
        count = db.fetchone()

    return {
        "data": data,
        "columns": columns,
        "totalCount": count,
        "pageIndex": page_index,
        "pageSize": page_size,
    }


@app.get("/task-status/{task_id}")
def task_status(task_id: str):
    status = tasks.get(task_id, "unknown")
    return {"task_id": task_id, "status": status}
