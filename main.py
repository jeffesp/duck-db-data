from contextlib import contextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import uuid
import time
import duckdb

@contextmanager
def _get_db(application_id='test-app'):
    db = duckdb.connect()
    try:
        db.sql(f"ATTACH '{application_id}'")
        db.sql("PRAGMA memory_limit='16GB'")
        db.sql("PRAGMA threads=4")
        yield db
    finally:
        db.close()

app = FastAPI()

datasets = {}
views = {}
tasks = {}  # Task status tracking

class Dataset(BaseModel):
    data: List[List[str]]

class Query(BaseModel):
    dataset_id: str
    query: str

class ViewQuery(BaseModel):
    dataset: List[List[str]]
    query: str


def process_dataset(dataset_id: str, dataset: List[List[str]]):
    try:
        tasks[dataset_id] = "processing"
        time.sleep(10)  # Simulating a long-running task
        datasets[dataset_id] = dataset
        tasks[dataset_id] = "completed"
    except Exception as e:
        tasks[dataset_id] = f"failed: {str(e)}"

def process_query(view_id: str, dataset_id: str, query: str):
    try:
        tasks[view_id] = "processing"
        time.sleep(5)  # Simulating a long-running task
        views[view_id] = datasets[dataset_id]  # Mock view creation
        tasks[view_id] = "completed"
    except Exception as e:
        tasks[view_id] = f"failed: {str(e)}"

@app.post("/register-dataset")
def register_dataset(dataset: Dataset, background_tasks: BackgroundTasks):
    dataset_id = str(uuid.uuid4().hex)
    background_tasks.add_task(process_dataset, dataset_id, dataset.data)
    tasks[dataset_id] = "queued"
    return {"dataset_id": dataset_id, "message": "Dataset registration in progress"}

@app.post("/execute-query")
def execute_query(query: Query, background_tasks: BackgroundTasks):
    if query.dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    view_id = str(uuid.uuid4().hex)
    background_tasks.add_task(process_query, view_id, query.dataset_id, query.query)
    tasks[view_id] = "queued"
    return {"view_id": view_id, "message": "Query execution in progress"}

@app.post("/create-temporary-view")
def create_temporary_view(query: ViewQuery):
    view_id = str(len(views) + 1)
    views[view_id] = query.dataset  # Creating a view from the given dataset
    return {"view_id": view_id}

@app.get("/query-results")
def query_results(view_id: Optional[str] = None, dataset_id: Optional[str] = None):
    if view_id:
        if view_id in views:
            return views[view_id]
        else:
            raise HTTPException(status_code=404, detail="View not found")
    elif dataset_id:
        if dataset_id in datasets:
            return datasets[dataset_id]
        else:
            raise HTTPException(status_code=404, detail="Dataset not found")
    else:
        raise HTTPException(status_code=400, detail="View ID or Dataset ID required")

@app.get("/task-status/{task_id}")
def task_status(task_id: str):
    status = tasks.get(task_id, "unknown")
    return {"task_id": task_id, "status": status}


