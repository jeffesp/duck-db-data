from fastapi import FastAPI, HTTPException

app = FastAPI()

# Mock databases to store registered datasets and views
datasets = {}
views = {}

# Operation 1: POST RegisterDataset, returns a dataset_id
@app.post("/datasets/", response_model=dict)
def register_dataset(data: dict):
    dataset_id = str(len(datasets) + 1)
    datasets[dataset_id] = data
    return {"dataset_id": dataset_id, "message": "Dataset registered successfully"}

# Operation 2: POST ExecuteQuery, takes a dataset_id and a query, returns a view_id
@app.post("/execute-query/", response_model=dict)
def execute_query(dataset_id: str, query: str):
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Assume some processing for query execution
    view_id = f"view_{len(views) + 1}"
    views[view_id] = {"dataset_id": dataset_id, "query": query, "results": ["result1", "result2"]}
    return {"view_id": view_id, "message": "Query executed successfully"}

# Operation 3: POST CreateTemporaryView, takes a dataset_id and a query, returns a view_id
@app.post("/create-temporary-view/", response_model=dict)
def create_temporary_view(dataset_id: str, query: str):
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Assume some processing for creating a temporary view
    view_id = f"view_{len(views) + 1}"
    views[view_id] = {"dataset_id": dataset_id, "query": query, "results": ["temp_result1", "temp_result2"]}
    return {"view_id": view_id, "message": "Temporary view created successfully"}

# Operation 4: GET QueryResults, takes a view_id or dataset_id, returns a 2D array of data
@app.get("/query-results/", response_model=list)
def query_results(view_id: str|None = None, dataset_id: str|None = None):
    if view_id:
        if view_id not in views:
            raise HTTPException(status_code=404, detail="View not found")
        return views[view_id]["results"]
    
    elif dataset_id:
        if dataset_id not in datasets:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return datasets[dataset_id]

    raise HTTPException(status_code=400, detail="Either view_id or dataset_id must be provided")

