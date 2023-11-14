# curl commands here have ids that will need to be changed for your own uses.
# also need to change the path to some local parquet files.

curl --request POST \
  --url http://localhost:9933/register-dataset \
  --header 'content-type: application/json' \
  --data '{"source_type": "parquet","sources": ["/home/jeffrey/Data/sf-fire.parquet/*.parquet"],"connection_attr": {}}'

curl --request GET \
  --url http://localhost:9933/task-status/f9e27a68289747dfbf130e776a91fe40

curl --request POST \
  --url http://localhost:9933/execute-query \
  --header 'content-type: application/json' \
  --data '{"dataset_id": "f3fc887917df4edd94cb088db5f30723","cache_result": true,"query": "SELECT * FROM \"f3fc887917df4edd94cb088db5f30723\" USING SAMPLE reservoir(10000 ROWS) REPEATABLE (100)"}'

curl --request GET \
  --url 'http://localhost:9933/query-results?view_id=6af0dfbf9f97426bb9c9486f753f33bb' 