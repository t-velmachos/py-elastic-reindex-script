#!/bin/bash
export PYTHONPATH="<changeme>" # set the current absolutepath
export SELECTED_INDICES="<changeme>" # you can select one of the following ways to specify index  ("all","index_name","index_name1,index_name2")
export SELECTED_ENVIRONMENT="<changeme>" # dev
export SELECTED_ES_ENDPOINT="<changeme>" # "https://localhost:9300"
export ES_AUTH_USERNAME="<changeme>"
export ES_AUTH_PASSWORD="<changeme>"

ACTION="$1"

if [ "$ACTION" == "reindex" ];then
    python3 ./src/reindex/reindex.py
elif [ "$ACTION" == "tests" ];then
    python3 ./tests/test-functionality.py
else
    echo "Please specify on of the two options: reindex/tests"
fi
