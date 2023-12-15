#!/bin/bash
export PYTHONPATH="<specify>" # set the current absolutepath
export SELECTED_INDICES="<specify>" # you can select one of the following ways to specify index  ("all","index_name","index_name1,index_name2")
# export SELECTED_INDICES="all"
export SELECTED_ENVIRONMENT="<specify>"
export ES_AUTH_USERNAME="<specify>"
export ES_AUTH_PASSWORD="<specify>"

ACTION="$1"

if [ "$ACTION" == "reindex" ];then
    python3 ./src/reindex/reindex.py
elif [ "$ACTION" == "tests" ];then
    python3 ./tests/test-functionality.py
else
    echo "Please specify on of the two options: reindex/tests"
fi
