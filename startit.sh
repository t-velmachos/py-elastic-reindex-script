#!/bin/bash
export PYTHONPATH="<specify>"
export SELECTED_INDICES="<specify>"
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
