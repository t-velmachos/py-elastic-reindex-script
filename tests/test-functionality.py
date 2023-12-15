

import sys
import os
sys.path.insert(0,"..")
from src.reindex.reindex import *
from pathlib import Path

# ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def verify(app):
    verify_resp = app.verify_index_existence("tvs_test_index_bak2")
    if verify_resp['status_code'] != 200:
        app.errors_tasks_list.add(verify_resp['index'])
        print("error-list:",app.errors_tasks_list )
    print("verify:", verify_resp)

def create_index(app):
    # Create Indexes
    create_index = app.create_indices("questo_results_backup_2bak")
    if create_index['status_code'] == 200:
        print('Ok: status code', create_index['status_code'])
    else:
        print('Error status code:',create_index['status_code'])

def delete_index(app):
    # Delete Indexes
    app.delete_index("tvs_test_index2",False)

def reindex(app,es_path,payload):
    app.perform_post_requests("tvs_test_index1",es_path,payload)

def update_settings(app):
    app.block_unblock_write_index("tvs_test_index1", "false") 

if __name__ == "__main__":
    app = Reindex()
    app.app_settings['debug']= True

    app.read_index_conf()
    print(app.indices)
    
    # Test Reindex
    es_path = "/_reindex?wait_for_completion=true"
    payload = '{"source":{"index":"' + "tvs_test_index_bak" + '"},"dest":{"index":"' + "tvs_test_index1" + '"}}'
    # Set  Write to Index
    