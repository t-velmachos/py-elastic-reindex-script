import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import http
import os
import urllib3
import json
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent

class Reindex:
    def __init__(self):
        # Set debug = True if you want to enable debug mode
        # Declare Variables in Constructor
         
        self.root_path = get_project_root()
        self.selected_indices = os.environ['SELECTED_INDICES']
        self.selected_environment = os.environ['SELECTED_ENVIRONMENT'] 
        self.es_selected_endpoint = os.environ['SELECTED_ES_ENDPOINT']
        self.es_auth_username = os.environ['ES_AUTH_USERNAME']
        self.es_auth_password = os.environ['ES_AUTH_PASSWORD']
        self.app_settings = { 
            "environment": {
                "dev": { 
                    "url": self.es_selected_endpoint,
                    "username": self.es_auth_username,
                    "password": self.es_auth_password
                },
            },
            "debug": False,
            "timeout": 60
        }
        
        self.skipping_indices = set()
        self.completed_tasks_list = set()
        self.errors_tasks_list = set()
    
    def read_index_conf(self):
        file_path = self.root_path.joinpath("index_config.json")
        if os.path.isfile(file_path):
            conf_file = open(file_path, "r")
            conf_file = conf_file.read()
        else: 
            print("file is not found!")
        
        self.indices = json.loads(conf_file)

    def prep_reindex(self):
        selected_indices = []
        # Read Index Conf File
        print("=================\n","Reading index.conf.json \n")
        
        if self.selected_indices == "all":
            selected_indices = self.indices.keys()
        else:
            
            if len(self.selected_indices) == 0: 
                print("Please specify the list of indices you want to reindex,i.e \"questo_,\" in the ENV  var SELECTED_INDICES ")
                exit(1)

            if isinstance(self.selected_indices,str):
                if len(self.selected_indices[0].strip())>0:
                    selected_indices = [self.selected_indices]
                if len(self.selected_indices[0].strip())>0 and "," in self.selected_indices.strip():
                    selected_indices = self.selected_indices.strip().split(',')
            
        try: 
            es_path = ""
            
            if len(self.selected_environment) == 0:
                print("Please specify a environment: ENV var SELECTED_ENVIRONMENT is undefined")
                exit(1)
            elif self.selected_environment not in self.app_settings["environment"]:
                print("Please specify a valid Environment: {env} is not found in enviroments".format(env=self.selected_environment) )
                exit(1)

            for idx in selected_indices:
                idx = idx.strip()

                if self.app_settings['debug']:
                    print("Running Selected Index:",idx)
                
                if idx not in self.indices.keys():
                    self.skipping_indices.add(idx)
                    continue
                elif len(idx.strip()) == 0:
                    continue
                else:
                    # Declare Variables
                    source_idx = self.indices[idx]['idx_name']
                    destination_idx = self.indices[idx]['dst_name']
                    stop_on_delete_err=None

                    # Ensure Source Index exists
                    print("=================\n","Selected Index Conf \n",self.indices[idx])
                    print("=================\n","Test if the index Exists:",source_idx)
                    test_result = self.verify_index_existence(source_idx)

                    if test_result['status_code'] == 200:
                        print('Ok status code:', test_result['status_code'])
                    else:
                        print('Error status code:',test_result['status_code'])
                        self.errors_tasks_list.add(test_result['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Ensure that the Backup of Destination Index is deleted
                    print("=================\n","Try deleting Index Backup:",destination_idx)
                    stop_on_delete_err=False
                    delete_backup=self.delete_index(destination_idx,stop_on_delete_err)

                    if delete_backup['status_code'] == 200:
                        print('Ok: status code', delete_backup['status_code'])
                    else:
                        print('Error status code:',delete_backup['status_code'])
                        self.errors_tasks_list.add(delete_backup['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Set index.blocks.write = True
                    print("=================\n","Try Set index.blocks.write = True:",source_idx)
                    update_idx_settings = self.block_unblock_write_index(source_idx, "true") 

                    if update_idx_settings['status_code'] == 200:
                        print('Ok: status code', update_idx_settings['status_code'])
                    else:
                        print('Error status code:',update_idx_settings['status_code'])
                        self.errors_tasks_list.add(update_idx_settings['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Clone Original Index
                    print("=================\n","Try Clone source:",source_idx,"destination:",destination_idx )
                    
                    es_path = "/" + source_idx + "/_clone/" + destination_idx
                    clone_index_resp = self.perform_post_requests(idx,es_path,None)

                    if clone_index_resp['status_code'] == 200:
                        print('Ok: status code', clone_index_resp['status_code'])
                    else:
                        print('Error status code:',clone_index_resp['status_code'])
                        self.errors_tasks_list.add(clone_index_resp['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Delete Original Index
                    print("=================\n","Try deleting Original Index Backup:",source_idx)
                    stop_on_delete_err=True
                    delete_backup=self.delete_index(source_idx,stop_on_delete_err)

                    if delete_backup['status_code'] == 200:
                        print('Ok: status code', delete_backup['status_code'])
                    else:
                        print('Error status code:',delete_backup['status_code'])
                        self.errors_tasks_list.add(delete_backup['index'])
                        print("error-list:",self.errors_tasks_list )
                    
                    # Create Original Index again with the Correct Mapping
                    print("=================\n","Try Creating Original Index Backup:",source_idx)
                    create_index=self.create_indices(source_idx)

                    if create_index['status_code'] == 200:
                        print('Ok: status code', create_index['status_code'])
                    else:
                        print('Error status code:',create_index['status_code'])
                        self.errors_tasks_list.add(create_index['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Try Run Reindex
                    print("=================\n","Try Reindex source:", destination_idx,"destination:",source_idx)
                    
                    es_path = "/_reindex?wait_for_completion=true"
                    payload = '{"source":{"index":"' + destination_idx + '"},"dest":{"index":"' + source_idx + '"}}'
                    reindex_resp = self.perform_post_requests(idx,es_path,payload)

                    if reindex_resp['status_code'] == 200:
                        print('Ok: status code', reindex_resp['status_code'])
                    else:
                        print('Error status code:',reindex_resp['status_code'])
                        self.errors_tasks_list.add(reindex_resp['index'])
                        print("error-list:",self.errors_tasks_list )

                    # Set index.blocks.write = False
                    print("=================\n","Try Set index.blocks.write = False:",source_idx)
                    update_idx_settings = self.block_unblock_write_index(source_idx, "false") 

                    if update_idx_settings['status_code'] == 200:
                        print('Ok: status code', update_idx_settings['status_code'])
                    else:
                        print('Error status code:',update_idx_settings['status_code'])
                        self.errors_tasks_list.add(update_idx_settings['index'])
                        print("error-list:",self.errors_tasks_list )
                    
                    # Add to the completed List
                    self.completed_tasks_list.add(idx)
                    

        except RuntimeError as err:
            print(err);
    
    def verify_index_existence(self,idx):
        
        try:
            resp = None
            url = self.app_settings['environment'][self.selected_environment]['url'] + "/_cat/indices/" + idx + "?v&pretty"
            headers = {'Content-Type': "application/json", 'Accept': "application/json"}
            resp = requests.get(url, timeout=self.app_settings['timeout'], headers=headers, verify=False, auth=HTTPBasicAuth(self.app_settings['environment'][self.selected_environment]['username'],self.app_settings['environment'][self.selected_environment]['password'])) #GET test connection
        except RuntimeError as err:
            print("An error occured while sending the. Please check the output.".format(e=err))
            exit(1)
        finally:

            if resp == None:
                resp_status = None
                resp_body = None
            else:
                resp_status=resp.status_code
                resp_body = resp.json()
                
            resp_dict = {
                "index": idx,
                "status_code": resp_status,
                "response": resp_body
            }

            if self.app_settings['debug']:
                print("=======URL======")
                print(url)
                print("=======HEADERS======")
                print(headers)
                print("====response====")
                print("Status Code:", resp_dict['status_code'])
                print("Response:", resp_dict['response'])

        return resp_dict

    def delete_index(self,idx,stop_on_delete_err):
        
        try:
            resp = None
            url = self.app_settings['environment'][self.selected_environment]['url'] + "/" + idx
            headers = {'Content-Type': "application/json", 'Accept': "application/json"}
            resp = requests.delete(url, timeout=self.app_settings['timeout'], headers=headers, verify=False, auth=HTTPBasicAuth(self.app_settings['environment'][self.selected_environment]['username'],self.app_settings['environment'][self.selected_environment]['password'])) #GET test connection
        except RuntimeError as err:
            pass
            print("An error occured while sending the DELETE Index {index} Request. Please check the output.".format(index=idx))
            
            if stop_on_delete_err:
                print("An error occured while sending the DELETE Index {index} Request. Please check the output \n{e}.".format(e=err,index=idx))
                exit(1)
        finally:

            if resp == None:
                resp_status = None
                resp_raise_status = None
                resp_body = None
            else:
                resp_status=resp.status_code
                resp_raise_status = None
                resp_body = resp.json()
                if stop_on_delete_err:
                    resp.raise_for_status()
            resp_dict = {
                "index": idx,
                "status_code": resp_status,
                "raise_for_status": resp_raise_status,
                "response": resp_body
            }

            if self.app_settings['debug']:
                print("=======URL======")
                print(url)
                print("=======HEADERS======")
                print(headers)
                print("====response====")
                print("Status Code:", resp_dict['status_code'])
                print("Response:", resp_dict['response'])

        return resp_dict
    
    def block_unblock_write_index(self,idx,block_write="true"):
        
        try:
            resp = None
            url = self.app_settings['environment'][self.selected_environment]['url'] + "/" + idx + "/_settings"
            headers = {'Content-Type': "application/json", 'Accept': "application/json"}
            payload = '{"settings": {"index.blocks.write": ' + block_write + '} }' 
            resp = requests.put(url, timeout=self.app_settings['timeout'], headers=headers, data=payload, verify=False, auth=HTTPBasicAuth(self.app_settings['environment'][self.selected_environment]['username'],self.app_settings['environment'][self.selected_environment]['password']))
        
        except RuntimeError as err:
            print("An error occured while sending the PUT block unblock settings to Index {index} Request. Please check the output \n{e}.".format(e=err,index=idx))

            exit(1)
        finally:

            if resp == None:
                resp_status = None
                resp_raise_status = None
                resp_body = None
            else:
                resp_status=resp.status_code
                resp_raise_status = resp.raise_for_status()
                resp_body = resp.json()
                
            resp_dict = {
                "index": idx,
                "status_code": resp_status,
                "raise_for_status": resp_raise_status,
                "response": resp_body
            }

            if self.app_settings['debug']:
                print("=======URL======")
                print(url)
                print("=======HEADERS======")
                print(headers)
                print("====response====")
                print("Status Code:", resp_dict['status_code'])
                print("Response:", resp_dict['response'])

        return resp_dict
    
    def perform_post_requests(self,idx,es_path,payload=None):
        
        try:
            resp = None
            url = self.app_settings['environment'][self.selected_environment]['url'] + es_path
            headers = {'Content-Type': "application/json", 'Accept': "application/json"}
            resp = requests.post(url, timeout=self.app_settings['timeout'], headers=headers, data=payload, verify=False, auth=HTTPBasicAuth(self.app_settings['environment'][self.selected_environment]['username'],self.app_settings['environment'][self.selected_environment]['password'])) 
        
        except RuntimeError as err:
            print("An error occured while sending the POST reindex Index Request. Please check the output \n{e}.".format(e=err))
            exit(1)
        finally:

            if resp == None:
                resp_status = None
                resp_raise_status = None
                resp_body = None
            else:
                resp_status=resp.status_code
                resp_raise_status = resp.raise_for_status()
                resp_body = resp.json()
                
            resp_dict = {
                "index": idx,
                "status_code": resp_status,
                "raise_for_status": resp_raise_status,
                "response": resp_body
            }

            if self.app_settings['debug']:
                print("=======URL======")
                print(url)
                print("=======HEADERS======")
                print(headers)
                print("====response====")
                print("Status Code:", resp_dict['status_code'])
                print("Response:", resp_dict['response'])

        return resp_dict
    
    def create_indices(self,idx):

        try:
            file_path = self.root_path.joinpath(self.indices[idx]['indexfile'])
            index_file = None
            es_path = "/" + idx + "?pretty"
            payload=None
            resp = None
            
            if os.path.isfile(file_path):
                index_file = open(file_path, "r")
                payload = index_file.read()
            else: 
                print("file is not found!")
            
            if self.app_settings['debug']:
                print("payload",payload)

            url = self.app_settings['environment'][self.selected_environment]['url'] + es_path
            headers = {'Content-Type': "application/json", 'Accept': "application/json"}
            resp = requests.put(url, timeout=self.app_settings['timeout'], headers=headers, data=payload, verify=False, auth=HTTPBasicAuth(self.app_settings['environment'][self.selected_environment]['username'],self.app_settings['environment'][self.selected_environment]['password']))
        
        except RuntimeError as err:
            print("An error occured while sending the POST reindex Index Request. Please check the output \n{e}.".format(e=err))
            exit(1)
        finally:

            if resp == None:
                resp_status = None
                resp_raise_status = None
                resp_body = None
            else:
                resp_status=resp.status_code
                resp_raise_status = resp.raise_for_status()
                resp_body = resp.json()
                
            resp_dict = {
                "index": idx,
                "status_code": resp_status,
                "raise_for_status": resp_raise_status,
                "response": resp_body
            }

            if self.app_settings['debug']:
                print("=======URL======")
                print(url)
                print("=======HEADERS======")
                print(headers)
                print("====response====")
                print("Status Code:", resp_dict['status_code'])
                print("Response:", resp_dict['response'])

        return resp_dict

    def return_summary(self):
        print("=======================")
        if len(self.completed_tasks_list) > 0:
            print("=======---Completed---======")
            print("The following reindex tasks completed without error!")

            for task in self.completed_tasks_list:
                if len(task.strip()) >0:
                    print("index:", task)
                elif len(task.strip()) == 0:
                    continue
                else:
                    print("None of the indices")

        if len(self.errors_tasks_list) > 0:
            print("=======---Failed---======")
            print("The following reindex tasks completed with error!")

            for task in self.errors_tasks_list:
                if len(task.strip()) >0:
                    print("index:", task)
                elif len(task.strip()) == 0:
                    continue
                else:
                    print("None of the indices")

        if len(self.skipping_indices) > 0:
            print("=======---Skipped---======")
            print("The following reindex tasks skipped!")
            for task in self.skipping_indices:
                if len(task.strip()) >0:
                    print("index:", task)
                elif len(task.strip()) == 0:
                    continue
                else:
                    print("None of the indices")
        
        print("date and time =", datetime.now().strftime("%m/%d/%Y %H:%M:%S"))

            


def main():
    app = Reindex()
    
    if app.app_settings['debug']:
        http.client.HTTPConnection.debuglevel = 1
    app.read_index_conf()
    app.prep_reindex()
    app.return_summary()

if __name__ == "__main__":
    main()