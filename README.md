# py-elastic-reindex-script
DISCLAIMER

## ** This script is under development, I cannot provide any guarantees ** 

Automate the Reindexing Process with Python

How to run the following script:
1. Start by adding a new indice file inside the indices directory
2. Update the index_config.json file 
    To explain the properties 
     "<index name>":{
        "idx_name": "<changeme>", # add the name of the index you want to reindex
        "dst_name": "<changeme>", # add the name of the tmp index, it is used only for storing the data until you recreate the indice <idx_name>
        "indexfile": "indices/<changeme>" # specify the path of the indice file, remember change only the part 
    }
3. Update the start.sh # add the necessary information
4. finally run the script 
