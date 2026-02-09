# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2c2008c-f4f3-4c70-952e-04defee28461",
# META       "default_lakehouse_name": "DEV_IDM",
# META       "default_lakehouse_workspace_id": "6006c11e-4d20-44d5-8de5-e3968c2a42f5",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2c2008c-f4f3-4c70-952e-04defee28461"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "508f2e8c-ead9-b0e8-440c-6d294fd7aeb4",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

# passed parameters from the master notebook master_idm_data_transform
udm_table_name = None
idm_table_name = None
ancestor_name = None
saved_table_name = None
pdi_shortcut = None
pdi_config_json_path = None
pdi_config_python_path = None
partitionBy = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Logger and Help Functions

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
import json

if not pdi_shortcut or not pdi_config_json_path or not pdi_config_python_path:
    mssparkutils.notebook.exit("Missing required parameters from the master notebook")

# TODO: after being able to  auotload the python files into notebook's resource folder, the python class would be directly available to the notebook - then no need to add SC
files = mssparkutils.fs.ls(pdi_config_python_path)
for file in files:
    sc.addPyFile(file.path)

from config import Config
config = Config(sc, pdi_shortcut, pdi_config_json_path, enable_etl_status=False)

if partitionBy:
    partitionBy = json.loads(partitionBy)
    
from logger import Logger
context = notebookutils.runtime.context
filter_context = {
    'Mode': 'Ancestor Hierarchy',
    "IDM_Table": idm_table_name,
    'Notebook': context['currentNotebookName'],
    'Workspace': context['currentWorkspaceName']
}

logger = Logger(config, filter_context=filter_context)

ancestor_max_level = config.ancestor_max_ancestor_level
logger.info(f"[UDM table name: {udm_table_name}; IDM ancestor table name: {saved_table_name}; Max ancestor level: {ancestor_max_level}]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Process Ancestor Levels

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from notebookutils import mssparkutils
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from IPython.utils.capture import capture_output
import logging

# save the data to delta table
def _save(df, saved_table_name, logger, mode='append'):
    try:
        if partitionBy and mode == 'overwrite':
            df.write.format("delta").partitionBy(*partitionBy).mode(mode).option("overwriteSchema", "true").saveAsTable(saved_table_name)
        else:
            df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(saved_table_name)
    except Exception as error:
        logger.error(f"[Save Error: {error}]")

# process ancestor hierarchy data 
def process_ancestors(udm_table_name, idm_table_name, ancestor_name, ancestor_max_level, logger):
    try:
        # fix the deadlock ancestor assignment, otherwise, the hierarchy level calculation will be endless
        spark.sql(f"update {idm_table_name} set {ancestor_name}=null where id={ancestor_name} and row_is_current=1")
 
        # the most top hierarchy as initial recursive entry where there are no ancestors
        sql = f"""
            select tenant_id, id as {udm_table_name}_id, {ancestor_name}, 0 as level
            from {idm_table_name} 
            where {ancestor_name} is null and row_is_current=1
        """
        df = spark.sql(sql)
        df.createOrReplaceTempView('recursion_df')
        level = 1
        while True:
            sql = f"""
                    select t.tenant_id, t.id as {udm_table_name}_id, t.{ancestor_name}, {level} as level
                    from recursion_df rf
                    inner join {idm_table_name} t on rf.tenant_id = t.tenant_id and rf.{udm_table_name}_id = t.{ancestor_name}
                    where t.row_is_current=1
            """
            recursion_df = spark.sql(sql)
            recursion_df.createOrReplaceTempView('recursion_df')
            df = df.union(recursion_df)
            if recursion_df.isEmpty() or level==ancestor_max_level:
                return df
            else:
                level += 1
        return df
    except Exception as error:
        logger.exception(f"[Recursive Errror: {error}]")
    return None

def run():
    if udm_table_name and idm_table_name and ancestor_name and saved_table_name:
        df = process_ancestors(udm_table_name, idm_table_name, ancestor_name, ancestor_max_level, logger)
        if df:
            _save(df, saved_table_name, logger, 'overwrite')

    mssparkutils.notebook.exit(f"Ancestor-{idm_table_name}-{ancestor_name}")

if config.environment_name.upper() == 'PROD':
    # no outpouts
    with capture_output():
        run()
else:
    run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential
from azure.graphrbac import GraphRbacManagementClient

# Admin SPN credentials
client_id = "<admin-spn-client-id>"
client_secret = "<admin-spn-client-secret>"
tenant_id = "<tenant-id>"

credential = ClientSecretCredential(
    client_id=client_id,
    client_secret=client_secret,
    tenant_id=tenant_id
)

# Create GraphRbacManagementClient
graph_client = GraphRbacManagementClient(credential, tenant_id)


# Create application
app = graph_client.applications.create({
    "display_name": spn_name,
    "identifier_uris": [f"https://{spn_name}"]
})

# Create service principal for the app
sp = graph_client.service_principals.create({"app_id": app.app_id})

# Create client secret
password_cred = graph_client.applications.create_password(app.object_id, {
    "start_date": "2025-01-01T00:00:00Z",
    "end_date": "2030-01-01T00:00:00Z",
    "value": str(uuid.uuid4())
})

print("SPN AppID:", app.app_id)
print("Secret:", password_cred.value)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
