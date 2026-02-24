# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4ec4872-6401-4d4f-a094-636c978ff17a",
# META       "default_lakehouse_name": "IDM",
# META       "default_lakehouse_workspace_id": "8004a349-5e7a-49f6-8dcf-02e3a9d6659d",
# META       "known_lakehouses": [
# META         {
# META           "id": "f9586463-fa0e-4156-af62-bc199933a96d"
# META         },
# META         {
# META           "id": "a4ec4872-6401-4d4f-a094-636c978ff17a"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

PDI_SHORTCUT = 'pdi'
UDM_LAKEHOUSE = None
KeyVaultName = None
Fabric_Capacity_ID = None
Metadata_Lakehouse = None
PipelineRunID = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Logger and Helper Methods

# CELL ********************

from pyspark.sql.functions import *
import json

# PDI config
PDI_CONFIG_JSON_PATH = "/configuration/pdi_app_config.json"
DDC_CONFIG_JSON_PATH = "/configuration/pdi_ddc_connfig.json"
PYTHON_CONFIG_DIR = f"Files/{PDI_SHORTCUT}/configuration/python"

for f in notebookutils.fs.ls(PYTHON_CONFIG_DIR):
    if f.path.endswith(".py"):
        sc.addPyFile(f.path)

from config import Config
config = Config(sc, PDI_SHORTCUT, PDI_CONFIG_JSON_PATH)


from logger import Logger

context = notebookutils.runtime.context
filter_context = {
    'Mode': 'DDC_deployments',
    'Notebook': spark.conf.get("spark.synapse.notebook.name", 'Unknown'),
    'Workspace': context['currentWorkspaceName']
}
if PipelineRunID:
    filter_context['PipelineRunID'] = PipelineRunID

logger = Logger(config, filter_context=filter_context)

# DDC config
from ddc_config import DDC_Config
ddc_config = DDC_Config(PDI_SHORTCUT, DDC_CONFIG_JSON_PATH, KeyVaultName, logger)

def get_ddc_tms_licenses():
    sql = f"""
    SELECT 
    FROM  
    WHERE active = true
    """
    ddc_tms_licenses = spark.sql(sql)
    return [row.asDict() for row in ddc_tms_licenses.collect()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check DDC TMS Licenses And Deploy New DDC Tenants

# CELL ********************

