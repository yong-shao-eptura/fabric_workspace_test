# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d714e4b8-618d-441d-9c51-56b0608b0b2a",
# META       "default_lakehouse_name": "DEV_UDM",
# META       "default_lakehouse_workspace_id": "6006c11e-4d20-44d5-8de5-e3968c2a42f5",
# META       "known_lakehouses": [
# META         {
# META           "id": "d714e4b8-618d-441d-9c51-56b0608b0b2a"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "712b31fb-eb89-bf3d-4067-c34cfe6ea124",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

# passed values from its master notebook master_captured_udm_data_ingestion
namespace = None
module = None
eventhub_name = None
udm_table_name = None
pdi_shortcut = None
pdi_config_json_path = None
pdi_config_python_path = None
pdi_sp = None
udm_partition_path = None
is_new_release = False
edw_eventhub_offsets_table = 'edw_eventhub_offsets'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configuration and Logger

# CELL ********************

import notebookutils
from pyspark.sql.functions import (
    col, lit, row_number, to_timestamp, when, get_json_object,
    explode, expr, from_json, to_json, length, array, max
)
from pyspark.sql.types import StructType, ArrayType
from azure.identity import DefaultAzureCredential
from azure.schemaregistry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.window import Window
from functools import reduce
from collections import defaultdict
import json
import time

if not pdi_shortcut or not pdi_config_json_path or not pdi_config_python_path or not pdi_sp:
    notebookutils.notebook.exit("Missing required parameters from the master notebook")

EXCLUDED_SHORTCUTS = [pdi_shortcut]
# EnqueuedTimeUtc from eventhub captured data in "MM/dd/yyyy hh:mm:ss a"
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.parquet.int96RebaseModeInWrite=LEGACY")
spark.sql("set spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY")

# TODO: after being able to  auotload the python files into notebook's resource folder, the python class would be directly available to the notebook - then no need to add SC
files = notebookutils.fs.ls(pdi_config_python_path)
for file in files:
    sc.addPyFile(file.path)

from config import Config
config = Config(sc, pdi_shortcut, pdi_config_json_path)

config.setup_notebook_env(json.loads(pdi_sp))

from logger import Logger
context = notebookutils.runtime.context
filter_context = {
    'UDM_Table': udm_table_name,
    'Notebook': spark.conf.get('spark.synapse.context.notebookname'),
    'Workspace': context['currentWorkspaceName']
}
logger = Logger(config, filter_context=filter_context)

table_partitions = json.loads(notebookutils.fs.head(udm_partition_path))
def get_table_partitionBy(table_name):
    for table_partition in table_partitions:
        if table_partition['table'] == table_name:
            return table_partition['partition_columns']
    return None

# check if edw_eventhub_offsets table exists to avoid multiple checks
is_edw_eventhub_offsets_table_exists = spark.catalog.tableExists(edw_eventhub_offsets_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Helper Functions

# CELL ********************

cached_avro_schema = {}

def split_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

# retreat schema definition from schema ID
def get_avro_schema(raw_schema_id, credential, namespace, eventhub_name, udm_table_name, logger):
    schema = cached_avro_schema.get(raw_schema_id, None)
    if not schema:
        schema_id = raw_schema_id.split('+')[1]
        try:
            if schema_id:
                schema_registry_client = SchemaRegistryClient(fully_qualified_namespace=f'{namespace}.servicebus.windows.net/', credential=credential)
                with schema_registry_client:
                    schema = schema_registry_client.get_schema(schema_id)
                    if schema:
                        cached_avro_schema[raw_schema_id] = schema.definition
                        return schema.definition
        except Exception as e:
            logger.exception(f"[Error with Azure schema register with namespace: {namespace}; Eventhub: {eventhub_name}; Table: {udm_table_name}; Schema ID: {schema_id} -  {e}]")
            return None
    else:
        return schema

# deduplicate latest data rows based on edw_start_time
# caused by UDM publish issue from automated test???
def _deduplicate_latest(df):
    # Drop 'edw_start_time' and 'timestamp' if they exist for partitioning
    partition_columns = [c for c in df.columns if c not in ('edw_start_time', 'timestamp')]

    # Create window spec and filter to get latest rows
    window = Window.partitionBy(*partition_columns).orderBy(col("edw_start_time").desc())
    return df.withColumn("row_number", row_number().over(window)).filter(col("row_number") == 1).drop("row_number")

# build partition filter condition for delta table
# partition_cols is a list of partition column names
# batch is a list of distinct rows for partition columns
def _build_partition_filter_condition(partition_cols, batch):
    row_conditions = []
    for row in batch:
        condition = reduce(lambda acc, c: acc & (col(c) == row[c]), partition_cols[1:], col(partition_cols[0]) == row[partition_cols[0]])
        row_conditions.append(condition)
    return reduce(lambda acc, cond: acc | cond, row_conditions)

# write delta table with retry logic to handle transient errors
# write_func is a function to write delta table
# df is the dataframe to write
# tablename is the delta table name
# mode is the write mode such as append or overwrite
# schema_mode is the schema mode such as mergeSchema or overwriteSchema
# partitionBy is a list of column names
# max_retries is the maximum number of retries - default is 5
# base_delay_sec is the base delay in seconds for exponential backoff - default is 2 seconds
def _write_delta_table_with_retry(write_func, df, tablename, mode, schema_mode, partitionBy=None, max_retries=5, base_delay_sec=2):
    attempt = 0
    while True:
        try:
            write_func(df, tablename, mode, schema_mode, partitionBy)
            break
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(f"Max retries exceeded for writing to {tablename}") from e
            time.sleep(base_delay_sec * attempt)

# write dataframe to delta table with specified mode and schema mode
# partitionBy is a list of column names
# batch_size is to control how many partitions to be processed in one batch - small batch size is safer for large partition data
def _write_delta_table(df, tablename, mode, schema_mode, partitionBy=None, batch_size=10):
    def write_func(df, tablename, mode, schema_mode, partitionBy):
        if partitionBy:
            distinct_rows = df.select(*partitionBy).distinct().collect()
            if not distinct_rows:
                # No data to write
                return
            batches = [distinct_rows[i:i+batch_size] for i in range(0, len(distinct_rows), batch_size)]
            for batch in batches:
                condition = _build_partition_filter_condition(partitionBy, batch)
                df_batch = df.filter(condition)
                if len(batch) > 1:
                    df_batch = df_batch.repartition(len(batch), *partitionBy)
                df_batch.write.format("delta").mode(mode).partitionBy(*partitionBy).option(schema_mode, "true").saveAsTable(tablename)
        else:
            df.write.format("delta").mode(mode).option(schema_mode, "true").saveAsTable(tablename)
    _write_delta_table_with_retry(write_func, df, tablename, mode, schema_mode, partitionBy)

# force to cast delta table's column types to new ones and keep all exitsing data
def _force_overwrite_table(dataframe, tablename, partitionBy=None):
    try:
        # if table exists, cast column types first before overwriting schema
        if spark.catalog.tableExists(tablename):
            df = spark.table(tablename)
            table_schema = {s.name:s.dataType for s in df.schema}
            new_schema = {s.name:s.dataType for s in dataframe.schema}
            selected_list = [col(c).cast(new_schema[c]) if c in new_schema and new_schema[c] != table_schema[c]  else col(c) for c in table_schema]
            df = df.select(*selected_list)

            _write_delta_table(df, tablename, mode="overwrite", schema_mode="overwriteSchema", partitionBy=partitionBy)
        else:
            _write_delta_table(dataframe, tablename, mode="overwrite", schema_mode="overwriteSchema", partitionBy=partitionBy)
    except Exception as error:
        logger.exception(f"[Error to force overwrite {tablename}: {error}]")

# save dataFrame to delta table
def save_to_table(tablename, dataframe, mode='append'):
    # avoid duplication - UDM publish issue from automated test???
    dataframe = _deduplicate_latest(dataframe)
    partitionBy = get_table_partitionBy(tablename)
    schema_mode = 'mergeSchema'

    if not spark.catalog.tableExists(tablename):
        schema_mode = 'overwriteSchema'
        mode = 'overwrite'

    try:
        _write_delta_table(dataframe, tablename, mode=mode, schema_mode=schema_mode, partitionBy=partitionBy)
    except Exception as error:
        logger.warning(f"[Fallback triggered for {tablename}: {error}]")
        # force-cast column types with delta table before appending new data
        _force_overwrite_table(dataframe, tablename, partitionBy)
        # Append again with corrected schema
        _write_delta_table(dataframe, tablename, mode="append", schema_mode="mergeSchema", partitionBy=partitionBy)

# end of notebook
def finalize(message='Stopped'):
    notebookutils.notebook.exit(message)

# get shortcuts from lakehouse Files excluding specified file names or paths (better with shortcut API???)
# TODO: shortcut API
def get_shortcuts():
    shortcuts = []
    files = notebookutils.fs.ls("Files/")
    for file in files:
        if file.isDir and file.name not in EXCLUDED_SHORTCUTS:
            shortcuts.append({'name': file.name, 'path': file.path})
    return shortcuts

def list_folder(path):
    try:
        return notebookutils.fs.ls(path)
    except Exception:
        return []
def parse_int(name):
    return int(name.lstrip("0") or "0")
# scan captured UDM file paths based on latest max ingested time
# return a list of path in chunks - each chunk contains a list of path and all the partitions (0-N) is in the same chunk
# chunk size is to control how many days data to be processed in one batch 
def get_udm_active_paths(namespace, eventhub_name, ingested_max_time, shortcuts, chunk_size=20):
    partition_day_map = defaultdict(list)
    for shortcut in shortcuts:
        base_path = shortcut.get("path")
        if base_path.endswith(namespace):
            base_path = f"{base_path}/{eventhub_name}/"
        elif not base_path.endswith(eventhub_name):
            base_path = f"{base_path}/{namespace}/{eventhub_name}/"
        if not notebookutils.fs.exists(base_path):
            continue
        for partition in list_folder(base_path):
            for year_f in list_folder(partition.path):
                year = parse_int(year_f.name)
                if ingested_max_time and year < ingested_max_time.year:
                    continue
                for month_f in list_folder(year_f.path):
                    month = parse_int(month_f.name)
                    if ingested_max_time and year == ingested_max_time.year and month < ingested_max_time.month:
                        continue
                    for day_f in list_folder(month_f.path):
                        day = parse_int(day_f.name)
                        if ingested_max_time and year == ingested_max_time.year and month == ingested_max_time.month and day < ingested_max_time.day:
                            continue
                        key = f"{namespace}/{eventhub_name}/{year}/{month}/{day}"
                        partition_day_map[key].append(day_f.path)
    if not partition_day_map:
        logger.info(f"No active UDM data paths found for namespace: {namespace}, eventhub: {eventhub_name}, ingested max time: {ingested_max_time}")
        return []
    sorted_days = sorted(partition_day_map.keys())
    # to speed up processing with multiple chunks in parallel - each chunk contains a list of path and all the partitions (0-N) is in the same chunk
    chunks = []
    for i in range(0, len(sorted_days), chunk_size):
        chunk_days = sorted_days[i:i+chunk_size]
        chunk_paths = []
        for day in chunk_days:
            chunk_paths.extend(partition_day_map[day])
        chunks.append(chunk_paths)

    return chunks

# add UUID as PK when a UDM table has no PK since if it has sub entities, its sub entities could not look up their parent records without such PK
# nested booking data case
def _add_uuid(field_name, df):
    columns = df.columns
    if field_name not in columns and "id" not in columns:
        return df.withColumn(field_name, expr("uuid()"))
    return df

# process pusblished UDM data with nested table data such as booking.spaces.attendees, building.operating_hours...
def process_udm_nested_table(name, schema, is_arrayType, field_names, udm_table_name, df, embedded_save_tablename, full_name, level):
    udm_table_id = f"{udm_table_name}_id"
    df_columns = df.columns
    root_parent_id_column_name = udm_table_id if udm_table_id in df_columns else "id"
    root_parent_id_column = col(root_parent_id_column_name).alias(udm_table_id)
    base_common_selected = ["edw_start_time", "history_time", "timestamp"]
    if 'tenant_id' not in field_names:
        base_common_selected.append('tenant_id')
    # visir recurrence pattern splitting case
    if embedded_save_tablename in ['visit_recurrence_pattern']:
        base_common_selected.extend(['start_time', 'end_time', 'visitor_ids', 'status'])
    # booking recurrence splitting case
    elif embedded_save_tablename in ['booking_recurrence']:
        base_common_selected.extend(['start_time', 'end_time', 'recurrence_id', 'title'])
    common_selected = base_common_selected + [udm_table_id]
    temp_names = full_name.split('.')
    size = len(temp_names)
    for index, temp_name in enumerate(temp_names):
        if size > 2 and index > 0 and index + 1 != size:
            # pass corresponding parent pk column into child dataframe. e.g. grasp space_id from booking.recurrence_exceptions.booking_space.requester into requester
            temp_name = temp_name.replace(f"{udm_table_name}_", '') if f"{udm_table_name}_" in temp_name else temp_name 
             # TODO: more relible way to convert plurable to singular but it's up to how AVRO schema is defined
            temp_name = f"{temp_name[:-1] if temp_name.endswith('s') else temp_name}_id"
            if temp_name in df_columns and temp_name not in field_names and temp_name not in base_common_selected:
                base_common_selected.append(temp_name)
                common_selected.append(temp_name)

    embedded_json_df = df.select(base_common_selected + [root_parent_id_column, explode(col(name)).alias('json') if is_arrayType else to_json(col(name)).alias('json')])
    if is_arrayType:
        selected =  common_selected + [expr(f"json['{field_name}']").cast("timestamp").alias(field_name) if "_time" in field_name else expr(f"json['{field_name}']").alias(field_name) for field_name in field_names]
    else:
        selected = common_selected + [get_json_object("json", f"$.{field_name}").cast("timestamp").alias(field_name) if "_time" in field_name else get_json_object("json", f"$.{field_name}").alias(field_name) for field_name in field_names]

    embedded_json_df = embedded_json_df.select(*selected)
    if not embedded_json_df.isEmpty():
        _name = name.replace(f"{udm_table_name}_", '') if f"{udm_table_name}_" in name else name 
        if embedded_save_tablename.endswith('_host') or embedded_save_tablename.endswith('_requester') or embedded_save_tablename.endswith('_attendees'):
            columns = embedded_json_df.columns
            # filter out data rows with null or empty email column
            if 'email' in columns:
                embedded_json_df = embedded_json_df.filter("email is not null and email != ''")
        else:
            embedded_json_df = _add_uuid(f"{_name[:-1] if _name.endswith('s') else _name}_id", embedded_json_df)
        # save nested data to delta table
        _save(embedded_json_df, udm_table_name, embedded_save_tablename, full_name, level)

# fix spark bug with nested JSON content - even its schema type is Array or Struct but its value is String type??? 
def auto_parse_json_like_columns(df, level):
    # JSON deep nested content issue when level > 2 such as booking.recurrence_exceptions.booking_space.attendees
    if level <= 2:
        return df

    json_schema_map = {}
    fields = df.schema.fields
    for field in fields:
        name = field.name
        # column value likes JSON string "{...}" or "[...]"
        json_check_condition = (
            col(name).isNotNull() &
            (length(col(name)) > 2) &
            (
                (col(name).startswith("{") & col(name).endswith("}")) |
                (col(name).startswith("[") & col(name).endswith("]"))
            )
        )
        try:
            temp_df = df.select(name).filter(json_check_condition)
            if not temp_df.isEmpty():
                value = temp_df.first()[0]
                inferred_schema = schema_of_json(value)
                json_schema_map[name] = inferred_schema
        except Exception as error:
            # ok if it fails - it means the column value is not a valid JSON string or the column is not a string type (already a JSON object)
            logger.info(f"cannot convert string json to json object with column {name} - {error}")

    if json_schema_map:
        # convert any column with a string type JSON content into JSON object in dataframe
        updated_cols = [
            from_json(col(field.name), json_schema_map[field.name]).alias(field.name)
            if field.name in json_schema_map
            else col(field.name)
            for field in fields
        ]
        df = df.select(*updated_cols)

    return df

# save UDM data and any corresponding nested data to delta tables
def _save(df, udm_table_name, save_table_name, full_name, level):
    df = auto_parse_json_like_columns(df, level)
    json_schema = df.schema
    for schema in json_schema:
        is_arrayType = isinstance(schema.dataType, ArrayType)
        is_structType = isinstance(schema.dataType, StructType)
        if is_arrayType or is_structType:
            # embedded JSON data case
            name = schema.name
            field_names = []
            if is_arrayType:
                element_type = schema.dataType.elementType
                if isinstance(element_type, StructType):
                    # array of struct type
                    field_names = element_type.names
            elif is_structType:
                # struct type
                field_names = schema.dataType.fieldNames()

            if field_names:
                df_embedded = df.filter(col(name).isNotNull())
                if not df_embedded.isEmpty():
                    # nested data such building_operating_hours, booking_spaces/attendees ...
                    embedded_save_tablename = (save_table_name + '_' + name.replace(f"{udm_table_name}_", '')) if f"{udm_table_name}_" in name else (save_table_name + '_' + name )
                    process_udm_nested_table(name, schema, is_arrayType, field_names, udm_table_name, df_embedded, embedded_save_tablename, f"{full_name}.{name}", level + 1)
                # drop nested data column
                df = df.drop(name)

            # if the column is a simple array type, convert its values into JSON strings since Fabric lakehouse SQL endpoint does not support array type column
            if is_arrayType and not field_names:
                df = df.withColumn(name, to_json(when(col(name).isNull(), array()).otherwise(col(name))))
    
    # save to delta table
    save_to_table(save_table_name, df)

# error message to indicate its published datatime
def _get_published_dates(df, datetime_column):
    return sorted(set([str(time)[0:10] for time in df.select(datetime_column).distinct().rdd.flatMap(lambda x: x).collect()]))

# deletion case: add missed columns from df_live into df_source
def _add_missed_columns(udm_table_name, df_live, df_source):
    missed_columns = {}
    df_columns = df_source.columns
    schema  = df_live.schema
    for name in df_live.columns:
        if name not in df_columns:
            missed_columns[name] = lit(None).cast(schema[name].dataType)

    return df_source.withColumns(missed_columns)

# deletion case: get last rows from new data and existing delta table data, and merge those data rows together - new data comes first
# then use those data rows to update all missed column data of the deleted data rows
def _add_missed_values(udm_table_name, df_source):
    # df_source: frame with deleted data before adding missed columns yet
    pk_columns = [c for c in df_source.columns if c not in ['edw_start_time', 'history_time', 'timestamp']]

    # from delta table existing last data rows: group by id order by timestamp desc
    if spark.catalog.tableExists(udm_table_name):
        sql = f"""
            with cte as (
                Select *, ROW_NUMBER() over (PARTITION BY {', '.join(pk for pk in pk_columns)} ORDER BY timestamp desc) as rank from {udm_table_name}
            )
            select * from cte where rank=1"""
        df_table = spark.sql(sql).drop('rank')
        # add all missed columns to df_source
        df_source = _add_missed_columns(udm_table_name, df_table, df_source)
        df_table.createOrReplaceTempView(f"{udm_table_name}_source")
        df_source.createOrReplaceTempView(f"{udm_table_name}_deletion")

        sql = f"""
            select D.edw_start_time, D.history_time, D.timestamp, {', '.join(f"L.{c}" for c in df_table.columns if c not in ['edw_start_time', 'history_time', 'timestamp'])}
            from {udm_table_name}_deletion D inner join {udm_table_name}_source L ON {' AND '.join(f'D.{pk}=L.{pk}' for pk in pk_columns)}"""

        return spark.sql(sql)
    return df_source

def _get_max_edw_start_time(df, ingested_max_time):
    # get overall max time from all processed data
    chunk_max_row = df.selectExpr("max(edw_start_time) as max_time").first()
    if chunk_max_row and chunk_max_row['max_time']:
        return chunk_max_row['max_time']
    return ingested_max_time

# save ingested max time to edw_eventhub_offsets table
# this is to be used for incremental UDM data ingestion
# it is used to avoid reprocessing the same data again per namespace and eventhub
def _save_ingested_max_time(namespace, eventhub_name, overall_max_time, logger):
    if not overall_max_time or not is_edw_eventhub_offsets_table_exists:
        return
    attempt = 0
    key = f"{namespace}_{eventhub_name}"
    table_df = spark.table(edw_eventhub_offsets_table)
    new_df = spark.createDataFrame([(key, overall_max_time)], table_df.schema)
    
    while attempt < 5:
        try:
            existing_max_time = (table_df.filter(col("eventhub_name") == key).agg(max("last_processed_time").alias("max_time")).first()["max_time"])
            # only append if overall_max_time is greater than existing max time or a new record
            if existing_max_time is None or overall_max_time > existing_max_time:
                # append only to avoid orphaned parquert file and delta_log issue
                new_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(edw_eventhub_offsets_table)
            break
        except Exception as e:
            err_msg = str(e).lower()
            if any(x in err_msg for x in ["concurrent", "conflict", "optimistic"]):
                attempt += 1
                wait = 5 * (2 ** (attempt - 1)) 
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to upsert into {edw_eventhub_offsets_table}: {err_msg}")

# process deleted UDM data 
def _process_deleted_data(ingested_max_time, df, credential, namespace, eventhub_name, udm_table_name, logger, schema_id_column_name='key_schema_id', deleted_value_column_name='encoded_key'):
    max_time = ingested_max_time
    if not df.isEmpty():
        key_schema_ids = list(set([row[schema_id_column_name] for row in df.select(schema_id_column_name).filter(col(schema_id_column_name).isNotNull()).collect()]))
        for raw_schema_id in key_schema_ids:
            df_temp = df.filter(col(schema_id_column_name) == raw_schema_id)
            if not raw_schema_id.startswith('avro/binary+'):
                logger.exception(f"[Corrupted deleted UDM data without any AVRO Schema ID - publish dates: {_get_published_dates(df_temp, df_temp.edw_start_time)}]")
                continue
            avro_schema = get_avro_schema(raw_schema_id, credential, namespace, eventhub_name, udm_table_name, logger)
            if avro_schema:
                try:
                    df_temp = df_temp.withColumn("history_time", col("timestamp"))
                    if 'tenant_id' in df_temp.columns and udm_table_name != 'tenant':
                        df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp', 'tenant_id', from_avro(col(deleted_value_column_name), avro_schema).alias('json')])
                        df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp', 'tenant_id', 'json.*'])
                    else:
                        df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp', from_avro(col(deleted_value_column_name), avro_schema).alias('json')])
                        df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp', 'json.*'])
                    df_temp = _add_missed_values(udm_table_name, df_temp)
                    if not df_temp.isEmpty():
                        save_to_table(udm_table_name, df_temp)
                        max_time_ = _get_max_edw_start_time(df_temp, ingested_max_time)
                        if max_time_ > max_time:
                            max_time = max_time_
                        _save_ingested_max_time(namespace, eventhub_name, max_time, logger)
                except Exception as e:
                    logger.exception(f"[Error processing deleted UDM data for table '{udm_table_name}' with schema ID '{raw_schema_id}': {e}]")
    return max_time
# process history and live UDM data
def _process_normal_data(ingested_max_time, df, credential, namespace, eventhub_name, udm_table_name, logger):
    max_time = ingested_max_time
    if not df.isEmpty():
        key_schema_ids = list(set([row['schema_id'] for row in df.select('schema_id').filter(col('schema_id').isNotNull()).collect()]))
        for raw_schema_id in key_schema_ids:
            df_temp = df.filter(col('schema_id') == raw_schema_id)
            if not raw_schema_id.startswith('avro/binary+'):
                logger.exception(f"[Corrupted UDM data without any AVRO Schema ID - publish dates: {_get_published_dates(df_temp, df_temp.edw_start_time)}]")
                continue
            avro_schema = get_avro_schema(raw_schema_id, credential, namespace, eventhub_name, udm_table_name, logger)
            if avro_schema:
                try:
                    df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp',  from_avro(col('Body'), avro_schema).alias('json')])
                    df_temp = df_temp.select(["edw_start_time", "history_time", 'timestamp',  'json.*'])
                    # A publishing issue with duplication????
                    df_temp = df_temp.dropDuplicates()
                    if not df_temp.isEmpty():
                        _save(df_temp, udm_table_name, udm_table_name, udm_table_name, 1)
                        max_time_ = _get_max_edw_start_time(df_temp, ingested_max_time)
                        if max_time_ > max_time:
                            max_time = max_time_
                        _save_ingested_max_time(namespace, eventhub_name, max_time, logger)
                except Exception as e:
                    logger.exception(f"[Error processing UDM data for table '{udm_table_name}' with schema ID '{raw_schema_id}': {e}]")
        return max_time

# process published UDM data per UDM table
def entity_udm_data(ingested_max_time, df, credential, namespace, eventhub_name, udm_table_name, logger):
    logger.info(f"[Process published UDM data with table '{udm_table_name}' ]")
    max_time = ingested_max_time
    try:
        max_time = _process_normal_data(ingested_max_time,  df.filter(df.Body != ''), credential, namespace, eventhub_name, udm_table_name, logger)
    except Exception as e:
        logger.exception(f"[Error processing UDM data for table '{udm_table_name}': {e}]")
    # deletion case after regular case
    try:
        _process_deleted_data(max_time, df.filter(df.Body == ''), credential, namespace, eventhub_name, udm_table_name, logger)
    except Exception as e:
        logger.exception(f"[Error processing deleted UDM data for table '{udm_table_name}': {e}]")

# process published UDM data per UDM module (bundled all module's UDM tables into one single event hub)
def module_udm_data(ingested_max_time, df, module, credential, namespace, eventhub_name, udm_table_name, logger):
    logger.info(f"[Process published UDM data with bundled module '{module}' ]")
    rows = df.select('avro_type').collect()
    # TODO: key rows for deletion ???
    avro_types = list(set([row['avro_type'] for row in rows if not row['avro_type'].endswith('_key')]))
    deletion_avro_types = list(set([row['avro_type'] for row in rows if row['avro_type'].endswith('_key')]))
    
    if len(avro_types) > 0:
        # loop all bundled UDM tables via published module UDM data
        for avro_type in avro_types:
            try:
                # each AVRO type is mapping target module's UDM table - naming pattern:  ....
                temp_df = df.filter(df.avro_type == avro_type)
                table_name = avro_type.split('.')[3]
                table_name = table_name if table_name.startswith(module) else f"{module}_{table_name}"
                entity_udm_data(ingested_max_time, temp_df, credential, namespace, eventhub_name, table_name, logger)
            except Exception as e:
                logger.exception(f"[Error processing UDM data for table '{table_name}': {e}]")
    # deletion case after regular case
    if len(deletion_avro_types) > 0:
        for avro_type in deletion_avro_types:
            try:
                # each AVRO type is mapping target module's UDM table - naming pattern:  ....
                temp_df = df.filter(df.avro_type == avro_type)
                table_name = avro_type.split('.')[3]
                table_name = table_name if table_name.startswith(module) else f"{module}_{table_name}"
                _process_deleted_data(ingested_max_time, temp_df, credential, namespace, eventhub_name, table_name, logger, schema_id_column_name='schema_id', deleted_value_column_name='Body')
            except Exception as e:
                logger.exception(f"[Error processing deleted UDM data for table '{table_name}': {e}]")

# check if specified property exists in AVRO message properties
def _avro_property_exists(df, property_name):
    parts = property_name.split(".")
    try:
        df_temp = df.select(property_name)
        if df_temp:
            row = df_temp.first()
            return row and row[parts[len(parts)-1]]
    except Exception:
        return False
    return False

# process UDM data 
def process_udm_data(ingested_max_time, df, credential, namespace, module, eventhub_name, udm_table_name, logger):
    # new columns: UDM published message header properties
    columns_to_add = {
        'history_time': col("Properties.history_time.member2").cast("timestamp"),
        'timestamp': col("Properties.timestamp.member2").cast("timestamp"),
        'version': col("Properties.version.member2"),
        'schema_id': col("SystemProperties.content-type.member2"),
        'key_schema_id': col("Properties.content-type-partition-key.member2"),
        'encoded_key': col("SystemProperties.x-opt-partition-key.member2").cast("binary"),
        'avro_type': col("Properties.avro-type.member2")
    }

    # only add tenant_id if Properties.tenant_id.member2 exists
    if _avro_property_exists(df, 'Properties.tenant_id.member2'):
        columns_to_add['tenant_id'] = col("Properties.tenant_id.member2")

    df = df.withColumns(columns_to_add)
    # temporarily fix booking UDM without mandatory message header timestamp issue???
    df = df.withColumn('timestamp', when(col('timestamp').isNull(), col('edw_start_time')).otherwise(col('timestamp')))
    
    module_df = df.filter(df.avro_type.isNotNull())
    if not module_df.isEmpty():
        return module_udm_data(ingested_max_time, module_df, module, credential, namespace, eventhub_name, udm_table_name, logger)
    
    entity_df = df.filter(df.avro_type.isNull())
    if not entity_df.isEmpty(): 
        return entity_udm_data(ingested_max_time, entity_df, credential, namespace, eventhub_name, udm_table_name, logger)
# get max ingested time from edw_eventhub_offsets table or UDM table
# if edw_eventhub_offsets table does not exist, get max time from UDM table
# if both tables do not exist, return None
# if both tables exist, return max time from edw_eventhub_offsets table
def _get_ingested_max_time(namespace, eventhub_name, udm_table_name, logger):
    ingested_max_time = None
    try:
        # get max processed time from edw_eventhub_offsets table
        if is_edw_eventhub_offsets_table_exists:
            row = spark.sql(f"select max(last_processed_time) as max_edw_start_time from {edw_eventhub_offsets_table} where eventhub_name = '{namespace}_{eventhub_name}'").first()
            if row and row['max_edw_start_time'] is not None:
                ingested_max_time = row['max_edw_start_time']
        logger.info(f"[Started: ingested max time for namespace: {namespace}, eventhub: {eventhub_name}, UDM table: {udm_table_name} is {ingested_max_time}]")
    except Exception as e:
        logger.warning(f"[Error getting ingested max time from edw_eventhub_offsets table or UDM table: {e}]")
        raise
    return ingested_max_time

# incrementally ingest UDM data with specified UDM table
def ingest_data(udm_table_name, namespace, module, eventhub_name, logger):
    # no udm table name provided, stop
    if not udm_table_name:
        return

    credential = DefaultAzureCredential()
    ingested_max_time = _get_ingested_max_time(namespace, eventhub_name, udm_table_name, logger)
    no_data_message = f"No new data with UDM table: '{udm_table_name}'"
    # UDM data shortcuts
    shortcuts = get_shortcuts()
    has_new_data = False
    # incrmentally scanning UDM data folders
    chunk_paths = get_udm_active_paths(namespace, eventhub_name, ingested_max_time, shortcuts)
    for paths in chunk_paths:
        df = spark.read.option("ssl", "true").option("ignoreCorruptFiles", "true").option("mode", "PERMISSIVE").option("ignoreExtension", "true").option("mergeSchema", "true").format("avro").option("recursiveFileLookup", "true").load(paths)
        if df.isEmpty():
            continue

        df = df.withColumn("edw_start_time", to_timestamp(col("EnqueuedTimeUtc") ,"MM/dd/yyyy hh:mm:ss a").cast("timestamp"))
        if ingested_max_time:
            df = df.filter(df.edw_start_time > ingested_max_time)
            if df.isEmpty():
                continue
        has_new_data = True

        process_udm_data(ingested_max_time, df, credential, namespace, module, eventhub_name, udm_table_name, logger)
    
    if not has_new_data:
        logger.info(no_data_message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Captured UDM Data Ingestion

# CELL ********************

from IPython.utils.capture import capture_output
def run():
    message = "Completed"
    try:
        ingest_data(udm_table_name, namespace, module, eventhub_name, logger)
    except Exception as error:
        logger.exception(f"[Error: {error}]")
        message = "Failed"
    finally:
        finalize(message=message)

if config.environment_name.upper() == 'PROD':
    # no outputs
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

EPTURA_ENVISION_NAME = 'EPTURA ENVISION'
EPTURA_ENVISION_CODE = 'EPTURAENVISION'

sql = """

 WITH tenant_cte AS (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                    FROM tenant
                    WHERE disabled = 0 AND history_time IS NULL
                ),
                license_cte AS (
                    SELECT id, tenant_id, product_id, module_id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                    FROM license
                    WHERE to_date(start_date) <= current_date() AND (end_date IS NULL OR to_date(end_date) >= current_date())
                    AND history_time IS NULL
                ),
                tenant_product_cte AS (
                    SELECT id, tenant_id, product_id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                    FROM tenant_product
                    WHERE is_provisioned = 1 AND history_time IS NULL
                ),
                product_cte AS (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                    FROM product
                    WHERE history_time IS NULL
                )           
                SELECT DISTINCT t.id AS tenant_id, p.id as product_id, l.id as license_id
                FROM tenant_cte t
                INNER JOIN license_cte l ON l.tenant_id = t.id
                INNER JOIN tenant_product_cte tp ON tp.tenant_id = l.tenant_id AND tp.product_id = l.product_id
                INNER JOIN product_cte p ON p.id = tp.product_id
                WHERE t.rn = 1 AND l.rn = 1 AND tp.rn = 1 AND p.rn = 1

"""

display(spark.sql(sql))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("select * from edw_eventhub_offsets"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



def run_once(spark, job_name, callback, control_table= "edw_etl_run_control"):
    # Ensure control table exists
    if not spark.catalog.tableExists(control_table):
        spark.createDataFrame([], "job_name STRING, executed_at TIMESTAMP, status STRING").write.mode("overwrite").saveAsTable(control_table)
    

    #Check for existing successful run
    existing = spark.sql(f"""
        SELECT * FROM {control_table}
        WHERE job_name = '{job_name}'
          AND status = 'success'
    """).collect()

    if existing:
        return "skipped"
    
    # Insert a provisional 'in-progress' record (prevents parallel runs)
    spark.sql(f"""
        MERGE INTO {control_table} t
        USING (SELECT '{job_name}' AS job_name) s
        ON t.job_name = s.job_name
        WHEN NOT MATCHED THEN
          INSERT (job_name, executed_at, status)
          VALUES ('{job_name}', current_timestamp(), 'in-progress')
    """)

    try:
        callback() 
        spark.sql(f"""
            UPDATE {control_table}
            SET executed_at = current_timestamp(), status = 'success'
            WHERE job_name = '{job_name}'
        """)
        return "success"
    except Exception as e:
        spark.sql(f"""
            UPDATE {control_table}
            SET executed_at = current_timestamp(), status = 'failed'
            WHERE job_name = '{job_name}'
        """)
        raise
def test():
    display(spark.sql("select * from edw_etl_run_control")) 
    display(spark.sql("select * from app_access")) 
run_once(spark, 'test3', test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"drop table if exists app_access")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
