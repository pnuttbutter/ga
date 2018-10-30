"""
requires a bucket setup with the config & schema for each table to be saved in /etl/
each task in config definted as:
	{
		"destination_table"	: "account"
		, "source_file"		: "account.json"
		, "schema_file"		: "ga.account_schema.json"
		, "write_disposition"	: "WRITE_TRUNCATE"	
		, "source_format"	: "NEWLINE_DELIMITED_JSON"
		, "dataset"		: "ga"
	}
code retrieves config, then iterates over tasks to retreive the a table schema which is used to create a load job
from gcs to bq

second part of design pattern - after ingestion to gcs, new files are ingested into truncated tables.
"""

from google.cloud import storage
from google.cloud import bigquery as bq
import mantis_util as mu
import json
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\aces_etl\\Springer Nature Analytics-3214f42aeecc.json"

gcs_bucket = 'mantis_staging'
gcs_path = 'ga/etl/'
etl_config = 'etl_config.json'
output_path = "D:\\aces_etl\\ga\\temp\\"

class Schema:    
    def __init__(self, bucket,path):
        self.schema = []
        self.import_schema = self.retrieve_schema(bucket,path)

    def retrieve_schema (self,schema_bucket, schema_path):
        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(schema_bucket)
        blob = bucket.get_blob(schema_path)
        return blob.download_as_string()   

    def process_schema (self):        
        for field_dict in json.loads(self.import_schema):
            self.schema.append(self.get_field_schema(field_dict))               
        
    def get_field_schema(self,field_dict):
        if field_dict['type'] == 'RECORD':
            x = []
            for field in field_dict['fields']:
                x.append(self.get_field_schema(field))
            fields = tuple(x)            
        else:
            fields = ()
        return bq.SchemaField(name = field_dict['name'], field_type = field_dict['type'], mode =field_dict['mode'], fields = fields)  

## main
if __name__ == '__main__':
    ## initialise
    gcs_client = storage.Client()
    bq_client = bq.Client()
    log_file = os.path.join(output_path,'log', 'etl_gcs_to_bq' + '.txt')    
    
    ## import etl config
    mu.update_log(log_file, 'importing etl config')
    bucket = gcs_client.get_bucket(gcs_bucket)
    blob = bucket.get_blob(gcs_path + etl_config)
    task_config = json.loads(blob.download_as_string())

    ## iterate over task_config
    for task in task_config:
        mu.update_log(log_file, 'importing schema ' + task['destination_table'])
        job_schema = Schema(gcs_bucket,gcs_path + task['schema_file'])
        job_schema.process_schema()
        mu.update_log(log_file, 'running import for ' + task['destination_table'])
        dataset_ref = bq_client.dataset(task['dataset'])
        job_config = bq.LoadJobConfig()
        job_config.write_disposition = task['write_disposition']
        job_config.source_format = task['source_format']
        job_config.schema = job_schema.schema
        source_uri = 'gs://' + gcs_bucket + '/'+ task['dataset'] + '/' + task['source_file']

        try:
            load_job = bq_client.load_table_from_uri(source_uri, dataset_ref.table(task['destination_table']), job_config = job_config)
            assert load_job, job_type == 'load'
            load_job.result()
            assert load_job.state == 'DONE'
            assert bq_client.get_table(dataset_ref.table(task['destination_table'])).num_rows > 0
        except Exception as e:
            print e
        
