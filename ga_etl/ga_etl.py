"""
code calls the ga api to extract the following entity data:
* account
* property (aka web property)
* view (aka profile)
* custom dimensions
* account filters
* view filters
* account users
* property users
* view users

...and saves them to a local folder in JSON format
these files are them loaded into a gcs bucket
"""

## imports
import os
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import json
import mantis_util as mu
import sys
import time
from google.cloud import storage


## variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = #keyfile location
gcs_bucket = 'mantis_staging'
gcs_path = 'ga/'

service_account = #keyfile location
a_scope = 'https://www.googleapis.com/auth/analytics.readonly'
au_scope = 'https://www.googleapis.com/auth/analytics.manage.users.readonly'

output_path = #log output path
entities = {'account':{},'property':{},'view':{}, 'view_filter':[], 'account_filter':[], 'account_user':[], 'custom_dimension':[], 'view_user':[], 'property_user':[]}

## functions
def get_service(api_name, api_version, scopes, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
    service = build(api_name, api_version, credentials=credentials)
    return service

def write_to_new_line_json(items, output_path, entity):
    try:
        #filename = os.path.join(output_path, entity + '_' + last_run_date + '.json')
        filename = os.path.join(output_path, entity + '.json')
        
        with open(filename,'w') as out:
            if k in ['account', 'property', 'view']:
                for item in items:
                    json.dump(item,out)
                    out.write('\n')
            else:
                for d in items:
                    for item in d:
                        json.dump(item,out)
                        out.write('\n')
                    
        return True, filename
    except Exception as e:
        mu.update_log(log_file, 'write_to_new_line_json' + str(e))
        return False, None

def wait_check(query_count):
    if query_count == 1900:
        mu.update_log(log_file, 'quick snooze')
        time.sleep(100)
        query_count = 0
    
## main
if __name__ == '__main__':
    
    ## initialise
    log_file = os.path.join(output_path,'log', 'ga_etl' + '.txt')    
    return_value= 1
    files_to_upload = []
    query_count = 0

    ## connects to api
    mu.update_log(log_file, 'connecting to api service')
    a_serv = get_service(api_name='analytics', api_version='v3', scopes=a_scope, key_file_location=service_account)
    au_serv = get_service(api_name='analytics', api_version='v3', scopes=au_scope, key_file_location=service_account)

    ## extracts accounts, properties & views
    mu.update_log(log_file, 'extracting accounts, properties & views')
    entities['account'] = (a_serv.management().accounts().list().execute()).get('items',[])
    entities['property'] = (a_serv.management().webproperties().list(accountId='~all').execute()).get('items',[])
    entities['view'] = (a_serv.management().profiles().list(accountId='~all',webPropertyId='~all').execute()).get('items',[])
    query_count += 3

    ## iterates over accounts
    mu.update_log(log_file, 'iterate over accounts')  
    for a in entities['account']:
        ## populates account_filter & account_user
        entities['account_filter'].append((a_serv.management().filters().list(accountId=a['id']).execute()).get('items',[]))                                        
        entities['account_user'].append((au_serv.management().accountUserLinks().list(accountId=a['id']).execute()).get('items',[]))
        query_count += 2

    ## iterates over properties
    mu.update_log(log_file, 'iterate over properties')
    for p in entities['property']:
        ## populates property_user & custom_dimension
        entities['property_user'].append((au_serv.management().webpropertyUserLinks().list(accountId = p['accountId'], webPropertyId = p['id']).execute()).get('items',[]))
        entities['custom_dimension'].append((a_serv.management().customDimensions().list(accountId = p['accountId'], webPropertyId = p['id']).execute()).get('items',[]))
        query_count += 2
        wait_check(query_count)

    ## iterates over views
    mu.update_log(log_file, 'iterate over views')  
    for v in entities['view']:            
        ## populates view_filter & view_user
        entities['view_filter'].append((a_serv.management().profileFilterLinks().list(accountId = v['accountId'], webPropertyId = v['webPropertyId'], profileId = v['id']).execute()).get('items',[]))
        entities['view_user'].append((au_serv.management().profileUserLinks().list(accountId = v['accountId'], webPropertyId = v['webPropertyId'], profileId = v['id']).execute()).get('items',[]))  
        query_count += 2

    ## write to json and add to list
    mu.update_log(log_file, 'write to json')      
    for k,v in entities.iteritems():        
        success, filename = write_to_new_line_json(v, output_path, k)
        files_to_upload.append({'success':success, 'filename':filename, 'entity':k})    
             
    ## upload to gcs
    mu.update_log(log_file, 'upload to gcs')
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(gcs_bucket)
    for file_to_upload in files_to_upload:
        if file_to_upload['success']:
            mu.update_log(log_file, 'uploading ' + file_to_upload['entity'] )
            blob = bucket.blob(gcs_path + file_to_upload['entity'] + '.json' )
            blob.upload_from_filename(file_to_upload['filename'])
    

    

    mu.update_log(log_file, 'complete - return code = ' + str(return_value))    
    

    
    
    
