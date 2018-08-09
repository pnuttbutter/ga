"""
copy all filters from one view to another
"""


## imports
import os
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import mantis_util as mu
import sys

service_account = #path to service acccount json
a_readonly_scope = 'https://www.googleapis.com/auth/analytics.readonly'
a_edit_scope = 'https://www.googleapis.com/auth/analytics.edit'

output_path = "D:\\aces_etl\\ga\\temp\\"

source_account = #accountID string
dest_account = #accountID string

class Filter:
    def __init__(self, data):
        self.name = data['name']
        self.type = data['type']

class Exclude(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        del data['excludeDetails']['kind']
        self.excludeDetails = data['excludeDetails'] 
        
class Include(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        del data['includeDetails']['kind']
        self.includeDetails = data['includeDetails']

class LowerCase(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        self.lowercaseDetails = data['lowercaseDetails']

class UpperCase(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        self.uppercaseDetails = data['uppercaseDetails']

class SearchAndReplace(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        self.searchAndReplaceDetails = data['searchAndReplaceDetails']

class Advanced(Filter):
    def __init__(self, data):
        Filter.__init__(self,data)
        self.advancedDetails = data['advancedDetails']


## functions
def get_service(api_name, api_version, scopes, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
    service = build(api_name, api_version, credentials=credentials)
    return service

## main
if __name__ == '__main__':

    ## init
    log_file = os.path.join(output_path,'log','ga_filters' + '.txt')
    mu.update_log(log_file, 'exe filepath = ' + os.path.realpath(__file__))
    return_value = 1

    ## connect to api
    mu.update_log(log_file, 'connecting to api')
    a_readonly = get_service(api_name='analytics', api_version='v3', scopes=a_readonly_scope, key_file_location=service_account)
    a_edit = get_service(api_name='analytics', api_version='v3', scopes=a_edit_scope, key_file_location=service_account)

    ## get source account filters
    mu.update_log(log_file, 'getting source filters for ' + source_account)
    source_filters = a_readonly.management().filters().list(accountId=source_account).execute()
    dest_filters = []

    ## populate the destination filter array
    for source_filter in source_filters.get('items',[]):
        if source_filter['type'] == 'EXCLUDE':
            dest_filters.append(Exclude(source_filter).__dict__)
        elif source_filter['type'] == 'INCLUDE':
            dest_filters.append(Include(source_filter).__dict__)
        elif source_filter['type'] == 'LOWERCASE':
            dest_filters.append(LowerCase(source_filter).__dict__)
        elif source_filter['type'] == 'UPPERCASE':
            dest_filters.append(UpperCase(source_filter).__dict__)
        elif source_filter['type'] == 'SEARCH_AND_REPLACE':
            dest_filters.append(SearchAndReplace(source_filter).__dict__)
        elif source_filter['type'] == 'ADVANCED':
            dest_filters.append(Advanced(source_filter).__dict__)

    ## insert each filter into the destination account
    mu.update_log(log_file, 'importing filters into ' + dest_account)
    for dest_filter in dest_filters:
        try:
            a_edit.management().filters().insert(accountId=dest_account, body=dest_filter).execute()
            mu.update_log(log_file, 'SUCCESS - ' + dest_filter['name'])
        except Exception as e:
            mu.update_log(log_file, 'FAIL - ' + dest_filter['name']) 

    mu.update_log(log_file, 'complete - return code = ' + str(return_value))
    sys.exit(return_value)
        
