"""
for a specified account, link all filters set at the account level
to a specified view
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
dest_view = #ID string for view/profile

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

    link_filters = []

    ## get all filter refs for the specified account
    mu.update_log(log_file, 'getting source filters for ' + source_account)
    link_filters = []
    source_filter_ids = a_readonly.management().filters().list(accountId=source_account).execute()
    [link_filters.append(source_filter_id['id']) for source_filter_id in source_filter_ids.get('items',[]) ]
    
    ## get the web property id, view id & view name for the views associated with specified account
    link_views = []
    views = a_readonly.management().profiles().list(accountId=source_account, webPropertyId='~all').execute()
    [link_views.append({'web_property_id':view['webPropertyId'], 'view_id' : view['id'], 'view_name':view['name']})  for view in views.get('items', [])]

    ## insert a link to each filter to specifed view
    for link_view in link_views:
        if link_view['view_id'] == dest_view:
            for link_filter in link_filters:
                try:
                    a_edit.management().profileFilterLinks().insert(accountId=source_account, webPropertyId=link_view['web_property_id'],profileId=link_view['view_id'],body={'filterRef': {'id': link_filter}}).execute()
                    mu.update_log(log_file, 'SUCCESS - ' + link_filter + ' added to ' + link_view['view_name'])
                except Exception as e:
                    mu.update_log(log_file, 'FAIL - ' + link_filter + ' not added to ' + link_view['view_name'])
                    return_value = 2
                    pass
                
    ## finalise            
    mu.update_log(log_file, 'complete - return code = ' + str(return_value))
    sys.exit(return_value)
