import sys
import boto3
import openpyxl
import pandas as pd
import numpy as np
import pytz
import json
import urllib
import math
import time
import re
from io import BytesIO
import io
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR
from datetime import datetime,timedelta,timezone,date,time
import requests
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth
from decimal import Decimal
import base64
from urllib.parse import quote
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_der_private_key

#Function to fetch secrets from secrets manager
def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets

#Function to extract secrets fetched secrets
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['graph_secret_id','graph_client_id','graph_tenant_id','sharepoint_url_base','sharepoint_url_end',
'zoom_client_id','zoom_account_id','zoom_secret_id','zoom_webinar_user_ids','snowflake_bizops_user','snowflake_account','snowflake_salesmarketing_schema','snowflake_fivetran_db','snowflake_bizops_role','snowflake_key_pass','snowflake_bizops_wh']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

#Initialize secret values
graph_secret = extracted_secrets['graph_secret_id']['graph_secret_id']
graph_client_id = extracted_secrets['graph_client_id']['graph_client_id']
graph_tenant_id = extracted_secrets['graph_tenant_id']['graph_tenant_id']
sharepoint_url_base = extracted_secrets['sharepoint_url_base']['sharepoint_url_base']
sharepoint_url_end = extracted_secrets['sharepoint_url_end']['sharepoint_url_end']
zoom_client_id = extracted_secrets['zoom_client_id']['zoom_client_id']
zoom_account_id = extracted_secrets['zoom_account_id']['zoom_account_id']
zoom_secret_id = extracted_secrets['zoom_secret_id']['zoom_secret_id']
zoom_webinar_user_id_raw = extracted_secrets['zoom_webinar_user_ids']['zoom_webinar_user_ids']
zoom_user_ids = [i for i in zoom_webinar_user_id_raw.split(',') if i.strip()]
snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_schema = extracted_secrets['snowflake_salesmarketing_schema']['snowflake_salesmarketing_schema']
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']

#Convert password string to bytes
password = snowflake_key_pass.encode()

#AWS S3 Configuration params
s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

#Function to download file from S3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

#Download the private key file from S3
key_data = download_from_s3(s3_bucket, s3_key)

#Try loading the private key as PEM
private_key = load_pem_private_key(key_data, password=password)

#Extract the private key bytes in PKCS8 format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)


#Use the Microsfot Graph API to get the cognito form stream and provider jumpoff lists
secret = graph_secret
client_id = graph_client_id
tenant_id = graph_tenant_id

url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

#Fetch Graph API auth token
data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': secret,
    'scope':  'https://graph.microsoft.com/.default'}
response = requests.post(url, data=data)
response_json = response.json()
access_token = response_json.get('access_token')

url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url_base}:/personal/{sharepoint_url_end}"

headers = {
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)
site_data = response.json()
site_id = site_data.get("id")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)

drive_id = None
if response.status_code == 200:
    drives = response.json().get('value', [])
    for drive in drives:
        if drive['name']== 'OneDrive':
            drive_id = drive['id']
            break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'

headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)
items = response.json()

for item in items['value']:
    if item['name'] == 'Desktop':
        item_id = item['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
children = response.json().get('value', [])

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
children = response.json().get('value', [])

for child in children:
    if child['name'] == 'Cognito':
        child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{child_item_id}/children'

response = requests.get(url, headers=headers)
nested_children = response.json().get('value', [])

for child in nested_children:
    if child['name'] == 'Michigan':
        nested_child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{nested_child_item_id}/children'

response = requests.get(url, headers=headers)
nested_children_final = response.json().get('value', [])

for child in nested_children_final:
    if child['name'] == 'Michigan Provider Jumpoff.xlsx':
        final_nested_child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{final_nested_child_item_id}/content'

response = requests.get(url, headers=headers)
michigan_jumpoff = pd.read_excel(BytesIO(response.content), dtype={'Provider TAX ID': str})

url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': secret,
    'scope':  'https://graph.microsoft.com/.default'}
response = requests.post(url, data=data)
response_json = response.json()
access_token = response_json.get('access_token')

url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url_base}:/personal/{sharepoint_url_end}"
headers = {
    "Authorization": f"Bearer {access_token}"
}
response = requests.get(url, headers=headers)
site_data = response.json()
site_id = site_data.get("id")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)

drive_id = None
if response.status_code == 200:
    drives = response.json().get('value', [])
    for drive in drives:
        if drive['name']== 'OneDrive':
            drive_id = drive['id']
            break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'

headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)
items = response.json()

for item in items['value']:
    if item['name'] == 'Cognito Forms':
        item_id = item['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'


response = requests.get(url, headers=headers)
children = response.json().get('value', [])

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
children = response.json().get('value', [])

for child in children:
    if child['name'] == 'Michigan':
        child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{child_item_id}/children'

response = requests.get(url, headers=headers)
nested_children_final = response.json().get('value', [])

for child in nested_children_final:
    if child['name'] == 'Michigan_Stream.xlsx':
        final_nested_child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{final_nested_child_item_id}/content'

response = requests.get(url, headers=headers)
cognito_form = pd.read_excel(BytesIO(response.content))

url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url_base}:/personal/{sharepoint_url_end}"

headers = {
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)
site_data = response.json()
site_id = site_data.get("id")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)

drive_id = None
if response.status_code == 200:
    drives = response.json().get('value', [])
    for drive in drives:
        if drive['name']== 'OneDrive':
            drive_id = drive['id']
            break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'

headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)
items = response.json()

for item in items['value']:
    if item['name'] == 'Cognito Forms':
        item_id = item['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
sub_items = response.json()

for item in sub_items['value']:
    if item['name'] == 'Michigan':
        item_id = item['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
child_items = response.json()

for item in child_items['value']:
    if item['name'] == 'MI_Payers':
        item_id = item['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
sub_child_items = response.json()

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children'

response = requests.get(url, headers=headers)
nested_children_final = response.json().get('value', [])

for child in nested_children_final:
    if child['name'] == 'MI_Payers.xlsx':
        final_nested_child_item_id = child['id']
        break

url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{final_nested_child_item_id}/content'

response = requests.get(url, headers=headers)
michigan_payers = pd.read_excel(BytesIO(response.content), dtype={'Provider TAX ID': str})

#Function to format Cognito data into usable format
def reformat_df(df):
    df['FederalTaxID'] = df.groupby('MichiganDepartmentOfHealthAndHu_Id')['FederalTaxID'].transform(lambda x: x.ffill().bfill())
    df['NPI'] = df.groupby('MichiganDepartmentOfHealthAndHu_Id')['NPI'].transform(lambda x: x.ffill().bfill())
    
    df['DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService'] = df.groupby('MichiganDepartmentOfHealthAndHu_Id')['DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService'].transform(lambda x: x.bfill().ffill())
    
    df = df.drop_duplicates(subset=['MichiganDepartmentOfHealthAndHu_Id', 'FederalTaxID'])
    
    return df

cognito_form_formatted = reformat_df(cognito_form)

cognito_form_formatted = cognito_form_formatted.dropna(subset=['FederalTaxID'])

cognito_form_formatted['FederalTaxID'] = cognito_form_formatted['FederalTaxID'].astype(int).astype(str)

#Set ZOOM API Credentials
credentials = f"{zoom_client_id}:{zoom_secret_id}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()


headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {encoded_credentials}"
}

body = {
    "grant_type": "account_credentials",
    "account_id": zoom_account_id
}

token_url = "https://zoom.us/oauth/token"

#Fetch ZOOM Auth token
response = requests.post(token_url, headers=headers, data=body)
access_token = response.json().get('access_token')

#All webinars will come from one of these two users (hhaexchangewebinar,providerexperience)
user_ids = zoom_user_ids

headers = {
    "Authorization": f"Bearer {access_token}"
}

def clean_tax_ids(column):
    def clean_value(x):
        if isinstance(x, str):
            return re.sub(r'[-\s]', '', x)
        elif pd.isnull(x):
            return np.nan
        else:
            return str(int(x))

    return column.apply(clean_value)

#Function to preprocess registrants and extract custom questions
def preprocess_registrants(registrants):
    for registrant in registrants:
        #Flatten custom questions
        for question in registrant.get('custom_questions', []):
            #Use the question title as the column name and its value as the value
            column_name = question['title']
            registrant[column_name] = question['value']
        registrant.pop('custom_questions', None)
    return registrants

#Function to construct URL for effective pagination of webinar participants
def construct_url(instance_id, next_page_token=None):
    url = f"https://api.zoom.us/v2/past_webinars/{instance_id}/participants"
    if next_page_token:
        url += f"?next_page_token={next_page_token}"
    return url

#Function to construct URL for effective pagination of webinar registrants
def construct_url_pre(webinar_id, next_page_token=None):
    url = f"https://api.zoom.us/v2/webinars/{webinar_id}/registrants"
    if next_page_token:
        url += f"?next_page_token={next_page_token}"
    return url

#Fetch all necessary webinar data for this session
try:
    info_session_all_webinars = []
    info_session_all_instances = []
    info_session_all_webinar_details = []
    info_session_all_webinar_details_reg = []

    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            info_session_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
        
    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(info_session_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Informational Session Webinar']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuid
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        info_session_all_instances.extend(instances_data.get('webinars', []))

    info_session_occurrence_ids = [occurrence['uuid'] for occurrence in info_session_all_instances]

    #Iterate through each occurence to get participant details
    for instance in info_session_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            info_session_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    webinars_df_participants = pd.DataFrame(info_session_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                info_session_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break
        
        #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                info_session_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    webinars_df_registrants = pd.DataFrame(info_session_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(webinars_df_participants) > 0:
        info_session_merged_df = pd.merge(webinars_df_registrants,webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        info_session_merged_df = webinars_df_registrants

    #Call clean TaxID function
    info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass


#Fetch all necessary webinar data for this session
try:
    edi_all_webinars = []
    edi_all_instances = []
    edi_all_webinar_details = []
    edi_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            edi_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(edi_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Department of Health and Human Services: EDI Provider Onboarding Webinar']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        edi_all_instances.extend(instances_data.get('webinars', []))

    edi_occurrence_ids = [occurrence['uuid'] for occurrence in edi_all_instances]

    #Iterate through each occurence to get participant details
    for instance in edi_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            edi_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    edi_webinars_df_participants = pd.DataFrame(edi_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                edi_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                edi_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    edi_webinars_df_registrants = pd.DataFrame(edi_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(edi_webinars_df_participants) >0:
        edi_webinar_merged_df = pd.merge(edi_webinars_df_registrants,edi_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        edi_webinar_merged_df = edi_webinars_df_registrants

    #Call clean TaxID function
    edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

#Fetch all necessary webinar data for this session
try:
    sut_all_webinars = []
    sut_all_instances = []
    sut_all_webinar_details = []
    sut_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            sut_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(sut_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Department of Health and Human Services System User Training']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        sut_all_instances.extend(instances_data.get('webinars', []))

    sut_occurrence_ids = [occurrence['uuid'] for occurrence in sut_all_instances]

    #Iterate through each occurence to get participant details
    for instance in sut_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            sut_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    sut_df_participants = pd.DataFrame(sut_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                sut_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

       #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                sut_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    sut_webinars_df_registrants = pd.DataFrame(sut_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(sut_df_participants) > 0:
        sut_webinar_merged_df = pd.merge(sut_webinars_df_registrants,sut_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        sut_webinar_merged_df = sut_webinars_df_registrants

    #Call clean TaxID function
    sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

#Fetch all necessary webinar data for this session
try:
    gs_all_webinars = []
    gs_all_instances = []
    gs_all_webinar_details = []
    gs_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            gs_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(gs_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Health and Human Services Getting Started Webinar']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        gs_all_instances.extend(instances_data.get('webinars', []))

    gs_occurrence_ids = [occurrence['uuid'] for occurrence in gs_all_instances]

    #Iterate through each occurence to get participant details
    for instance in gs_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            gs_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    gs_webinars_df_participants = pd.DataFrame(gs_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

         #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                gs_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                gs_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    gs_webinars_df_registrants = pd.DataFrame(gs_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(gs_webinars_df_participants)>0:
        gs_webinar_merged_df = pd.merge(gs_webinars_df_registrants,gs_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        gs_webinar_merged_df = gs_webinars_df_registrants

    #Call clean TaxID function
    gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

#Fetch all necessary webinar data for this session
try:
    openhours_all_webinars = []
    openhours_all_instances = []
    openhours_all_webinar_details = []
    openhours_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            openhours_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(openhours_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'MDHHS - HHAX Open Hours - Onboarding and Adoption Training']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        openhours_all_instances.extend(instances_data.get('webinars', []))

    openhours_occurrence_ids = [occurrence['uuid'] for occurrence in openhours_all_instances]

    #Iterate through each occurence to get participant details
    for instance in openhours_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            openhours_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    openhours_webinars_df_participants = pd.DataFrame(openhours_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                openhours_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                openhours_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    openhours_webinars_df_registrants = pd.DataFrame(openhours_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(openhours_webinars_df_participants) > 0:
        openhours_webinar_merged_df = pd.merge(openhours_webinars_df_registrants,openhours_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        openhours_webinar_merged_df = openhours_webinars_df_registrants

    #Call clean TaxID function
    openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

    #####Home Help Section#####

    #Fetch all necessary webinar data for this session
try:
    hh_info_session_all_webinars = []
    hh_info_session_all_instances = []
    hh_info_session_all_webinar_details = []
    hh_info_session_all_webinar_details_reg = []

    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            hh_info_session_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
        
    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    df_webinars = pd.DataFrame(hh_info_session_all_webinars)
    filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Home Help Information Session Webinar']
    webinar_id_isolated = filtered_df['id']
    webinar_ids = webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuid
    for webinar_id in webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        hh_info_session_all_instances.extend(instances_data.get('webinars', []))

    hh_info_session_occurrence_ids = [occurrence['uuid'] for occurrence in hh_info_session_all_instances]

    #Iterate through each occurence to get participant details
    for instance in hh_info_session_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            hh_info_session_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    hh_is_webinars_df_participants = pd.DataFrame(hh_info_session_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                hh_info_session_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break
        
        #Get registrants for webinars that have not yet occurred
        for webinar_id in webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                hh_info_session_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    hh_is_webinars_df_registrants = pd.DataFrame(hh_info_session_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(hh_is_webinars_df_participants) > 0:
        hh_info_session_merged_df = pd.merge(hh_is_webinars_df_registrants,hh_is_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        hh_info_session_merged_df = hh_is_webinars_df_registrants

    #Call clean TaxID function
    hh_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(hh_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

#Fetch all necessary webinar data for this session
try:
    hh_edi_all_webinars = []
    hh_edi_all_instances = []
    hh_edi_all_webinar_details = []
    hh_edi_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            hh_edi_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    hh_edi_df_webinars = pd.DataFrame(hh_edi_all_webinars)
    hh_edi_filtered_df = hh_edi_df_webinars[hh_edi_df_webinars['topic'] == ' Michigan Home Help: EDI Provider Onboarding Webinar']
    hh_edi_webinar_id_isolated = hh_edi_filtered_df['id']
    hh_edi_webinar_ids = hh_edi_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in hh_edi_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        hh_edi_all_instances.extend(instances_data.get('webinars', []))

    hh_edi_occurrence_ids = [occurrence['uuid'] for occurrence in hh_edi_all_instances]

    #Iterate through each occurence to get participant details
    for instance in hh_edi_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            hh_edi_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    hh_edi_webinars_df_participants = pd.DataFrame(hh_edi_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in hh_edi_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                hh_edi_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in hh_edi_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                hh_edi_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    hh_edi_webinars_df_registrants = pd.DataFrame(hh_edi_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(hh_edi_webinars_df_participants) >0:
        hh_edi_webinar_merged_df = pd.merge(hh_edi_webinars_df_registrants,hh_edi_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        hh_edi_webinar_merged_df = hh_edi_webinars_df_registrants

    #Call clean TaxID function
    hh_edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(hh_edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

    #Fetch all necessary webinar data for this session
try:
    hh_sut_all_webinars = []
    hh_sut_all_instances = []
    hh_sut_all_webinar_details = []
    hh_sut_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            hh_sut_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    hh_sut_df_webinars = pd.DataFrame(hh_sut_all_webinars)
    hh_sut_filtered_df = hh_sut_df_webinars[hh_sut_df_webinars['topic'] == 'System User Training - Michigan Home Help']
    hh_sut_webinar_id_isolated = hh_sut_filtered_df['id']
    hh_sut_webinar_ids = hh_sut_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in hh_sut_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        hh_sut_all_instances.extend(instances_data.get('webinars', []))

    hh_sut_occurrence_ids = [occurrence['uuid'] for occurrence in hh_sut_all_instances]

    #Iterate through each occurence to get participant details
    for instance in hh_sut_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            hh_sut_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    hh_sut_webinars_df_participants = pd.DataFrame(hh_sut_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in hh_sut_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                hh_sut_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in hh_sut_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                hh_sut_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    hh_sut_webinars_df_registrants = pd.DataFrame(hh_sut_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(hh_sut_webinars_df_participants) >0:
        hh_sut_webinar_merged_df = pd.merge(hh_sut_webinars_df_registrants,hh_sut_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        hh_sut_webinar_merged_df = hh_sut_webinars_df_registrants

    #Call clean TaxID function
    hh_sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(hh_sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

try:
    hh_oh_all_webinars = []
    hh_oh_all_instances = []
    hh_oh_all_webinar_details = []
    hh_oh_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            hh_oh_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    hh_oh_df_webinars = pd.DataFrame(hh_oh_all_webinars)
    hh_oh_filtered_df = hh_oh_df_webinars[hh_oh_df_webinars['topic'] == 'Michigan Home Help - HHAeXchange Open Hours - Onboarding and Adoption Training']
    hh_oh_webinar_id_isolated = hh_oh_filtered_df['id']
    hh_oh_webinar_ids = hh_oh_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in hh_oh_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        hh_oh_all_instances.extend(instances_data.get('webinars', []))

    hh_oh_occurrence_ids = [occurrence['uuid'] for occurrence in hh_oh_all_instances]

    #Iterate through each occurence to get participant details
    for instance in hh_oh_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            hh_oh_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    hh_oh_webinars_df_participants = pd.DataFrame(hh_oh_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in hh_oh_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                hh_oh_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in hh_oh_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                hh_oh_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    hh_oh_webinars_df_registrants = pd.DataFrame(hh_oh_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(hh_oh_webinars_df_participants) >0:
        hh_oh_webinar_merged_df = pd.merge(hh_oh_webinars_df_registrants,hh_oh_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        hh_oh_webinar_merged_df = hh_oh_webinars_df_registrants

    #Call clean TaxID function
    hh_oh_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(hh_oh_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

try:
    hh_gs_all_webinars = []
    hh_gs_all_instances = []
    hh_gs_all_webinar_details = []
    hh_gs_all_webinar_details_reg = []

    #Get all webinar IDs for these two users
    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            hh_gs_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break

    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    hh_gs_df_webinars = pd.DataFrame(hh_gs_all_webinars)
    hh_gs_filtered_df = hh_gs_df_webinars[hh_gs_df_webinars['topic'] == 'MIchigan Home Help - Getting Started Webinar']
    hh_gs_webinar_id_isolated = hh_gs_filtered_df['id']
    hh_gs_webinar_ids = hh_gs_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuids
    for webinar_id in hh_gs_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        hh_gs_all_instances.extend(instances_data.get('webinars', []))

    hh_gs_occurrence_ids = [occurrence['uuid'] for occurrence in hh_gs_all_instances]

    #Iterate through each occurence to get participant details
    for instance in hh_gs_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            hh_gs_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    hh_gs_webinars_df_participants = pd.DataFrame(hh_gs_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in hh_gs_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids_2:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                hh_gs_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

        #Get registrants for webinars that have not yet occurred
        for webinar_id in hh_gs_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                hh_gs_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    hh_gs_webinars_df_registrants = pd.DataFrame(hh_gs_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(hh_gs_webinars_df_participants) >0:
        hh_gs_webinar_merged_df = pd.merge(hh_gs_webinars_df_registrants,hh_gs_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        hh_gs_webinar_merged_df = hh_gs_webinars_df_registrants

    #Call clean TaxID function
    hh_gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(hh_gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

    #####PCS Section#####

try:
    getting_ready_all_webinars = []
    getting_ready_all_instances = []
    getting_ready_all_webinar_details = []
    getting_ready_all_webinar_details_reg = []

    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            getting_ready_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
        
    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    gr_df_webinars = pd.DataFrame(getting_ready_all_webinars)
    gr_filtered_df = gr_df_webinars[gr_df_webinars['topic'] == 'Get Ready for EVV - Michigan']
    gr_webinar_id_isolated = gr_filtered_df['id']
    gr_webinar_ids = gr_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuid
    for webinar_id in gr_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        getting_ready_all_instances.extend(instances_data.get('webinars', []))

    getting_ready_occurrence_ids = [occurrence['uuid'] for occurrence in getting_ready_all_instances]

    #Iterate through each occurence to get participant details
    for instance in getting_ready_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            getting_ready_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    getting_ready_webinars_df_participants = pd.DataFrame(getting_ready_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in gr_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                getting_ready_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break
        
        #Get registrants for webinars that have not yet occurred
        for webinar_id in gr_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                getting_ready_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    getting_ready_df_registrants = pd.DataFrame(getting_ready_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(getting_ready_webinars_df_participants) > 0:
        getting_ready_merged_df = pd.merge(getting_ready_df_registrants,getting_ready_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        getting_ready_merged_df = getting_ready_df_registrants

    #Call clean TaxID function
    getting_ready_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(getting_ready_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

try:
    pcs_info_session_all_webinars = []
    pcs_info_session_all_instances = []
    pcs_info_session_all_webinar_details = []
    pcs_info_session_all_webinar_details_reg = []

    for user_id in user_ids:
        base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
        webinars_url = base_url
        next_page_token = None

        while True:
            if next_page_token:
                webinars_url = f"{base_url}?next_page_token={next_page_token}"
            
            response = requests.get(webinars_url, headers=headers)
            data = response.json()
            pcs_info_session_all_webinars.extend(data['webinars'])
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
        
    #Store all webinar data in df, filter to only include in scope session. Isolate ids into list for use later
    pcs_info_session_df_webinars = pd.DataFrame(pcs_info_session_all_webinars)
    pcs_info_session_df_webinars['topic'] = pcs_info_session_df_webinars['topic'].str.strip()
    pcs_info_session_filtered_df = pcs_info_session_df_webinars[pcs_info_session_df_webinars['topic'] == 'Michigan EVV Info Session - September Go Live']
    pcs_info_session_webinar_id_isolated = pcs_info_session_filtered_df['id']
    pcs_info_session_webinar_ids = pcs_info_session_webinar_id_isolated.to_list()

    #Iterate through each webinar id to fetch all unique occurence uuid
    for webinar_id in pcs_info_session_webinar_ids:
        webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
        response = requests.get(webinar_url,headers=headers)
        instances_data = response.json()
        pcs_info_session_all_instances.extend(instances_data.get('webinars', []))

    pcs_info_session_occurrence_ids = [occurrence['uuid'] for occurrence in pcs_info_session_all_instances]

    #Iterate through each occurence to get participant details
    for instance in pcs_info_session_occurrence_ids:
        next_page_token = None
        while True:
            participants_url = construct_url(instance, next_page_token)
            response = requests.get(participants_url, headers=headers)
            participants_data = response.json()
            pcs_info_session_all_webinar_details.extend(participants_data.get('participants', []))
            next_page_token = participants_data.get('next_page_token')

            if not next_page_token:
                break

    #Store participant results in df
    pcs_info_session_webinars_df_participants = pd.DataFrame(pcs_info_session_all_webinar_details)

    #Extract occurence ids once again, this time extracting the id and not uuid
    for webinar_id in gr_webinar_ids:
        webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
        response = requests.get(webinar_url, headers=headers)
        webinar_data = response.json()
        occurrences = webinar_data.get('occurrences', [])
        
        occurrence_ids = [occurrence['occurrence_id'] for occurrence in occurrences]

        #Iterate through each occurence, store all registrant data
        for occurrence_id in occurrence_ids:
            next_page_token = ' '
            
            while True:
                webinars_url = f'https://api.zoom.us/v2/webinars/{webinar_id}/registrants?occurrence_id={occurrence_id}&next_page_token={next_page_token}'
                response = requests.get(webinars_url, headers=headers)
                registrant_data = response.json()
                registrants = preprocess_registrants(registrant_data.get('registrants', []))
                pcs_info_session_all_webinar_details_reg.extend(registrants)
                next_page_token = registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break
        
        #Get registrants for webinars that have not yet occurred
        for webinar_id in gr_webinar_ids:
            next_page_token = None
                        
            while True:
                pre_webinar_url = construct_url_pre(webinar_id,next_page_token)
                response = requests.get(pre_webinar_url, headers=headers)
                pre_registrant_data = response.json()
                pre_registrants = preprocess_registrants(pre_registrant_data.get('registrants', []))
                pcs_info_session_all_webinar_details_reg.extend(pre_registrants)
                next_page_token = pre_registrant_data.get('next_page_token', '')

                if not next_page_token:
                    break

    #Store registrants in df
    pcs_info_session_df_registrants = pd.DataFrame(pcs_info_session_all_webinar_details_reg)

    #Merge registrant and partcipant dataframes
    if len(pcs_info_session_webinars_df_participants) > 0:
        pcs_info_session_merged_df = pd.merge(pcs_info_session_df_registrants,pcs_info_session_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')
    else:
        pcs_info_session_merged_df = pcs_info_session_df_registrants

    #Call clean TaxID function
    pcs_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(pcs_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except Exception as e:
    pass

#Add column in cases where no attendance is yet available
if 'status_y' not in info_session_merged_df.columns:
    info_session_merged_df['status_y'] = np.nan

if 'status_y' not in edi_webinar_merged_df.columns:
    edi_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in sut_webinar_merged_df.columns:
    sut_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in gs_webinar_merged_df.columns:
    gs_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in openhours_webinar_merged_df.columns:
    gs_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in hh_info_session_merged_df.columns:
    hh_info_session_merged_df['status_y'] = np.nan

if 'status_y' not in hh_edi_webinar_merged_df.columns:
    hh_edi_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in hh_sut_webinar_merged_df.columns:
    hh_sut_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in hh_oh_webinar_merged_df.columns:
    hh_oh_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in hh_gs_webinar_merged_df.columns:
    hh_gs_webinar_merged_df['status_y'] = np.nan

if 'status_y' not in getting_ready_merged_df.columns:
    getting_ready_merged_df['status_y'] = np.nan
    
if 'status_y' not in pcs_info_session_merged_df.columns:
    pcs_info_session_merged_df['status_y'] = np.nan

#Distinguish between attendees and registress for each webinar
try:
    info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    info_session_in_meeting_df = info_session_merged_df[info_session_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(info_session_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_INFO_SESSION'] = False
    michigan_jumpoff['REGISTERED_INFO_SESSION'] = False

try:
    edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    edi_webinar_in_meeting_df = edi_webinar_merged_df[edi_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(edi_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_EDI_SESSION'] = False
    michigan_jumpoff['REGISTERED_EDI_SESSION'] = False

try:
    sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    sut_webinar_in_meeting_df = sut_webinar_merged_df[sut_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(sut_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_SUT_SESSION'] = False
    michigan_jumpoff['REGISTERED_SUT_SESSION'] = False

try:
    gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    gs_webinar_in_meeting_df = gs_webinar_merged_df[gs_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gs_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_GS_SESSION'] = False
    michigan_jumpoff['REGISTERED_GS_SESSION'] = False

try:
    openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    openhours_webinar_in_meeting_df = openhours_webinar_merged_df[openhours_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(openhours_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_OH_SESSION'] = False
    michigan_jumpoff['REGISTERED_OH_SESSION'] = False

try:
    hh_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = hh_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    hh_is_webinar_in_meeting_df = hh_info_session_merged_df[hh_info_session_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_HH_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_is_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_HH_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_HH_INFO_SESSION'] = False
    michigan_jumpoff['REGISTERED_HH_INFO_SESSION'] = False

try:
    hh_edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = hh_edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    hh_edi_webinar_in_meeting_df = hh_edi_webinar_merged_df[hh_edi_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_HH_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_edi_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_HH_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_HH_EDI_SESSION'] = False
    michigan_jumpoff['REGISTERED_HH_EDI_SESSION'] = False

try:
    hh_sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = hh_sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    hh_sut_webinar_in_meeting_df = hh_sut_webinar_merged_df[hh_sut_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_HH_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_sut_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_HH_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_HH_SUT_SESSION'] = False
    michigan_jumpoff['REGISTERED_HH_SUT_SESSION'] = False

try:
    hh_gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = hh_gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    hh_gs_webinar_in_meeting_df = hh_gs_webinar_merged_df[hh_gs_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_HH_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_gs_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_HH_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_HH_GS_SESSION'] = False
    michigan_jumpoff['REGISTERED_HH_GS_SESSION'] = False

try:
    hh_oh_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = hh_oh_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    hh_oh_webinar_in_meeting_df = hh_oh_webinar_merged_df[hh_oh_webinar_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_HH_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_oh_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_HH_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(hh_oh_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_HH_OH_SESSION'] = False
    michigan_jumpoff['REGISTERED_HH_OH_SESSION'] = False

try:
    getting_ready_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = getting_ready_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    gr_webinar_in_meeting_df = getting_ready_merged_df[getting_ready_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_PCS_GR_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gr_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_PCS_GR_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gr_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_PCS_GR_SESSION'] = False
    michigan_jumpoff['REGISTERED_PCS_GR_SESSION'] = False
    
try:
    pcs_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = pcs_info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
    pcs_info_session_in_meeting_df = pcs_info_session_merged_df[pcs_info_session_merged_df['status_y'] == 'in_meeting']
    michigan_jumpoff['ATTENDED_PCS_INFO_SESSION_WEBINAR'] = michigan_jumpoff['Provider TAX ID'].isin(pcs_info_session_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    michigan_jumpoff['REGISTERED_PCS_INFO_SESSION_WEBINAR'] = michigan_jumpoff['Provider TAX ID'].isin(pcs_info_session_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
except (NameError, KeyError):
    michigan_jumpoff['ATTENDED_PCS_INFO_SESSION_WEBINAR'] = False
    michigan_jumpoff['REGISTERED_PCS_INFO_SESSION_WEBINAR'] = False
    

#Get latest LMS data
ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)
    
cs = ctx.cursor()
script = """
select * from "PC_FIVETRAN_DB"."DOCEBO"."CUSTOM_LEARNING_PLAN"
where ((learning_plan_name = 'Michigan Home Health Provider Learning Plan') or (learning_plan_name = 'Michigan Home Help Provider Learning Plan'))
"""
payload = cs.execute(script)
docebo_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

#Get PCS in person info session data
cs = ctx.cursor()
script = """
select * from PC_FIVETRAN_DB.HUBSPOT.MARKETING_ENGAGEMENTS
where lower(event_name) like '%mi roadshow%'
"""
payload = cs.execute(script)
pcs_in_person_infosession = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

pcs_in_person_infosession['TAX_ID'] = clean_tax_ids(pcs_in_person_infosession['TAX_ID'])

pcs_event_registration = pcs_in_person_infosession[pcs_in_person_infosession['MARKETING_ENGAGEMENT_TYPE'] == 'event-registration']
pcs_event_attendance = pcs_in_person_infosession[pcs_in_person_infosession['MARKETING_ENGAGEMENT_TYPE'] == 'event-attendance']

michigan_jumpoff['REGISTERED_PCS_INFO_SESSION_INPERSON'] = michigan_jumpoff['Provider TAX ID'].isin(pcs_event_registration['TAX_ID'])
michigan_jumpoff['ATTENDED_PCS_INFO_SESSION_INPERSON'] = michigan_jumpoff['Provider TAX ID'].isin(pcs_event_attendance['TAX_ID'])

docebo_df = docebo_df.dropna(subset=['AGENCY_TAX_ID'])

health_docebo_df = docebo_df[docebo_df['LEARNING_PLAN_NAME'] == 'Michigan Home Health Provider Learning Plan']

help_docebo_df = docebo_df[docebo_df['LEARNING_PLAN_NAME'] == 'Michigan Home Help Provider Learning Plan'] 

#Merge michigan_jumpoff with health_docebo_df
merged_health_df = pd.merge(michigan_jumpoff, health_docebo_df, left_on='Provider TAX ID', right_on='AGENCY_TAX_ID', how='left', suffixes=('_michigan', '_health'))

#Merge the resulting dataframe with help_docebo_df
final_merged_df = pd.merge(merged_health_df, help_docebo_df, left_on='Provider TAX ID', right_on='AGENCY_TAX_ID', how='left', suffixes=('', '_help'))

#Drop duplicate columns that may arise from the merge
final_merged_df = final_merged_df.drop_duplicates(subset=['Wave', 'Provider TAX ID'])

#Append LMS status for each provider
lms_update_df = final_merged_df[['Provider TAX ID', 'Provider Name', 'Provider NPI Number', 'Tax ID+NPI',
    'Provider Address 1', 'Provider City', 'Provider State',
    'Provider Zip Code', 'Provider Contact Name', 'Provider Email Address',
    'Provider Phone Number ', 'In HHAX', 'Wave', 'ATTENDED_INFO_SESSION',
    'REGISTERED_INFO_SESSION', 'ATTENDED_EDI_SESSION',
    'REGISTERED_EDI_SESSION', 'ATTENDED_SUT_SESSION',
    'REGISTERED_SUT_SESSION', 'ATTENDED_GS_SESSION',
    'REGISTERED_GS_SESSION', 'ATTENDED_OH_SESSION', 'REGISTERED_OH_SESSION',
    'ATTENDED_HH_INFO_SESSION','REGISTERED_HH_INFO_SESSION','ATTENDED_HH_EDI_SESSION','REGISTERED_HH_EDI_SESSION',
    'ATTENDED_HH_SUT_SESSION','REGISTERED_HH_SUT_SESSION','ATTENDED_HH_GS_SESSION','REGISTERED_HH_GS_SESSION',
    'ATTENDED_HH_OH_SESSION','REGISTERED_HH_OH_SESSION','ATTENDED_PCS_GR_SESSION','REGISTERED_PCS_GR_SESSION','REGISTERED_PCS_INFO_SESSION_INPERSON','ATTENDED_PCS_INFO_SESSION_INPERSON','ATTENDED_PCS_INFO_SESSION_WEBINAR','REGISTERED_PCS_INFO_SESSION_WEBINAR','LEARNING_PLAN_ENROLLMENT_STATUS','LEARNING_PLAN_ENROLLMENT_STATUS_help']]

lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS'] = lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS'].fillna('Not Registered')
lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS_help'] = lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS_help'].fillna('Not Registered')

final_merged_df = pd.merge(lms_update_df, cognito_form_formatted, left_on='Provider TAX ID',right_on='FederalTaxID',how='left')

final_merged_df =  final_merged_df[['Provider TAX ID', 'Provider Name', 'Provider NPI Number', 'Tax ID+NPI',
    'Provider Address 1', 'Provider City', 'Provider State',
    'Provider Zip Code', 'Provider Contact Name', 'Provider Email Address',
    'Provider Phone Number ', 'In HHAX', 'Wave', 'ATTENDED_INFO_SESSION',
    'REGISTERED_INFO_SESSION', 'ATTENDED_EDI_SESSION',
    'REGISTERED_EDI_SESSION', 'ATTENDED_SUT_SESSION',
    'REGISTERED_SUT_SESSION', 'ATTENDED_GS_SESSION',
    'REGISTERED_GS_SESSION', 'ATTENDED_OH_SESSION', 'REGISTERED_OH_SESSION',
    'ATTENDED_HH_INFO_SESSION','REGISTERED_HH_INFO_SESSION','ATTENDED_HH_EDI_SESSION','REGISTERED_HH_EDI_SESSION',
    'ATTENDED_HH_SUT_SESSION','REGISTERED_HH_SUT_SESSION','ATTENDED_HH_GS_SESSION','REGISTERED_HH_GS_SESSION',
    'ATTENDED_HH_OH_SESSION','REGISTERED_HH_OH_SESSION','ATTENDED_PCS_GR_SESSION','REGISTERED_PCS_GR_SESSION','LEARNING_PLAN_ENROLLMENT_STATUS','LEARNING_PLAN_ENROLLMENT_STATUS_help',
    'ATTENDED_PCS_INFO_SESSION_INPERSON','REGISTERED_PCS_INFO_SESSION_INPERSON','ATTENDED_PCS_INFO_SESSION_WEBINAR','REGISTERED_PCS_INFO_SESSION_WEBINAR',
    'DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService']]

import_list = final_merged_df.rename(columns={'Provider TAX ID' : 'PROVIDER_TAX_ID', 'Provider Name' : 'PROVIDER_NAME', 'Provider NPI Number' : 'PROVIDER_NPI_NUMBER', 'Tax ID+NPI' : 'TAX_ID_NPI',
    'Provider Address 1':'PROVIDER_ADDRESS_1', 'Provider City' : 'PROVIDER_CITY', 'Provider State' : 'PROVIDER_STATE',
    'Provider Zip Code' : 'PROVIDER_ZIP_CODE', 'Provider Contact Name' : 'PROVIDER_CONTACT_NAME', 'Provider Email Address' : 'PROVIDER_EMAIL_ADDRESS',
    'Provider Phone Number ' : 'PROVIDER_PHONE_NUMBER', 'In HHAX' : 'IN_HHAX', 'Wave' : 'WAVE','DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService' : 'EVV_SYSTEM_CHOICE',
    'LEARNING_PLAN_ENROLLMENT_STATUS_help':'LEARNING_PLAN_ENROLLMENT_STATUS_HH'})

import_list['EVV_SYSTEM_CHOICE'] = import_list['EVV_SYSTEM_CHOICE'].fillna('Missing Cognito Form')

#Join Payer list to output
michigan_payers.dropna(subset=['Provider TAX ID'], inplace=True)

payer_df = michigan_payers.groupby('Provider TAX ID').agg({
    'Payer': lambda x: ', '.join(sorted(set(x))),
    'Wave': lambda x: ', '.join(sorted(set(x)))
}).reset_index()

payer_df['Provider TAX ID'] = clean_tax_ids(payer_df['Provider TAX ID'])

temp_import_list = pd.merge(import_list, payer_df, left_on=['PROVIDER_TAX_ID','WAVE'],right_on=['Provider TAX ID', 'Wave'], how='left')

import_list = temp_import_list.applymap(str)

#Get the current date and time in UTC
utc_now = datetime.now(pytz.utc)

#Convert to Eastern Time
eastern = pytz.timezone('US/Eastern')
now = utc_now.astimezone(eastern)

current_day = f"{now.day:02d}"
current_year = now.year
current_month = f"{now.month:02d}"

date = f"{current_month}/{current_day}/{current_year}"

time_series_dataframe = pd.DataFrame({'EVENT_DATE': [date]})

import_list['EVENT_DATE'] = pd.DataFrame({'EVENT_DATE': [date]})

ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)
    
cs = ctx.cursor()

#Query snowflake to determine portal creations
script = """
select
"Federal Tax Number" as TAX_ID,
"Platform Type" as PLATFORM_TAG,
"Application Provider Id" as PROVIDER_ID
from "ANALYTICS"."BI"."DIMPROVIDER"
where "Is Demo" = 'FALSE' and lower("Environemnt") like '%clo%'
"""

payload = cs.execute(script)
portals = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

import_list['PORTAL_CREATED'] = import_list['PROVIDER_TAX_ID'].isin(portals['TAX_ID']).astype(str)

import_list = import_list.merge(portals[['TAX_ID', 'PLATFORM_TAG',"PROVIDER_ID"]], left_on='PROVIDER_TAX_ID', right_on='TAX_ID', how='left')

import_list['PORTAL_TYPE'] = import_list['PLATFORM_TAG']

import_list.drop(columns=['TAX_ID','PLATFORM_TAG','Provider TAX ID','Wave'], inplace=True)

import_list = import_list.rename(columns={'Payer':'PAYERS'})

time_series_dataframe['PROVIDER_COUNT'] = import_list['PROVIDER_TAX_ID'].nunique()
hh_provider_count = import_list[import_list['WAVE'] == 'Home Help']['PROVIDER_TAX_ID'].nunique()
health_provider_count = import_list[import_list['WAVE'] == 'Home Health']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['HH_PROVIDER_COUNT'] = hh_provider_count
time_series_dataframe['HEALTH_PROVIDER_COUNT'] = health_provider_count
time_series_dataframe['COMPLETED_ONBOARDING_FORM'] = import_list[import_list['EVV_SYSTEM_CHOICE'] != 'Missing Cognito Form']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_INFO_SESSION'] = import_list[import_list['REGISTERED_INFO_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_INFO_SESSION'] = import_list[import_list['ATTENDED_INFO_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_EDI_SESSION'] = import_list[import_list['REGISTERED_EDI_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_EDI_SESSION'] = import_list[import_list['ATTENDED_EDI_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_SUT_SESSION'] = import_list[import_list['REGISTERED_SUT_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_SUT_SESSION'] = import_list[import_list['ATTENDED_SUT_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_GS_SESSION'] = import_list[import_list['REGISTERED_GS_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_GS_SESSION'] = import_list[import_list['ATTENDED_GS_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_OH_SESSION'] = import_list[import_list['REGISTERED_OH_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_OH_SESSION'] = import_list[import_list['ATTENDED_OH_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_PCS_INFO_SESSION_INPERSON'] = import_list[import_list['ATTENDED_PCS_INFO_SESSION_INPERSON'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_PCS_INFO_SESSION_INPERSON'] = import_list[import_list['REGISTERED_PCS_INFO_SESSION_INPERSON'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_PCS_INFO_SESSION_WEBINAR'] = import_list[import_list['ATTENDED_PCS_INFO_SESSION_WEBINAR'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_PCS_INFO_SESSION_WEBINAR'] = import_list[import_list['REGISTERED_PCS_INFO_SESSION_WEBINAR'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_HH_INFO_SESSION'] = import_list[import_list['REGISTERED_HH_INFO_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_HH_INFO_SESSION'] = import_list[import_list['ATTENDED_HH_INFO_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_HH_EDI_SESSION'] = import_list[import_list['REGISTERED_HH_EDI_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_HH_EDI_SESSION'] = import_list[import_list['ATTENDED_HH_EDI_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_HH_SUT_SESSION'] = import_list[import_list['REGISTERED_HH_SUT_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_HH_SUT_SESSION'] = import_list[import_list['ATTENDED_HH_SUT_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_HH_GS_SESSION'] = import_list[import_list['REGISTERED_HH_GS_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_HH_GS_SESSION'] = import_list[import_list['ATTENDED_HH_GS_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_HH_OH_SESSION'] = import_list[import_list['REGISTERED_HH_OH_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_HH_OH_SESSION'] = import_list[import_list['ATTENDED_HH_OH_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['ATTENDED_PCS_GR_SESSION'] = import_list[import_list['ATTENDED_PCS_GR_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['REGISTERED_PCS_GR_SESSION'] = import_list[import_list['REGISTERED_PCS_GR_SESSION'] != 'False']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['YES_INTEGRATE_EDI'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'Yes - I currently have my own EVV system and would like to integrate with HHAX (EDI)']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['YES_USE_HHAX'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'Yes - I currently have my own EVV system but would like to use HHAX (Free EVV)']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['NO_EVV_SYSTEM'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'No - I currently do not have my own EVV system and would like to use HHAX (Free EVV)']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_NOTREGISTERED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS'] == 'Not Registered']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_ENROLLED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS'] == 'Enrolled']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_INPROGRESS'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS'] == 'In Progress']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_COMPLETED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS'] == 'Completed']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_HH_NOTREGISTERED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS_HH'] == 'Not Registered']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_HH_ENROLLED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS_HH'] == 'Enrolled']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_HH_INPROGRESS'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS_HH'] == 'In Progress']['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['LMS_HH_COMPLETED'] = import_list[import_list['LEARNING_PLAN_ENROLLMENT_STATUS_HH'] == 'Completed']['PROVIDER_TAX_ID'].nunique()
hh_portals_created = import_list[(import_list['WAVE'] == 'Home Help') & (import_list['PORTAL_CREATED'] == 'True')]['PROVIDER_TAX_ID'].nunique()
health_portals_created = import_list[(import_list['WAVE'] == 'Home Health') & (import_list['PORTAL_CREATED'] == 'True')]['PROVIDER_TAX_ID'].nunique()
pcs_portals_created = import_list[(import_list['WAVE'] == 'PCS') & (import_list['PORTAL_CREATED'] == 'True')]['PROVIDER_TAX_ID'].nunique()
time_series_dataframe['HH_PORTALS_CREATED'] = hh_portals_created
time_series_dataframe['HEALTH_PORTALS_CREATED'] = health_portals_created
time_series_dataframe['PCS_PORTALS_CREATED'] = pcs_portals_created
time_series_dataframe['PORTALS_CREATED'] = import_list[import_list['PORTAL_CREATED'] == 'True']['PROVIDER_TAX_ID'].nunique()

#Load trend data into Snowflake
time_series_dataframe['EVENT_DATE'] = pd.to_datetime(time_series_dataframe['EVENT_DATE'])
for col in time_series_dataframe.columns:
    if col != 'EVENT_DATE':
        time_series_dataframe[col] = pd.to_numeric(time_series_dataframe[col], errors='coerce').astype('Int64')

# Construct the SQLAlchemy connection string
connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/CAMPAIGN_REPORTING?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

#Instantiate SQLAlchemy engine with the private key
engine = create_engine(
    connection_string,
    connect_args={
        "private_key": private_key_bytes
    }
)

chunk_size = 10000
chunks = [x for x in range(0, len(time_series_dataframe), chunk_size)] + [len(time_series_dataframe)]
table_name = 'michigantrend' 

for i in range(len(chunks) - 1):
    time_series_dataframe[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

import_list.drop(['EVENT_DATE'],axis=1,inplace=True)

import_list['EVENT_DATE'] = date

import_list = import_list.drop_duplicates(subset=['WAVE', 'PROVIDER_TAX_ID'])

#Load row by row data into Snowflake
chunk_size = 1000
chunks = [x for x in range(0, len(import_list), chunk_size)] + [len(import_list)]
table_name = 'michigan' 

for i in range(len(chunks) - 1):
    import_list[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': secret,
    'scope':  'https://graph.microsoft.com/.default'
}

#Begin process to load file to internal Sharepoint folder
response = requests.post(url, data=data)
response_json = response.json()

access_token = response_json.get('access_token')
hostname = 'hhaexchange.sharepoint.com'
site_relative_path = 'sites/AllEmployees'

url = f"https://graph.microsoft.com/v1.0/sites/hhaexchange.sharepoint.com:/sites/AllEmployees"
headers = {
    "Authorization": f"Bearer {access_token}"
}
response = requests.get(url, headers=headers)
site_data = response.json()
site_id = site_data.get("id")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)

drive_id = None

#Check if the request was successful
if response.status_code == 200:
    drives = response.json().get('value', [])
    for drive in drives:
        #Check if drive name is "Documents" and store its ID
        if drive['name'] == 'Documents':
            drive_id = drive['id']
            break  #Exit the loop as we found the drive ID

current_date = now.strftime("%Y-%m-%d")
file_name = f'Michigan Campaign Report - {current_date}.xlsx'

destination_path = f'Campaign Reports/Michigan/{file_name}'

#Full endpoint to the folder
upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{destination_path}:/content"

#Create an Excel file in memory
output = io.BytesIO()

with pd.ExcelWriter(output, engine='openpyxl') as writer:
    import_list.to_excel(writer, index=False, sheet_name='Michigan')

#Move the cursor to the beginning of the stream
output.seek(0)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

response = requests.put(upload_url, headers=headers, data=output)