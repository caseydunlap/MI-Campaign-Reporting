#Import packages
import pandas as pd
import numpy as np
import pytz
import json
import time
import os
import logging
from datetime import datetime,timedelta,timezone
import requests
import re
import snowflake.connector
from sqlalchemy import create_engine
from decimal import Decimal
from requests.auth import HTTPBasicAuth
import urllib
from urllib.parse import quote
from io import BytesIO
import base64

#Config logging
script_dir = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(
    filename=os.path.join(script_dir,'logs.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

try:
    #Load Secrets
    graph_secret = os.getenv('graph_secret')
    graph_client_id = os.getenv('graph_client')
    graph_tenant_id = os.getenv('graph_tenant')
    sharepoint_url_base = os.getenv('sharepoint_url_base')
    sharepoint_url_end = os.getenv('sharepoint_url_end')
    snowflake_user = os.getenv('snowflake_user')
    snowflake_pass = os.getenv('snowflake_password')
    snowflake_wh = os.getenv('snowflake_fivetran_wh')
    snowflake_role = os.getenv('snowflake_role')
    snowflake_schema = os.getenv('snowflake_schema')
    snowflake_account = os.getenv('snowflake_account')
    snowflake_fivetran_db = os.getenv('snowflake_fivetran_db')
    zoom_client_id = os.getenv('zoom_client')
    zoom_secret_id = os.getenv('zoom_secret')
    zoom_account_id = os.getenv('zoom_account')
    zoom_user_id_raw = os.getenv('zoom_user_ids', '')
    zoom_user_ids = [item for item in zoom_user_id_raw.split(',') if item.strip()]

    #Use the Microsfot Graph API to get the Cognito Form and Provider Jumpoff lists
    secret = graph_secret
    client_id = graph_client_id
    tenant_id = graph_tenant_id

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

    # Body
    body = {
        "grant_type": "account_credentials",
        "account_id": zoom_account_id
    }

    # Token API endpoint
    token_url = "https://zoom.us/oauth/token"

    # Make the request
    response = requests.post(token_url, headers=headers, data=body)
    access_token = response.json().get('access_token')

    #All webinars will come from one of these two users (hhaexchangewebinar,providerexperience)
    user_ids = zoom_user_ids

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    def clean_tax_ids(column):
    # Use a regular expression to remove spaces and dashes for each value in the series
        return column.apply(lambda x: re.sub(r'[-\s]', '', x))

            # Function to preprocess registrants and extract custom questions
    def preprocess_registrants(registrants):
        for registrant in registrants:
            # Flatten custom questions
            for question in registrant.get('custom_questions', []):
                # Use the question title as the column name and its value as the value
                column_name = question['title']  # Truncate column name if too long
                registrant[column_name] = question['value']
            # Remove the original custom_questions list to avoid redundancy
            registrant.pop('custom_questions', None)
        return registrants

        # Function to construct URL for effective pagination
    def construct_url(instance_id, next_page_token=None):
        url = f"https://api.zoom.us/v2/past_webinars/{instance_id}/participants"
        if next_page_token:
            url += f"?next_page_token={next_page_token}"
        return url

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
                # Append the next_page_token to the URL if it's not None
                if next_page_token:
                    webinars_url = f"{base_url}?next_page_token={next_page_token}"
                
                response = requests.get(webinars_url, headers=headers)
                data = response.json()
                info_session_all_webinars.extend(data['webinars'])
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    # If there's no next_page_token, exit the loop
                    break

        # Create DataFrame from the collected webinars data
        df_webinars = pd.DataFrame(info_session_all_webinars)
        filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Informational Session Webinar']
        webinar_id_isolated = filtered_df['id']
        webinar_ids = webinar_id_isolated.to_list()

        for webinar_id in webinar_ids:
            webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
            response = requests.get(webinar_url,headers=headers)
            instances_data = response.json()
            info_session_all_instances.extend(instances_data.get('webinars', []))

        info_session_occurrence_ids = [occurrence['uuid'] for occurrence in info_session_all_instances]

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

        webinars_df_participants = pd.DataFrame(info_session_all_webinar_details)

        for webinar_id in webinar_ids:
            # Fetch webinar details to get occurrence IDs
            webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
            response = requests.get(webinar_url, headers=headers)
            webinar_data = response.json()
            occurrences = webinar_data.get('occurrences', [])
            
            occurrence_ids = [occurrence['occurrence_id'] for occurrence in occurrences]

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

        # Convert to DataFrame
        webinars_df_registrants = pd.DataFrame(info_session_all_webinar_details_reg)

        info_session_merged_df = pd.merge(webinars_df_registrants,webinars_df_participants, left_on='id', right_on='registrant_id',how='left')

        info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except Exception as e:
        pass

    #All webinars will come from one of these two users (hhaexchangewebinar,providerexperience)

    try:
        edi_all_webinars = []
        edi_all_instances = []
        edi_all_webinar_details = []
        edi_all_webinar_details_reg = []

        for user_id in user_ids:
            base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
            webinars_url = base_url
            next_page_token = None

            while True:
                # Append the next_page_token to the URL if it's not None
                if next_page_token:
                    webinars_url = f"{base_url}?next_page_token={next_page_token}"
                
                response = requests.get(webinars_url, headers=headers)
                data = response.json()
                edi_all_webinars.extend(data['webinars'])
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    # If there's no next_page_token, exit the loop
                    break

        # Create DataFrame from the collected webinars data
        df_webinars = pd.DataFrame(edi_all_webinars)
        filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Department of Health and Human Services: EDI Provider Onboarding Webinar']
        webinar_id_isolated = filtered_df['id']
        webinar_ids = webinar_id_isolated.to_list()


        for webinar_id in webinar_ids:
            webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
            response = requests.get(webinar_url,headers=headers)
            instances_data = response.json()
            edi_all_instances.extend(instances_data.get('webinars', []))

        edi_occurrence_ids = [occurrence['uuid'] for occurrence in edi_all_instances]

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

        edi_webinars_df_participants = pd.DataFrame(edi_all_webinar_details)

        for webinar_id in webinar_ids:
            # Fetch webinar details to get occurrence IDs
            webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
            response = requests.get(webinar_url, headers=headers)
            webinar_data = response.json()
            occurrences = webinar_data.get('occurrences', [])
            
            occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

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

        # Convert to DataFrame
        edi_webinars_df_registrants = pd.DataFrame(edi_all_webinar_details_reg)

        edi_webinar_merged_df = pd.merge(edi_webinars_df_registrants,edi_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')

        edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except Exception as e:
        pass

    #All webinars will come from one of these two users (hhaexchangewebinar,providerexperience)
    try:
        sut_all_webinars = []
        sut_all_instances = []
        sut_all_webinar_details = []
        sut_all_webinar_details_reg = []

        for user_id in user_ids:
            base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
            webinars_url = base_url
            next_page_token = None

            while True:
                # Append the next_page_token to the URL if it's not None
                if next_page_token:
                    webinars_url = f"{base_url}?next_page_token={next_page_token}"
                
                response = requests.get(webinars_url, headers=headers)
                data = response.json()
                sut_all_webinars.extend(data['webinars'])
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    # If there's no next_page_token, exit the loop
                    break

        # Create DataFrame from the collected webinars data
        df_webinars = pd.DataFrame(sut_all_webinars)
        filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Department of Health and Human Services System User Training']
        webinar_id_isolated = filtered_df['id']
        webinar_ids = webinar_id_isolated.to_list()


        for webinar_id in webinar_ids:
            webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
            response = requests.get(webinar_url,headers=headers)
            instances_data = response.json()
            sut_all_instances.extend(instances_data.get('webinars', []))

        sut_occurrence_ids = [occurrence['uuid'] for occurrence in sut_all_instances]

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

        sut_df_participants = pd.DataFrame(sut_all_webinar_details)

        for webinar_id in webinar_ids:
            # Fetch webinar details to get occurrence IDs
            webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
            response = requests.get(webinar_url, headers=headers)
            webinar_data = response.json()
            occurrences = webinar_data.get('occurrences', [])
            
            occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

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

        # Convert to DataFrame
        sut_webinars_df_registrants = pd.DataFrame(sut_all_webinar_details_reg)

        sut_webinar_merged_df = pd.merge(sut_webinars_df_registrants,sut_df_participants, left_on='id', right_on='registrant_id',how='left')

        sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except Exception as e:
        pass

    try:
        #All webinars will come from one of these two users (hhaexchangewebinar,providerexperience)
        gs_all_webinars = []
        gs_all_instances = []
        gs_all_webinar_details = []
        gs_all_webinar_details_reg = []

        for user_id in user_ids:
            base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
            webinars_url = base_url
            next_page_token = None

            while True:
                # Append the next_page_token to the URL if it's not None
                if next_page_token:
                    webinars_url = f"{base_url}?next_page_token={next_page_token}"
                
                response = requests.get(webinars_url, headers=headers)
                data = response.json()
                gs_all_webinars.extend(data['webinars'])
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    # If there's no next_page_token, exit the loop
                    break

        # Create DataFrame from the collected webinars data
        df_webinars = pd.DataFrame(gs_all_webinars)
        filtered_df = df_webinars[df_webinars['topic'] == 'Michigan Health and Human Services Getting Started Webinar']
        webinar_id_isolated = filtered_df['id']
        webinar_ids = webinar_id_isolated.to_list()


        for webinar_id in webinar_ids:
            webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
            response = requests.get(webinar_url,headers=headers)
            instances_data = response.json()
            gs_all_instances.extend(instances_data.get('webinars', []))

        gs_occurrence_ids = [occurrence['uuid'] for occurrence in gs_all_instances]

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

        gs_webinars_df_participants = pd.DataFrame(gs_all_webinar_details)

        for webinar_id in webinar_ids:
            # Fetch webinar details to get occurrence IDs
            webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
            response = requests.get(webinar_url, headers=headers)
            webinar_data = response.json()
            occurrences = webinar_data.get('occurrences', [])
            
            occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

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

        # Convert to DataFrame
        gs_webinars_df_registrants = pd.DataFrame(gs_all_webinar_details_reg)

        gs_webinar_merged_df = pd.merge(gs_webinars_df_registrants,gs_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')

        gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except Exception as e:
        pass

    try:
        openhours_all_webinars = []
        openhours_all_instances = []
        openhours_all_webinar_details = []
        openhours_all_webinar_details_reg = []

        for user_id in user_ids:
            base_url = f"https://api.zoom.us/v2/users/{user_id}/webinars"
            webinars_url = base_url
            next_page_token = None

            while True:
                # Append the next_page_token to the URL if it's not None
                if next_page_token:
                    webinars_url = f"{base_url}?next_page_token={next_page_token}"
                
                response = requests.get(webinars_url, headers=headers)
                data = response.json()
                openhours_all_webinars.extend(data['webinars'])
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    # If there's no next_page_token, exit the loop
                    break

        # Create DataFrame from the collected webinars data
        df_webinars = pd.DataFrame(openhours_all_webinars)
        filtered_df = df_webinars[df_webinars['topic'] == 'MDHHS - HHAX Open Hours - Onboarding and Adoption Training']
        webinar_id_isolated = filtered_df['id']
        webinar_ids = webinar_id_isolated.to_list()


        for webinar_id in webinar_ids:
            webinar_url = f"https://api.zoom.us/v2/past_webinars/{webinar_id}/instances"
            response = requests.get(webinar_url,headers=headers)
            instances_data = response.json()
            openhours_all_instances.extend(instances_data.get('webinars', []))

        openhours_occurrence_ids = [occurrence['uuid'] for occurrence in openhours_all_instances]

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

        openhours_webinars_df_participants = pd.DataFrame(openhours_all_webinar_details)

        for webinar_id in webinar_ids:
            # Fetch webinar details to get occurrence IDs
            webinar_url = f'https://api.zoom.us/v2/webinars/{webinar_id}?show_previous_occurrences=true'
            response = requests.get(webinar_url, headers=headers)
            webinar_data = response.json()
            occurrences = webinar_data.get('occurrences', [])
            
            occurrence_ids_2 = [occurrence['occurrence_id'] for occurrence in occurrences]

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

        # Convert to DataFrame
        openhours_webinars_df_registrants = pd.DataFrame(openhours_all_webinar_details_reg)

        openhours_webinar_merged_df = pd.merge(openhours_webinars_df_registrants,openhours_webinars_df_participants, left_on='id', right_on='registrant_id',how='left')

        openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = clean_tax_ids(openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except Exception as e:
        pass

    try:
        info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
        info_session_in_meeting_df = info_session_merged_df[info_session_merged_df['status_y'] == 'in_meeting']
        michigan_jumpoff['ATTENDED_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(info_session_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
        michigan_jumpoff['REGISTERED_INFO_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(info_session_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except NameError:
        michigan_jumpoff['ATTENDED_INFO_SESSION'] = False
        michigan_jumpoff['REGISTERED_INFO_SESSION'] = False

    try:
        edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = edi_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
        edi_webinar_in_meeting_df = edi_webinar_merged_df[edi_webinar_merged_df['status_y'] == 'in_meeting']
        michigan_jumpoff['ATTENDED_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(edi_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
        michigan_jumpoff['REGISTERED_EDI_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(edi_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except NameError:
        michigan_jumpoff['ATTENDED_EDI_SESSION'] = False
        michigan_jumpoff['REGISTERED_EDI_SESSION'] = False

    try:
        sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = sut_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
        sut_webinar_in_meeting_df = sut_webinar_merged_df[sut_webinar_merged_df['status_y'] == 'in_meeting']
        michigan_jumpoff['ATTENDED_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(sut_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
        michigan_jumpoff['REGISTERED_SUT_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(sut_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except NameError:
        michigan_jumpoff['ATTENDED_SUT_SESSION'] = False
        michigan_jumpoff['REGISTERED_SUT_SESSION'] = False

    try:
        gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = gs_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
        gs_webinar_in_meeting_df = gs_webinar_merged_df[gs_webinar_merged_df['status_y'] == 'in_meeting']
        michigan_jumpoff['ATTENDED_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gs_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
        michigan_jumpoff['REGISTERED_GS_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(gs_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])

    except NameError:
        michigan_jumpoff['ATTENDED_GS_SESSION'] = False
        michigan_jumpoff['REGISTERED_GS_SESSION'] = False
    try:
        openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'] = openhours_webinar_merged_df['Please enter your Tax ID number (without dashes) for attendance purposes.'].astype(str).str.strip()
        openhours_webinar_in_meeting_df = openhours_webinar_merged_df[openhours_webinar_merged_df['status_y'] == 'in_meeting']
        michigan_jumpoff['ATTENDED_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(openhours_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
        michigan_jumpoff['REGISTERED_OH_SESSION'] = michigan_jumpoff['Provider TAX ID'].isin(openhours_webinar_in_meeting_df['Please enter your Tax ID number (without dashes) for attendance purposes.'])
    except NameError:
        michigan_jumpoff['ATTENDED_OH_SESSION'] = False
        michigan_jumpoff['REGISTERED_OH_SESSION'] = False

    ctx = snowflake.connector.connect(
        user = snowflake_user,
        role = snowflake_role,
        warehouse = snowflake_wh,
        password = snowflake_pass,
        schema = snowflake_schema,
        account= snowflake_account)
        
    cs = ctx.cursor()
    script = """
    select * from "PC_FIVETRAN_DB"."DOCEBO"."CUSTOM_LEARNING_PLAN"
    where learning_plan_name = 'Michigan Home Health Provider Learning Plan'
    """
    payload = cs.execute(script)
    docebo_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    docebo_df = docebo_df.dropna(subset=['AGENCY_TAX_ID'])

    merged_df = pd.merge(michigan_jumpoff, docebo_df, left_on='Provider TAX ID',right_on='AGENCY_TAX_ID',how='left')

    lms_update_df = merged_df[['Provider TAX ID', 'Provider Name', 'Provider NPI Number', 'Tax ID+NPI',
        'Provider Address 1', 'Provider City', 'Provider State',
        'Provider Zip Code', 'Provider Contact Name', 'Provider Email Address',
        'Provider Phone Number ', 'In HHAX', 'Wave', 'ATTENDED_INFO_SESSION',
        'REGISTERED_INFO_SESSION', 'ATTENDED_EDI_SESSION',
        'REGISTERED_EDI_SESSION', 'ATTENDED_SUT_SESSION',
        'REGISTERED_SUT_SESSION', 'ATTENDED_GS_SESSION',
        'REGISTERED_GS_SESSION', 'ATTENDED_OH_SESSION', 'REGISTERED_OH_SESSION','LEARNING_PLAN_ENROLLMENT_STATUS']]

    lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS'] = lms_update_df['LEARNING_PLAN_ENROLLMENT_STATUS'].fillna('Not Registered')

    final_merged_df = pd.merge(lms_update_df, cognito_form_formatted, left_on='Provider TAX ID',right_on='FederalTaxID',how='left')

    final_merged_df =  final_merged_df[['Provider TAX ID', 'Provider Name', 'Provider NPI Number', 'Tax ID+NPI',
        'Provider Address 1', 'Provider City', 'Provider State',
        'Provider Zip Code', 'Provider Contact Name', 'Provider Email Address',
        'Provider Phone Number ', 'In HHAX', 'Wave', 'ATTENDED_INFO_SESSION',
        'REGISTERED_INFO_SESSION', 'ATTENDED_EDI_SESSION',
        'REGISTERED_EDI_SESSION', 'ATTENDED_SUT_SESSION',
        'REGISTERED_SUT_SESSION', 'ATTENDED_GS_SESSION',
        'REGISTERED_GS_SESSION', 'ATTENDED_OH_SESSION', 'REGISTERED_OH_SESSION',
        'LEARNING_PLAN_ENROLLMENT_STATUS',
        'DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService']]

    import_list = final_merged_df.rename(columns={'Provider TAX ID' : 'PROVIDER_TAX_ID', 'Provider Name' : 'PROVIDER_NAME', 'Provider NPI Number' : 'PROVIDER_NPI_NUMBER', 'Tax ID+NPI' : 'TAX_ID_NPI',
        'Provider Address 1':'PROVIDER_ADDRESS_1', 'Provider City' : 'PROVIDER_CITY', 'Provider State' : 'PROVIDER_STATE',
        'Provider Zip Code' : 'PROVIDER_ZIP_CODE', 'Provider Contact Name' : 'PROVIDER_CONTACT_NAME', 'Provider Email Address' : 'PROVIDER_EMAIL_ADDRESS',
        'Provider Phone Number ' : 'PROVIDER_PHONE_NUMBER', 'In HHAX' : 'IN_HHAX', 'Wave' : 'WAVE','DoesYourAgencyCurrentlyUseAnEVVSystemToCaptureTheStartTimeEndTimeAndLocationOfTheMembersService' : 'EVV_SYSTEM_CHOICE'})

    import_list['EVV_SYSTEM_CHOICE'] = import_list['EVV_SYSTEM_CHOICE'].fillna('Missing Cognito Form')

    import_list = import_list.applymap(str)

    now = datetime.now()
    current_day = f"{now.day:02d}"
    current_year = now.year
    current_month = f"{now.month:02d}"

    date = f"{current_month}/{current_day}/{current_year}"

    time_series_dataframe = pd.DataFrame({'EVENT_DATE': [date]})

    time_series_dataframe['PROVIDER_COUNT'] = import_list['PROVIDER_TAX_ID'].nunique()
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
    time_series_dataframe['YES_INTEGRATE_EDI'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'Yes - I currently have my own EVV system and would like to integrate with HHAX (EDI)']['PROVIDER_TAX_ID'].nunique()
    time_series_dataframe['YES_USE_HHAX'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'Yes - I currently have my own EVV system but would like to use HHAX (Free EVV)']['PROVIDER_TAX_ID'].nunique()
    time_series_dataframe['NO_EVV_SYSTEM'] = import_list[import_list['EVV_SYSTEM_CHOICE'] == 'No - I currently do not have my own EVV system and would like to use HHAX (Free EVV)']['PROVIDER_TAX_ID'].nunique()

    time_series_dataframe['EVENT_DATE'] = pd.to_datetime(time_series_dataframe['EVENT_DATE'])
    for col in time_series_dataframe.columns:
        if col != 'EVENT_DATE':
            time_series_dataframe[col] = pd.to_numeric(time_series_dataframe[col], errors='coerce').astype('Int64')

    engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_pass}@{snowflake_account}/{snowflake_fivetran_db}/CAMPAIGN_REPORTING?warehouse={snowflake_wh}&role={snowflake_role}')

    chunk_size = 10000
    chunks = [x for x in range(0, len(time_series_dataframe), chunk_size)] + [len(time_series_dataframe)]
    table_name = 'michigantrend' 

    for i in range(len(chunks) - 1):
        time_series_dataframe[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

    cs = ctx.cursor()
    delete = """ delete from "PC_FIVETRAN_DB"."CAMPAIGN_REPORTING"."MICHIGAN"
    """
    payload = cs.execute(delete)

    chunk_size = 1000  # define chunk size
    chunks = [x for x in range(0, len(import_list), chunk_size)] + [len(import_list)]
    table_name = 'michigan' 

    for i in range(len(chunks) - 1):
        import_list[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)
        
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Success')

except Exception as e:
    logging.exception('Operation failed due to an error')
logging.getLogger().setLevel(logging.ERROR)
