import time
import datetime
import requests
import numpy as np
import pandas as pd
import logging
import json
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from config import APPS_CONFIG, DB_CREDENTIALS, UPDATE_INTERVAL, TIME_DELAY, OFFSET_BT_SCRIPTS, LTV_SAAS_GOOGLE_ADS_ID, GOOGLE_ADS_CONFIG
from config import TFX_META_APP_ID, TFX_META_APP_SECRET, TFX_META_LONG_LIVED_TOKEN, TFX_META_AD_ACCOUNT_ID, TFX_META_CUSTOM_AUDIENCE_ID
from config import GA4_OAUTH
import yaml
import hashlib
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from apscheduler.schedulers.blocking import BlockingScheduler
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression, Filter
from google.oauth2.credentials import Credentials
from urllib.parse import urlparse, parse_qs
MAX_OPERATIONS_PER_REQUEST = 10

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', handlers=[logging.StreamHandler()])

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CREDENTIALS)
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {str(e)}")
        return None

############ REMOVE FROM META ADS LISTS ############
def meta_test_credentials():
    url = f"https://graph.facebook.com/v12.0/act_{TFX_META_AD_ACCOUNT_ID}"
    params = {
        'access_token': TFX_META_LONG_LIVED_TOKEN,
        'fields': 'name,account_status'
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        logging.info(f"Ad Account Name: {data['name']}")
        logging.info(f"Account Status: {data['account_status']}")
        logging.info("Your tokens and credentials are working correctly.")
        return True
    else:
        logging.info("Failed to fetch ad account information.")
        logging.info(f"Status Code: {response.status_code}")
        logging.info(f"Error: {response.json()}")
        return False

# Function to add users to the custom audience in batches of 10
def meta_add_users_to_custom_audience(user_emails):
    url = f"https://graph.facebook.com/v12.0/{TFX_META_CUSTOM_AUDIENCE_ID}/users"

    for i in range(0, len(user_emails), MAX_OPERATIONS_PER_REQUEST):
        batch_emails = user_emails[i:i + MAX_OPERATIONS_PER_REQUEST]
        hashed_emails = [hashlib.sha256(email.encode('utf-8')).hexdigest() for email in batch_emails]
        
        payload = {
            'payload': json.dumps({
                'schema': 'EMAIL_SHA256',
                'data': hashed_emails
            }),
            'access_token': TFX_META_LONG_LIVED_TOKEN
        }
        response = requests.post(url, data=payload)
        logging.info(f"Batch {i // MAX_OPERATIONS_PER_REQUEST + 1} Add Response: {response.json()}")
        if response.json().get('num_received') >= 1 and response.json().get('num_invalid_entries') == 0:
            logging.info(f"The email batch was successfully added to the TFX Active Users List")
        else:
            logging.info("The email insertion was not successful.")

def meta_remove_users_from_custom_audience(user_emails):
    url = f"https://graph.facebook.com/v12.0/{TFX_META_CUSTOM_AUDIENCE_ID}/users"

    for i in range(0, len(user_emails), MAX_OPERATIONS_PER_REQUEST):
        batch_emails = user_emails[i:i + MAX_OPERATIONS_PER_REQUEST]
        hashed_emails = [hashlib.sha256(email.encode('utf-8')).hexdigest() for email in batch_emails]
        
        payload = {
            'payload': json.dumps({
                'schema': 'EMAIL_SHA256',
                'data': hashed_emails
            }),
            'access_token': TFX_META_LONG_LIVED_TOKEN
        }
        response = requests.delete(url, data=payload)
        logging.info(f"Batch {i // MAX_OPERATIONS_PER_REQUEST + 1} Remove Response: {response.json()}")
        if response.json().get('num_received') >= 1 and response.json().get('num_invalid_entries') == 0:
            logging.info(f"The email batch was successfully removed from the TFX Active Users List")
        else:
            logging.info("The email insertion was not successful.")

############ REMOVE FROM GOOGLE ADS LISTS ############
def remove_emails_from_customer_list(client, customer_id, user_list_id, email_addresses):
    user_data_service = client.get_service("UserDataService")
    all_success = True
    failed_emails = []

    # Split email addresses into batches
    for i in range(0, len(email_addresses), MAX_OPERATIONS_PER_REQUEST):
        batch_emails = email_addresses[i:i + MAX_OPERATIONS_PER_REQUEST]
        user_data_operations = []

        for email_address in batch_emails:
            # Hash the email address using SHA-256
            hashed_email = hashlib.sha256(email_address.encode('utf-8')).hexdigest()
            #logging.info(f'Hashed email: {hashed_email}')

            # Create a user identifier with the hashed email
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_email = hashed_email

            # Create the user data
            user_data = client.get_type("UserData")
            user_data.user_identifiers.append(user_identifier)

            # Create the operation to remove the user from the user list
            user_data_operation = client.get_type("UserDataOperation")
            user_data_operation.remove = user_data

            user_data_operations.append(user_data_operation)

        # Create the metadata for the user list
        customer_match_user_list_metadata = client.get_type("CustomerMatchUserListMetadata")
        customer_match_user_list_metadata.user_list = f'customers/{customer_id}/userLists/{user_list_id}'

        # Create the request
        request = client.get_type("UploadUserDataRequest")
        request.customer_id = customer_id
        request.operations.extend(user_data_operations)
        request.customer_match_user_list_metadata = customer_match_user_list_metadata

        #logging.info(f'Request: {request}')

        try:
            # Make the upload user data request
            response = user_data_service.upload_user_data(request=request)
            logging.info(f'Response: {response}')
            
            # Check for partial failures
            if hasattr(response, 'partial_failure_error') and response.partial_failure_error:
                logging.error(f'Partial failure error: {response.partial_failure_error}')
                for error in response.partial_failure_error.errors:
                    operation_index = error.location.field_path_elements[0].index
                    failed_email = batch_emails[operation_index]
                    failed_emails.append(failed_email)
                    logging.error(f'Failed to remove user with email {failed_email}: {error.error_code} - {error.message}')
                all_success = False
            else:
                logging.info(f'Successfully removed batch of users from user list {user_list_id}')
        except GoogleAdsException as ex:
            logging.error(f'Request failed with status {ex.error.code().name}')
            logging.error(f'Error message: {ex.error.message}')
            logging.error('Errors:')
            for error in ex.failure.errors:
                logging.error(f'\t{error.error_code}: {error.message}')
            all_success = False
            failed_emails.extend(batch_emails)

    return all_success, failed_emails

################ ADD EMAILS LISTS TO GOOGLE ADS LISTS ############
def add_emails_to_customer_list(client, customer_id, user_list_id, email_addresses):
    user_data_service = client.get_service("UserDataService")
    all_success = True
    failed_emails = []

    # Split email addresses into batches
    for i in range(0, len(email_addresses), MAX_OPERATIONS_PER_REQUEST):
        batch_emails = email_addresses[i:i + MAX_OPERATIONS_PER_REQUEST]
        user_data_operations = []

        for email_address in batch_emails:
            # Hash the email address using SHA-256
            hashed_email = hashlib.sha256(email_address.encode('utf-8')).hexdigest()
            #logging.info(f'Hashed email: {hashed_email}')

            # Create a user identifier with the hashed email
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_email = hashed_email

            # Create the user data
            user_data = client.get_type("UserData")
            user_data.user_identifiers.append(user_identifier)

            # Create the operation to add the user to the user list
            user_data_operation = client.get_type("UserDataOperation")
            user_data_operation.create = user_data

            user_data_operations.append(user_data_operation)

        # Create the metadata for the user list
        customer_match_user_list_metadata = client.get_type("CustomerMatchUserListMetadata")
        customer_match_user_list_metadata.user_list = f'customers/{customer_id}/userLists/{user_list_id}'

        # Create the request
        request = client.get_type("UploadUserDataRequest")
        request.customer_id = customer_id
        request.operations.extend(user_data_operations)
        request.customer_match_user_list_metadata = customer_match_user_list_metadata

        #logging.info(f'Request: {request}')

        try:
            # Make the upload user data request
            response = user_data_service.upload_user_data(request=request)
            logging.info(f'Response: {response}')
            
            # Check for partial failures
            if hasattr(response, 'partial_failure_error') and response.partial_failure_error:
                logging.error(f'Partial failure error: {response.partial_failure_error}')
                for error in response.partial_failure_error.errors:
                    operation_index = error.location.field_path_elements[0].index
                    failed_email = batch_emails[operation_index]
                    failed_emails.append(failed_email)
                    logging.error(f'Failed to add user with email {failed_email}: {error.error_code} - {error.message}')
                all_success = False
            else:
                logging.info(f'Successfully added batch of users to user list {user_list_id}')
        except GoogleAdsException as ex:
            logging.error(f'Request failed with status {ex.error.code().name}')
            logging.error(f'Error message: {ex.error.message}')
            logging.error('Errors:')
            for error in ex.failure.errors:
                logging.error(f'\t{error.error_code}: {error.message}')
            all_success = False
            failed_emails.extend(batch_emails)

    return all_success, failed_emails

################ ADDS EMAILS TO GOOGLE AND META ADS LISTS ############
def add_emails_to_google_and_meta_ads():
    url = "https://api.intercom.io/contacts/search"
    base_url = "https://api.intercom.io/contacts"
    created_at_max = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # Intercom only allows to filter by dates, not datetimes
    created_at_min = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # LOGIC FOR JUST THE DAY BEFORE. For custom timeframes use the 2 lines below
    #created_at_max = int(datetime.datetime.strptime("2024-06-16 13:59:59", "%Y-%m-%d %H:%M:%S").timestamp())# UTC TIME
    #created_at_min = int(datetime.datetime.strptime("2024-06-16 14:00:00", "%Y-%m-%d %H:%M:%S").timestamp())# UTC TIME
    config_data = yaml.safe_load(GOOGLE_ADS_CONFIG)
    googleads_client = GoogleAdsClient.load_from_dict(config_data)
    for app in APPS_CONFIG:
        if app["app_name"] != 'SR': # SR and SATC repeat the same data, so only need to update once
            headers = {
            "Content-Type": "application/json",
            "Intercom-Version": "2.10",
            "Authorization": app['api_icm_token']
            }
            next_page_params = None
            contacts = []

            while True:
                payload = {
                "query": {
                    "operator": "AND",
                    "value": [
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "custom_attributes.installed_at",
                        "operator": ">",
                        "value": created_at_min # Unix Timestamp for initial date
                        },
                        {
                        "field": "custom_attributes.installed_at",
                        "operator": "=",
                        "value": created_at_min # Unix Timestamp for final date
                        }
                    ]
                    },
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "custom_attributes.installed_at",
                        "operator": "<",
                        "value": created_at_max # Unix Timestamp for initial date
                        },
                        {
                        "field": "custom_attributes.installed_at",
                        "operator": "=",
                        "value": created_at_max # Unix Timestamp for final date
                        }
                    ]
                    }
                    ]
                },
                "pagination": {
                    "per_page": 150,
                    "starting_after": next_page_params
                }
                }
            
                response = requests.post(url, json=payload, headers=headers)
                #time.sleep(0.1)
                if response.status_code != 200:
                    logging.error(f"Error: {response.status_code}")
                    continue

                data_temp = response.json()
                next_page_params = data_temp.get('pages',{}).get('next',{}).get('starting_after')
                contacts.extend(data_temp.get('data',{}))
                logging.info(f"##########{app['app_name']} Contacts fetched: {len(contacts)} ##########") if app['app_name'] not in ['SR', 'SATC'] else logging.info(f"##########COD Contacts fetched: {len(contacts)} ##########")
                if not next_page_params:
                        break  # Exit the loop if there are no more pages.
            
            emails_list = [contact['email'] for contact in contacts]
            logging.info(emails_list)
            
            # Add emails from Meta Ads custom audience
            if app['app_name'] == 'TFX':
                if meta_test_credentials():
                    meta_add_users_to_custom_audience(emails_list)

            # Add emails from Google Ads user list           
            try:
                # Replace with your actual customer ID and user list ID 
                customer_id = app['app_google_ads_id'] # Google Ads Account ID
                user_list_id = app['app_user_list']
                email_addresses = emails_list
                success, failed_emails = add_emails_to_customer_list(googleads_client, customer_id, user_list_id, email_addresses)
                
                if success:
                    logging.info(f'All emails were successfully added to user list {user_list_id} from {app["app_name"]}.')
                else:
                    if failed_emails:
                        logging.error(f'Failed to add the following emails: {failed_emails}')
                    else:
                        logging.error(f'All emails failed to be added.')
            except GoogleAdsException as ex:
                logging.error(f'Request failed with status {ex.error.code().name}')
                logging.error(f'Error message: {ex.error.message}')
                logging.error('Errors:')
                for error in ex.failure.errors:
                    logging.error(f'\t{error.error_code}: {error.message}')
                sys.exit(1)
            except ValueError as ve:
                logging.error(f'ValueError: {ve}')
                sys.exit(1)

def remove_emails_from_google_and_meta_ads():
    url = "https://api.intercom.io/contacts/search"
    base_url = "https://api.intercom.io/contacts"
    created_at_max = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # Intercom only allows to filter by dates, not datetimes
    created_at_min = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # LOGIC FOR JUST THE DAY BEFORE. For custom timeframes use the 2 lines below
    # created_at_max = int(datetime.datetime.strptime("2024-05-11 13:59:59", "%Y-%m-%d %H:%M:%S").timestamp()) - 86400 # UTC TIME
    # created_at_min = int(datetime.datetime.strptime("2024-05-10 14:00:00", "%Y-%m-%d %H:%M:%S").timestamp())        # UTC TIME
    config_data = yaml.safe_load(GOOGLE_ADS_CONFIG)
    googleads_client = GoogleAdsClient.load_from_dict(config_data)
    for app in APPS_CONFIG:
        if app["app_name"] != 'SR': # SR and SATC repeat the same data, so only need to update once
            headers = {
            "Content-Type": "application/json",
            "Intercom-Version": "2.10",
            "Authorization": app['api_icm_token']
            }
            next_page_params = None
            contacts = []

            while True:
                payload = {
                "query": {
                    "operator": "AND",
                    "value": [
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "custom_attributes.uninstalled_at",
                        "operator": ">",
                        "value": created_at_min # Unix Timestamp for initial date
                        },
                        {
                        "field": "custom_attributes.uninstalled_at",
                        "operator": "=",
                        "value": created_at_min # Unix Timestamp for final date
                        }
                    ]
                    },
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "custom_attributes.uninstalled_at",
                        "operator": "<",
                        "value": created_at_max # Unix Timestamp for initial date
                        },
                        {
                        "field": "custom_attributes.uninstalled_at",
                        "operator": "=",
                        "value": created_at_max # Unix Timestamp for final date
                        }
                    ]
                    }
                    ]
                },
                "pagination": {
                    "per_page": 150,
                    "starting_after": next_page_params
                }
                }
            
                response = requests.post(url, json=payload, headers=headers)
                #time.sleep(0.1)
                if response.status_code != 200:
                    logging.error(f"Error: {response.status_code}")
                    continue

                data_temp = response.json()
                next_page_params = data_temp.get('pages',{}).get('next',{}).get('starting_after')
                contacts.extend(data_temp.get('data',{}))
                logging.info(f"##########{app['app_name']} Contacts fetched: {len(contacts)} ##########") if app['app_name'] not in ['SR', 'SATC'] else logging.info(f"##########COD Contacts fetched: {len(contacts)} ##########")
                if not next_page_params:
                        break  # Exit the loop if there are no more pages.
            
            emails_list = [contact['email'] for contact in contacts]
            logging.info(emails_list)

            # Remove emails from Meta Ads custom audience
            if app['app_name'] == 'TFX':
                if meta_test_credentials():
                    meta_remove_users_from_custom_audience(emails_list)

            # Remove emails from Google Ads user list
            try:
                # Replace with your actual customer ID and user list ID 
                customer_id = app['app_google_ads_id'] # Google Ads Account ID
                user_list_id = app['app_user_list']
                email_addresses = emails_list
                success, failed_emails = remove_emails_from_customer_list(googleads_client, customer_id, user_list_id, email_addresses)
                
                if success:
                    logging.info(f'All emails were successfully removed.')
                else:
                    if failed_emails:
                        logging.error(f'Failed to remove the following emails: {failed_emails}')
                    else:
                        logging.error(f'All emails failed to be removed.')
            except GoogleAdsException as ex:
                logging.error(f'Request failed with status {ex.error.code().name}')
                logging.error(f'Error message: {ex.error.message}')
                logging.error('Errors:')
                for error in ex.failure.errors:
                    logging.error(f'\t{error.error_code}: {error.message}')
                sys.exit(1)
            except ValueError as ve:
                logging.error(f'ValueError: {ve}')
                sys.exit(1)

def update_coupons_data():
    conn = connect_to_db()  # Replace with your actual connection function
    if conn is not None:
        cursor = conn.cursor()   

    url = "https://api.intercom.io/contacts/search"
    base_url = "https://api.intercom.io/contacts"
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', handlers=[logging.StreamHandler()])
    created_at_max = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # Intercom only allows to filter by dates, not datetimes
    created_at_min = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # LOGIC FOR JUST THE DAY BEFORE. For custom timeframes use the 2 lines below
    # created_at_max = int(datetime.datetime.strptime("2024-05-11 13:59:59", "%Y-%m-%d %H:%M:%S").timestamp()) - 86400 # UTC TIME
    # created_at_min = int(datetime.datetime.strptime("2024-05-10 14:00:00", "%Y-%m-%d %H:%M:%S").timestamp())        # UTC TIME

    for app in APPS_CONFIG:
        if app["app_name"] != 'SR': # SR and SATC repeat the same data, so only need to update once
            headers = {
                "Content-Type": "application/json",
                "Intercom-Version": "2.10",
                "Authorization": app['api_icm_token']
                }
            next_page_params = None
            contacts = []

            while True: 
                if app['app_name'] in ['PC', 'ICU', 'TFX']:
                    payload = {
                        "query": {
                        "operator": "AND",
                        "value": [
                            {
                                "operator": "OR",
                                "value": [
                            {
                                "field": "custom_attributes.coupon_redeemed_at",
                                "operator": ">",
                                "value": created_at_min # Unix Timestamp for initial date
                            },
                            {
                                "field": "custom_attributes.coupon_redeemed_at",
                                "operator": "=",
                                "value": created_at_min # Unix Timestamp for final date
                            }
                            ]
                            },
                            {
                                "operator": "OR",
                                "value": [
                            {
                                "field": "custom_attributes.coupon_redeemed_at",
                                "operator": "<",
                                "value": created_at_max # Unix Timestamp for initial date
                            },
                            {
                                "field": "custom_attributes.coupon_redeemed_at",
                                "operator": "=",
                                "value": created_at_max # Unix Timestamp for final date
                            }
                            ]
                            }
                        ]
                        },
                        "pagination": {
                        "per_page": 150,
                        "starting_after": next_page_params
                        }
                    } 
                else:
                        payload = {
                        "query": {
                        "operator": "AND",
                        "value": [
                            {
                                "operator": "OR",
                                "value": [
                            {
                                "field": "custom_attributes.coupon_redeem_at",
                                "operator": ">",
                                "value": created_at_min # Unix Timestamp for initial date
                            },
                            {
                                "field": "custom_attributes.coupon_redeem_at",
                                "operator": "=",
                                "value": created_at_min # Unix Timestamp for final date
                            }
                            ]
                            },
                            {
                                "operator": "OR",
                                "value": [
                            {
                                "field": "custom_attributes.coupon_redeem_at",
                                "operator": "<",
                                "value": created_at_max # Unix Timestamp for initial date
                            },
                            {
                                "field": "custom_attributes.coupon_redeem_at",
                                "operator": "=",
                                "value": created_at_max # Unix Timestamp for final date
                            }
                            ]
                            }
                        ]
                        },
                        "pagination": {
                        "per_page": 150,
                        "starting_after": next_page_params
                        }
                    }  
                
                response = requests.post(url, json=payload, headers=headers)
                #time.sleep(0.1)
                if response.status_code != 200:
                    logging.error(f"Error: {response.status_code}")
                    continue

                data_temp = response.json()
                next_page_params = data_temp.get('pages',{}).get('next',{}).get('starting_after')
                contacts.extend(data_temp.get('data',{}))
                logging.info(f"{app['app_name']} Contacts fetched: {len(contacts)}") if app['app_name'] not in ['SR', 'SATC'] else logging.info(f"COD Contacts fetched: {len(contacts)}")
                if not next_page_params:
                    break  # Exit the loop if there are no more pages.
                
            for contact in contacts:
                current_id = contact.get('id')
                current_email = contact.get('email')
                current_coupon = contact.get('custom_attributes',{}).get('coupon_redeemed') if app['app_name'] in ['PC', 'ICU', 'TFX'] else contact.get('custom_attributes',{}).get('coupon_redeem')
                current_coupon_timestamp = contact.get('custom_attributes',{}).get('coupon_redeemed_at') if app['app_name'] in ['PC', 'ICU', 'TFX'] else contact.get('custom_attributes',{}).get('coupon_redeem_at')
                current_coupon_dt = datetime.datetime.fromtimestamp(current_coupon_timestamp).strftime("%Y-%m-%d %H:%M:%S") # Assure this is taken in UTC timezone
                current_coupon_value = contact.get('custom_attributes',{}).get('coupon_value')
                if conn is not None:
                    insert_query = sql.SQL(
                        '''
                        UPDATE intercom_contacts
                        SET
                        coupon_redeemed = %s,
                        coupon_redeemed_at = %s,
                        coupon_value = %s
                        WHERE id = %s AND app = %s;
                        '''
                    )
                    cursor.execute(insert_query, (current_coupon, current_coupon_dt, current_coupon_value, current_id, app['app_name']))
                    logging.info(f"Updated ID: {current_id} with email: {current_email} and coupon: {current_coupon} at: {current_coupon_dt}")
                else:
                    logging.error("Failed to insert data into the database.")

    conn.commit()
    cursor.close()
    conn.close()
    

def connect_to_db_sqlalchemy(): 
    user = DB_CREDENTIALS['user']
    password = DB_CREDENTIALS['password']
    host = DB_CREDENTIALS['host']
    dbname = DB_CREDENTIALS['database']
    port = DB_CREDENTIALS['port']
    sslmode = DB_CREDENTIALS['sslmode']
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"

    engine = create_engine(connection_string)
    connection = None

    while connection is None:
        try:
            connection = engine.connect()
            logging.info("Successfully connected to the database.")
        except OperationalError as e:
            logging.error(f"Connection failed: {e}")
            logging.info("Retrying in 1 minute...")
            time.sleep(60)
    
    return engine

# Function to parse URL and extract query parameters
def extract_query_params(url):
    query_string = urlparse(url).query
    params = parse_qs(query_string)
    return {
        'locale': params.get('locale', [None])[0],
        'user_id': params.get('user_id', [None])[0],
        'search_id': params.get('search_id', [None])[0],
        'surface_detail': params.get('surface_detail', [None])[0],
        'surface_inter_position': params.get('surface_inter_position', [None])[0],
        'surface_intra_position': params.get('surface_intra_position', [None])[0],
        'surface_type': params.get('surface_type', [None])[0]
    }

def insert_ga4_into_db(df):
  i = 1
  conn = connect_to_db()  # Replace with your actual connection function
  if conn is not None:
    cursor = conn.cursor()
    insert_query = sql.SQL(
      """
      INSERT INTO ga4_events (
        event_name,
        shop_id,
        url,
        date_hour_minute,
        campaign,
        source,
        medium,
        content,
        sessions,
        engaged_sessions,
        event_count,
        date_hour_minute_utc,
        date_hour_minute_est,
        locale,
        user_id,
        search_id,
        surface_detail,
        surface_inter_position,
        surface_intra_position,
        surface_type,
        app
      ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      )
      """)
    for index, row in df.iterrows():
      cursor.execute(insert_query, (
        row['eventName'],
        row['customEvent:shop_id'],
        row['landingPagePlusQueryString'],
        row['dateHourMinute'],
        row['sessionCampaignName'],
        row['sessionSource'],
        row['sessionMedium'],
        row['sessionManualAdContent'],
        row['sessions'],
        row['engagedSessions'],
        row['eventCount'],
        row['dateHourMinuteUTC'],
        row['dateHourMinuteEST'],
        row['locale'],
        row['user_id'],
        row['search_id'],
        row['surface_detail'],
        row['surface_inter_position'],
        row['surface_intra_position'],
        row['surface_type'],
        row['app']
      ))
      logging.info(f"{i}/{len(df)}")
      i += 1
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Done!")
  else:
    logging.info("Failed to insert data into the database.")


def insert_intercom_contacts_into_db(df):
    i = 1
    conn = connect_to_db()  # Replace with your actual connection function
    if conn is not None:
        cursor = conn.cursor()
        insert_query = sql.SQL(
        """
        INSERT INTO intercom_contacts (
            id,
            email,
            phone,
            name,
            app,
            created_at,
            signed_up_at,
            country,
            region,
            city,
            country_code,
            shopify_domain,
            shopify_plan,
            coupon_redeemed,
            coupon_redeemed_at,
            coupon_value
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """)
        for index, row in df.iterrows():
            cursor.execute(insert_query, (
                row['id'],
                row['email'],
                row['phone'],
                row['name'],
                row['app'],
                row['created_at'],
                row['signed_up_at'],
                row['country'],
                row['region'],
                row['city'],
                row['country_code'],
                row['shopify_domain'],
                row['shopify_plan'],
                row['coupon_redeemed'],
                row['coupon_redeemed_at'],
                row['coupon_value']
            ))
            logging.info(f"{i}/{len(df)}")
            i += 1
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Done!")
    else:
        logging.info("Failed to insert data into the database.")


def fetch_GA4_sessions():
    #Initialize the DF and the events to be tracked
    df = pd.DataFrame()
    dates = [(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
    logging.info(f"Dates considered:{dates}")    
    
    # Initialize OAuth2 credentials
    # Load the JSON string from the environment variable
    oauth_json_string = GA4_OAUTH
    if oauth_json_string is None:
       raise ValueError("OAUTH_JSON environment variable is not set")
    #Deserialize the JSON string into a Python dictionary
    oauth_data = json.loads(oauth_json_string)
    #Create credentials object from the dictionary
    credentials = Credentials(
        token=oauth_data["token"],
        refresh_token=oauth_data["refresh_token"],
        token_uri=oauth_data["token_uri"],
        client_id=oauth_data["client_id"],
        client_secret=oauth_data["client_secret"],
        scopes=oauth_data["scopes"],
        universe_domain=oauth_data["universe_domain"],
        account=oauth_data["account"],
        expiry = datetime.datetime.strptime(oauth_data['expiry'], "%Y-%m-%dT%H:%M:%S.%fZ")
    )
    # Initialize the GA4 client
    client = BetaAnalyticsDataClient(credentials=credentials)

    # Fetch sessions for each date
    for date in dates:
        for app in APPS_CONFIG:
            # Initialize dimensions list
            dimensions = [
                Dimension(name="landingPagePlusQueryString"),
                Dimension(name="dateHourMinute"),
                Dimension(name="sessionCampaignName"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionManualAdContent"),
            ]

            # Conditionally add customEvent:shop_id dimension
            if app['app_name'] != 'SR':
                dimensions.append(Dimension(name="customEvent:shop_id"))

            url_name_filter = Filter(
                field_name="landingPagePlusQueryString",
                string_filter=Filter.StringFilter(value=f"{app['api_ga4_url']}", match_type=Filter.StringFilter.MatchType.BEGINS_WITH)
            )

            # Define the request to fetch data with filter
            request = RunReportRequest(
                property=f"properties/{app['api_ga4_code']}",
                date_ranges=[DateRange(start_date=date, end_date=date)],
                dimensions=dimensions,
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="engagedSessions"),
                    Metric(name="eventCount")
                ],
                dimension_filter=FilterExpression(filter=url_name_filter)
            )

            # Run the report
            response = client.run_report(request)

            # Prepare data for DataFrame
            data = []
            for row in response.rows:
                row_dict = {header.name: value.value for header, value in zip(response.dimension_headers, row.dimension_values)}
                row_dict.update({header.name: value.value for header, value in zip(response.metric_headers, row.metric_values)})
                data.append(row_dict)

            df_temp = pd.DataFrame(data)
            df_temp['app'] = app['app_name']
            if app['app_name'] == 'SR':
                df_temp['customEvent:shop_id'] = "(not set)"
            df = pd.concat([df, df_temp], ignore_index=True)

    df['eventName'] = 'session' #Manually adds the event name to identify sessions       
    # Convert 'dateHourMinute' to datetime with the correct format (if needed)
    df['dateHourMinute'] = pd.to_datetime(df['dateHourMinute'], format='%Y%m%d%H%M')
    # Localize the datetime to Property Time without converting
    df['dateHourMinute'] = df['dateHourMinute'].dt.tz_localize('America/New_York') # All properties are new set to New York TZ
    # Convert 'dateHourMinute' from Property Time to UTC and assign to a new column
    df['dateHourMinuteUTC'] = df['dateHourMinute'].dt.tz_convert('UTC')
    # Convert 'dateHourMinute' from Property Time to EST and assign to a new column
    df['dateHourMinuteEST'] = df['dateHourMinute'].dt.tz_convert('America/New_York')
    # Convert datetimes to string adn gets rid of the timezone
    df['dateHourMinute'] = df['dateHourMinute'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['dateHourMinuteUTC'] = df['dateHourMinuteUTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['dateHourMinuteEST'] = df['dateHourMinuteEST'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Apply the function and assign results to new columns
    df_params = df['landingPagePlusQueryString'].apply(extract_query_params)
    df = df.join(pd.json_normalize(df_params))
    insert_ga4_into_db(df)
        
def fetch_ga4_events():
    #Initialize the DF and the events to be tracked
    df = pd.DataFrame()
    event_types = ["shopify_app_install", "Add App button"]
    dates = [(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
    logging.info(f"Dates considered:{dates}")    
    
    # Initialize OAuth2 credentials
    # Load the JSON string from the environment variable
    oauth_json_string = GA4_OAUTH
    if oauth_json_string is None:
       raise ValueError("OAUTH_JSON environment variable is not set")
    #Deserialize the JSON string into a Python dictionary
    oauth_data = json.loads(oauth_json_string)
    #Create credentials object from the dictionary
    credentials = Credentials(
        token=oauth_data["token"],
        refresh_token=oauth_data["refresh_token"],
        token_uri=oauth_data["token_uri"],
        client_id=oauth_data["client_id"],
        client_secret=oauth_data["client_secret"],
        scopes=oauth_data["scopes"],
        universe_domain=oauth_data["universe_domain"],
        account=oauth_data["account"],
        expiry = datetime.datetime.strptime(oauth_data['expiry'], "%Y-%m-%dT%H:%M:%S.%fZ")
    )
    # Initialize the GA4 client
    client = BetaAnalyticsDataClient(credentials=credentials)

    for date in dates:
        for app in APPS_CONFIG:
            for event in event_types:
                # Initialize dimensions list
                dimensions = [
                    Dimension(name="eventName"),
                    Dimension(name="landingPagePlusQueryString"),
                    Dimension(name="dateHourMinute"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionManualAdContent"),
                ]

                # Conditionally add customEvent:shop_id dimension
                if app['app_name'] != 'SR':
                    dimensions.append(Dimension(name="customEvent:shop_id"))
                
                # Define the filter for the event name
                event_name_filter = Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(value=f"{event}", match_type=Filter.StringFilter.MatchType.EXACT)
                )

                # Define the request to fetch data with filter
                request = RunReportRequest(
                    property=f"properties/{app['api_ga4_code']}",
                    date_ranges=[DateRange(start_date=date, end_date=date)],
                    dimensions=dimensions,
                    metrics=[
                        Metric(name="sessions"),
                        Metric(name="engagedSessions"),
                        Metric(name="eventCount")
                    ],
                    dimension_filter=FilterExpression(filter=event_name_filter)
                )

                # Run the report
                response = client.run_report(request)

                # Prepare data for DataFrame
                data = []
                for row in response.rows:
                    row_dict = {header.name: value.value for header, value in zip(response.dimension_headers, row.dimension_values)}
                    row_dict.update({header.name: value.value for header, value in zip(response.metric_headers, row.metric_values)})
                    data.append(row_dict)

                df_temp = pd.DataFrame(data)
                df_temp['app'] = app['app_name']
                if app['app_name'] == 'SR':
                    df_temp['customEvent:shop_id'] = "(not set)"
                df = pd.concat([df, df_temp], ignore_index=True)

    # Convert 'dateHourMinute' to datetime with the correct format (if needed)
    df['dateHourMinute'] = pd.to_datetime(df['dateHourMinute'], format='%Y%m%d%H%M')
    # Localize the datetime to Property Time without converting
    df['dateHourMinute'] = df['dateHourMinute'].dt.tz_localize('America/New_York') # All properties are new set to New York TZ
    # Convert 'dateHourMinute' from Property Time to UTC and assign to a new column
    df['dateHourMinuteUTC'] = df['dateHourMinute'].dt.tz_convert('UTC')
    # Convert 'dateHourMinute' from Property Time to EST and assign to a new column
    df['dateHourMinuteEST'] = df['dateHourMinute'].dt.tz_convert('America/New_York')
    # Convert datetimes to string adn gets rid of the timezone
    df['dateHourMinute'] = df['dateHourMinute'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['dateHourMinuteUTC'] = df['dateHourMinuteUTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['dateHourMinuteEST'] = df['dateHourMinuteEST'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Apply the function and assign results to new columns
    df_params = df['landingPagePlusQueryString'].apply(extract_query_params)
    df = df.join(pd.json_normalize(df_params))
    insert_ga4_into_db(df)

def fetch_intercom_contacts():
    url = "https://api.intercom.io/contacts/search"
    created_at_max = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # Intercom only allows to filter by dates, not datetimes
    created_at_min = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 86400 # LOGIC FOR JUST THE DAY BEFORE. For custom timeframes use the 2 lines below
    # created_at_max = int(datetime.datetime.strptime("2024-05-11 13:59:59", "%Y-%m-%d %H:%M:%S").timestamp()) - 86400 # UTC TIME
    # created_at_min = int(datetime.datetime.strptime("2024-05-10 14:00:00", "%Y-%m-%d %H:%M:%S").timestamp())        # UTC TIME
    df_contacts = pd.DataFrame()
    
    for app in APPS_CONFIG:
        headers = {
            "Content-Type": "application/json",
            "Intercom-Version": "2.10",
            "Authorization": app['api_icm_token']
        }
        next_page_params = None
        contacts = []
        test_contacts = []
        while True: 
            payload = {
                "query": {
                    "operator": "AND",
                    "value": [
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "created_at",
                        "operator": ">",
                        "value": created_at_min # Unix Timestamp for initial date
                        },
                        {
                        "field": "created_at",
                        "operator": "=",
                        "value": created_at_min # Unix Timestamp for final date
                        }
                    ]
                    },
                    {
                        "operator": "OR",
                        "value": [
                        {
                        "field": "created_at",
                        "operator": "<",
                        "value": created_at_max # Unix Timestamp for initial date
                        },
                        {
                        "field": "created_at",
                        "operator": "=",
                        "value": created_at_max # Unix Timestamp for final date
                        }
                    ]
                    }
                    ]
                },
                "pagination": {
                    "per_page": 150,
                    "starting_after": next_page_params
                }
                }  
            
            response = requests.post(url, json=payload, headers=headers)
            #time.sleep(0.1)
            if response.status_code != 200:
                logging.error(f"Error: {response.status_code}")
                continue

            data_temp = response.json()
            next_page_params = data_temp.get('pages',{}).get('next',{}).get('starting_after')
            contacts.extend(data_temp.get('data',{}))
            logging.info(f"Contacts fetched: {len(contacts)}")
            if not next_page_params:
                break  # Exit the loop if there are no more pages.
        
        # 1-app type df
        if app['app_name'] == "ICU" or app['app_name'] == "TFX" or app['app_name'] == "PC":
            for contact in contacts:
                # General Attributes
                id = contact.get('id')
                email = contact.get('email')
                phone = contact.get('phone')
                name = contact.get('name')
                created_at = contact.get('created_at')
                signed_up_at = contact.get('signed_up_at')
                country = contact.get('location',{}).get('country')
                region = contact.get('location',{}).get('region')
                city = contact.get('location',{}).get('city')
                country_code = contact.get('location',{}).get('country_code')
                # Custom Attributes
                app_name = app["app_name"]
                if app["app_name"] == 'ICU':
                    shopify_domain = contact.get('custom_attributes',{}).get('shop_url')
                    shopify_plan = contact.get('custom_attributes',{}).get('shopify_plan')
                    coupon_redeemed = contact.get('custom_attributes',{}).get('coupon_redeemed')
                    coupon_redeemed_at = contact.get('custom_attributes',{}).get('coupon_redeemed_at')
                    coupon_value = contact.get('custom_attributes',{}).get('coupon_value')
                elif app["app_name"] == 'TFX':
                    shopify_domain = contact.get('custom_attributes',{}).get('shopify_url')
                    shopify_plan = contact.get('custom_attributes',{}).get('plan_display_name')
                    coupon_redeemed = contact.get('custom_attributes',{}).get('coupon_redeemed')
                    coupon_redeemed_at = contact.get('custom_attributes',{}).get('coupon_redeemed_at')
                    coupon_value = contact.get('custom_attributes',{}).get('coupon_value')
                else: #PC
                    shopify_domain = contact.get('custom_attributes',{}).get('shop_url')
                    shopify_plan = contact.get('custom_attributes',{}).get('shopify_plan')
                    coupon_redeemed = contact.get('custom_attributes',{}).get('coupon_redeemed')
                    coupon_redeemed_at = contact.get('custom_attributes',{}).get('coupon_redeemed_at')
                    coupon_value = contact.get('custom_attributes',{}).get('coupon_value')
            
                test_contacts.append({
                    "id": id,
                    "email": email,
                    "phone": phone,
                    "name": name,
                    "app": app_name,
                    "created_at": created_at,
                    "signed_up_at": signed_up_at,
                    "country": country,
                    "region": region,
                    "city": city,
                    "country_code": country_code,
                    "shopify_domain": shopify_domain,
                    "shopify_plan": shopify_plan,
                    "coupon_redeemed": coupon_redeemed,
                    "coupon_redeemed_at": coupon_redeemed_at,
                    "coupon_value": coupon_value
                })
            df_temp = pd.DataFrame(test_contacts)
        
        # 2-app type df
        if app['app_name'] == "SATC":
            for contact in contacts:
                # General Attributes
                id = contact.get('id')
                email = contact.get('email')
                phone = contact.get('phone')
                name = contact.get('name')
                created_at = contact.get('created_at')
                signed_up_at = contact.get('signed_up_at')
                country = contact.get('location',{}).get('country')
                region = contact.get('location',{}).get('region')
                city = contact.get('location',{}).get('city')
                country_code = contact.get('location',{}).get('country_code')
                # Custom Attributes
                app_raw = contact.get('custom_attributes',{}).get('App name')
                app_name = "SATC" if app_raw == 'Sticky' else "SR"
                shopify_domain = contact.get('custom_attributes',{}).get('Shop name')
                shopify_plan = contact.get('custom_attributes',{}).get('Plan display name')
                coupon_redeemed = contact.get('custom_attributes',{}).get('coupon_redeem')
                coupon_redeemed_at = contact.get('custom_attributes',{}).get('coupon_redeem_at')
                coupon_value = contact.get('custom_attributes',{}).get('coupon_value')
                test_contacts.append({
                    "id": id,
                    "email": email,
                    "phone": phone,
                    "name": name,
                    "app": app_name,
                    "created_at": created_at,
                    "signed_up_at": signed_up_at,
                    "country": country,
                    "region": region,
                    "city": city,
                    "country_code": country_code,
                    "shopify_domain": shopify_domain,
                    "shopify_plan": shopify_plan,
                    "coupon_redeemed": coupon_redeemed,
                    "coupon_redeemed_at": coupon_redeemed_at,
                    "coupon_value": coupon_value
                })
            df_temp = pd.DataFrame(test_contacts)

        # Avoid reprocessing of COD contacts for SR (SR and SATC share the same property on Intercom)
        if app['app_name'] == "SR":
            df_temp = pd.DataFrame() # Appends a void df to df_contacts

        #Appends the temporary dataframe to the main one
        df_contacts = pd.concat([df_contacts, df_temp], ignore_index=True)
        logging.info(f"Partner {app['app_name']} processed. Total {len(test_contacts)} contacts. For COD all contacts are processed and segregated (SATC-SR) within SATC")
    
    logging.info(f"Done! Total contacts: {len(df_contacts)}")
    # Update these columns to datetime instead of Unix timestamps, replaces NaN and NaT with None
    df_contacts = df_contacts.replace({np.nan: None})
    df_contacts['created_at'] = pd.to_datetime(df_contacts['created_at'], unit='s',utc=True)
    #df_contacts['created_at'] = df_contacts['created_at'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    df_contacts['signed_up_at'] = pd.to_datetime(df_contacts['signed_up_at'], unit='s',utc=True)
    #df_contacts['signed_up_at'] = df_contacts['updated_at'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    df_contacts['coupon_redeemed_at'] = pd.to_datetime(df_contacts['coupon_redeemed_at'], unit='s',utc=True)
    #df_contacts['signed_up_at'] = df_contacts['updated_at'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    df_contacts = df_contacts.replace({pd.NaT: None})
    insert_intercom_contacts_into_db(df_contacts)


def update_intercom_contacts():
    for app in APPS_CONFIG:
        df = pd.DataFrame() # Create an empty dataframe
        # Connect to the database
        engine = connect_to_db_sqlalchemy() #SQLAlchemy is needed for this connection type, instead of psycopg2
        # SQL query
        query = f"SELECT * FROM intercom_customer_list_{app['app_name'].lower()};"
        with engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall())
            df.columns = result.keys()        
        # Convert datetime columns to string in ISO 8601 format
        datetime_cols = ['installed_at', 'uninstalled_at', 'last_subscription_charged_at', 'store_open_at', 'store_closed_at', 'first_install']  # Add all datetime columns
        for col in datetime_cols:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        logging.info(f"App {app['app_name']} DB table processed. Total contacts: {len(df)}")
        # Update the contacts using Intercom API
        headers = {
            "Content-Type": "application/json",
            "Intercom-Version": "2.11",
            "Authorization": app['api_icm_token']
            }
        base_url = "https://api.intercom.io/contacts"
        for index, row in df.iterrows():
            logging.info(f"Updating record {int(index)+1}/{len(df)} for {app['app_name']}")
            url = f"{base_url}/{row['id']}" # ID needed to update - ids used for testing ONLY - PROD row['id']
            
            # Creates the payload depending on the app
            if app['app_name'] == "SR" or app['app_name'] == "SATC":
                payload = {
                    "email": row['email'],  
                    "name": row['shop_name'],
                    "custom_attributes": {
                        "App name": row['app_name'], #Comes from the app name column for COD, for the rest is disposable
                        "Plan display name": row['shopify_plan'],
                        "Shop name": row['shop_url'], 
                        "shop_url": row['shop_url'], # Same as shop name
                        "active": row['active'],
                        "installed_at": row['installed_at'],
                        "uninstalled_at": row['uninstalled_at'], #This could be NaT
                        "last_subscription_charged_at": row['last_subscription_charged_at'],
                        "number_of_subscription_charges":row['number_of_subscription_charges'] if pd.notnull(row['number_of_subscription_charges']) else None,
                        "subscription_charge": str(row['subscription_charge']),
                        "first_install": row['first_install'],
                        "store_open_at": row['store_open_at'],
                        "store_closed_at": row['store_closed_at'],
                        "is_open": row['is_open'],
                        "days_uninstalled": int(row['days_uninstalled']),
                        "days_closed": int(row['days_closed']),
                        "trial_days_remaining": row['trial_days_remaining'],
                        "days_since_billed": row['days_since_billed'],
                        "paid_active": row['paid_active'],
                        #"plan": row['plan'], ### FOR PC ONLY
                        "time_closed": int(row['time_closed']),
                        "time_uninstalled": int(row['time_uninstalled']),
                        "shopify_url_raw": row['shopify_url_raw'] # Just for COD
                    }
                }
            if app['app_name'] == "PC":
                payload = {
                    "email": row['email'],  
                    "name": row['shop_name'],
                    "custom_attributes": {
                        # "App name": row['app_name'], #Comes from the app name column for COD, for the rest is disposable
                        "shopify_plan": row['shopify_plan'],
                        #"Shop name": row['shop_url'], # Not needed for PC
                        "shop_url": row['shop_url'], # Same as shop name
                        "active": row['active'],
                        "installed_at": row['installed_at'],
                        "uninstalled_at": row['uninstalled_at'], #This could be NaT
                        "last_subscription_charged_at": row['last_subscription_charged_at'],
                        "number_of_subscription_charges":row['number_of_subscription_charges'] if pd.notnull(row['number_of_subscription_charges']) else None,
                        "subscription_charge": str(row['subscription_charge']),
                        "first_install": row['first_install'],
                        "store_open_at": row['store_open_at'],
                        "store_closed_at": row['store_closed_at'],
                        "is_open": row['is_open'],
                        "days_uninstalled": int(row['days_uninstalled']),
                        "days_closed": int(row['days_closed']),
                        "trial_days_remaining": row['trial_days_remaining'],
                        "days_since_billed": row['days_since_billed'],
                        "paid_active": row['paid_active'],
                        "plan": row['plan'], ### FOR PC ONLY
                        "time_closed": int(row['time_closed']),
                        "time_uninstalled": int(row['time_uninstalled']),
                        #"shopify_url_raw": row['shopify_url_raw'] # Just for COD
                    }
                }
            if app['app_name'] == "ICU":
                payload = {
                    "email": row['email'],  
                    "name": row['shop_name'],
                    "custom_attributes": {
                        # "App name": row['app_name'], #Comes from the app name column for COD, for the rest is disposable
                        "shopify_plan": row['shopify_plan'],
                        # "Shop name": row['shop_url'], # Not needed for ICU
                        "shop_url": row['shop_url'], # Same as shop name
                        "active": row['active'],
                        "installed_at": row['installed_at'],
                        "uninstalled_at": row['uninstalled_at'], #This could be NaT
                        "last_subscription_charged_at": row['last_subscription_charged_at'],
                        "number_of_subscription_charges":row['number_of_subscription_charges'] if pd.notnull(row['number_of_subscription_charges']) else None,
                        "subscription_charge": str(row['subscription_charge']),
                        "first_install": row['first_install'],
                        "store_open_at": row['store_open_at'],
                        "store_closed_at": row['store_closed_at'],
                        "is_open": row['is_open'],
                        "days_uninstalled": int(row['days_uninstalled']),
                        "days_closed": int(row['days_closed']),
                        "trial_days_remaining": row['trial_days_remaining'],
                        "days_since_billed": row['days_since_billed'],
                        "paid_active": row['paid_active'],
                        #"plan": row['plan'], ### FOR PC ONLY
                        "time_closed": int(row['time_closed']),
                        "time_uninstalled": int(row['time_uninstalled']),
                        #"shopify_url_raw": row['shopify_url_raw'] # Just for COD
                    }
                }
            if app['app_name'] == "TFX":
                payload = {
                    "email": row['email'],  
                    "name": row['shop_name'],
                    "custom_attributes": {
                        # "App name": row['app_name'], #Comes from the app name column for COD, for the rest is disposable
                        "plan_display_name": row['shopify_plan'],
                        "shop_url": row['shop_url'], 
                        "shopify_url": row['shop_url'], # Same as shop name
                        "active": row['active'],
                        "installed_at": row['installed_at'],
                        "uninstalled_at": row['uninstalled_at'], #This could be NaT
                        "last_subscription_charged_at": row['last_subscription_charged_at'],
                        "number_of_subscription_charges":row['number_of_subscription_charges'] if pd.notnull(row['number_of_subscription_charges']) else None,
                        "subscription_charge": str(row['subscription_charge']),
                        "first_install": row['first_install'],
                        "store_open_at": row['store_open_at'],
                        "store_closed_at": row['store_closed_at'],
                        "is_open": row['is_open'],
                        "days_uninstalled": int(row['days_uninstalled']),
                        "days_closed": int(row['days_closed']),
                        "trial_days_remaining": row['trial_days_remaining'],
                        "days_since_billed": row['days_since_billed'],
                        "paid_active": row['paid_active'],
                        #"plan": row['plan'], ### FOR PC ONLY
                        "time_closed": int(row['time_closed']),
                        "time_uninstalled": int(row['time_uninstalled']),
                        #"shopify_url_raw": row['shopify_url_raw'] # Just for COD
                    }
                }
            # Update the contacts
            response = requests.put(url, json=payload, headers=headers)
            data = response.json()
            logging.info(data)
        logging.info(f"App {app['app_name']} contacts updated. Total contacts: {len(df)}")
    


if __name__ == "__main__":
    logging.info("Starting main execution.")
    scheduler = BlockingScheduler()

    # Immediate execution upon deployment
    #time.sleep(int(OFFSET_BT_SCRIPTS))

    # Schedule the tasks to run daily at 12:00 PM UTC TIME
    scheduler.add_job(fetch_GA4_sessions, 'cron', hour=11, minute=6)
    scheduler.add_job(fetch_ga4_events, 'cron', hour=11, minute=4)
    scheduler.add_job(fetch_intercom_contacts, 'cron', hour=11, minute=0)
    scheduler.add_job(update_coupons_data, 'cron', hour=11, minute=2)
    scheduler.add_job(update_intercom_contacts, 'cron', hour=11, minute=8)
    scheduler.add_job(add_emails_to_google_and_meta_ads, 'cron', hour=14, minute=0)
    scheduler.add_job(remove_emails_from_google_and_meta_ads, 'cron', hour=14, minute=2)
    #scheduler.add_job(fetch_transactions, 'cron', hour=6, minute=int(OFFSET_BT_SCRIPTS) / 60)  # Assuming OFFSET_BT_SCRIPTS is in seconds
    scheduler.start()
