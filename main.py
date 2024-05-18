import time
import datetime
import requests
import numpy as np
import pandas as pd
import logging
import json
import psycopg2
from psycopg2 import sql
from config import APPS_CONFIG, DB_CREDENTIALS, UPDATE_INTERVAL, TIME_DELAY, OFFSET_BT_SCRIPTS
from config import GA4_OAUTH
from apscheduler.schedulers.blocking import BlockingScheduler
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression, Filter
from google.oauth2.credentials import Credentials
from urllib.parse import urlparse, parse_qs

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', handlers=[logging.StreamHandler()])

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CREDENTIALS)
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {str(e)}")
        return None

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
            shopify_plan
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
                row['shopify_plan']
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
    dates = [(datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")]
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
    dates = [(datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")]
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
                elif app["app_name"] == 'TFX':
                    shopify_domain = contact.get('custom_attributes',{}).get('shopify_url')
                    shopify_plan = contact.get('custom_attributes',{}).get('plan_display_name')
                else: #PC
                    shopify_domain = contact.get('custom_attributes',{}).get('shop_url')
                    shopify_plan = contact.get('custom_attributes',{}).get('shopify_plan')
            
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
                    "shopify_plan": shopify_plan
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
                    "shopify_plan": shopify_plan
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
    df_contacts = df_contacts.replace({pd.NaT: None})
    insert_intercom_contacts_into_db(df_contacts)


if __name__ == "__main__":
    logging.info("Starting main execution.")
    scheduler = BlockingScheduler()

    # Immediate execution upon deployment
    fetch_intercom_contacts()
    #time.sleep(int(OFFSET_BT_SCRIPTS))
    #fetch_transactions()

    # Schedule the tasks to run daily at 7:00 AM
    scheduler.add_job(fetch_GA4_sessions, 'cron', hour=12, minute=0)
    scheduler.add_job(fetch_ga4_events, 'cron', hour=12, minute=2)
    scheduler.add_job(fetch_ga4_events, 'cron', hour=12, minute=4)
    #scheduler.add_job(fetch_transactions, 'cron', hour=6, minute=int(OFFSET_BT_SCRIPTS) / 60)  # Assuming OFFSET_BT_SCRIPTS is in seconds
    scheduler.start()
