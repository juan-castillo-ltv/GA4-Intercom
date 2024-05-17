import time
import datetime
import requests
import pandas as pd
import logging
import json
import psycopg2
from psycopg2 import sql
from config import APPS_CONFIG, DB_CREDENTIALS, UPDATE_INTERVAL, TIME_DELAY, OFFSET_BT_SCRIPTS
from config import GA4_TOKEN, GA4_REFRESH_TOKEN, GA4_TOKEN_URI, GA4_CLIENT_ID, GA4_CLIENT_SECRET, GA4_SCOPES, GA4_UNIVERSE_DOMAIN, GA4_ACCOUNT, GA4_EXPIRY
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

def insert_sessions_into_db(df):
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

def fetch_GA4_sessions():
    #Initialize the DF and the events to be tracked
    df = pd.DataFrame()
    dates = [(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
    logging.info(f"Dates considered:{dates}")    
    
    # Initialize OAuth2 credentials
    # Load the JSON string from the environment variable
    # oauth_json_string = OAUTH_FILE
    # if oauth_json_string is None:
    #    raise ValueError("OAUTH_JSON environment variable is not set")
    # Deserialize the JSON string into a Python dictionary
    # oauth_data = json.loads(oauth_json_string)
    # Create credentials object from the dictionary
    credentials = Credentials(
        token=GA4_TOKEN,
        refresh_token=GA4_REFRESH_TOKEN,
        token_uri=GA4_TOKEN_URI,
        client_id=GA4_CLIENT_ID,
        client_secret=GA4_CLIENT_SECRET,
        scopes=GA4_SCOPES,
        universe_domain=GA4_UNIVERSE_DOMAIN,
        account=GA4_ACCOUNT,
        expiry=GA4_EXPIRY
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
    insert_sessions_into_db(df)
        

if __name__ == "__main__":
    logging.info("Starting main execution.")
    scheduler = BlockingScheduler()

    # Immediate execution upon deployment
    fetch_GA4_sessions()
    #time.sleep(int(OFFSET_BT_SCRIPTS))
    #fetch_transactions()

    # Schedule the tasks to run daily at 6:00 AM
    scheduler.add_job(fetch_GA4_sessions, 'cron', hour=5, minute=0)
    #scheduler.add_job(fetch_transactions, 'cron', hour=6, minute=int(OFFSET_BT_SCRIPTS) / 60)  # Assuming OFFSET_BT_SCRIPTS is in seconds
    scheduler.start()
