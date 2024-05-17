import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv("config.env")
load_dotenv()

UPDATE_INTERVAL = os.getenv("UPDATE_INTERVAL")
TIME_DELAY = os.getenv("TIME_DELAY")
OFFSET_BT_SCRIPTS = os.getenv("OFFSET_BT_SCRIPTS")
OAUTH_FILE = os.getenv("OAUTH_FILE")


GA4_TOKEN = os.getenv("GA4_TOKEN")
GA4_REFRESH_TOKEN = os.getenv("GA4_REFRESH_TOKEN")
GA4_TOKEN_URI = os.getenv("GA4_TOKEN_URI")
GA4_CLIENT_ID = os.getenv("GA4_CLIENT_ID")
GA4_CLIENT_SECRET = os.getenv("GA4_CLIENT_SECRET")
GA4_SCOPES = os.getenv("GA4_SCOPES")
GA4_UNIVERSE_DOMAIN = os.getenv("GA4_UNIVERSE_DOMAIN")
GA4_ACCOUNT = os.getenv("GA4_ACCOUNT")
GA4_EXPIRY = os.getenv("GA4_EXPIRY")

# Configurations for multiple apps
APPS_CONFIG = [
    {
        "app_name": os.getenv("APP1_Name"),
        "api_ga4_url": os.getenv("APP1_API_GA4_URL"),
        "api_ga4_code": os.getenv("APP1_API_GA4_CODE"),
        "api_icm_token": os.getenv("APP1_API_ICM_TOKEN"),
        "api_icm_id": os.getenv("APP1_API_ICM_ID")
    },
    {
       "app_name": os.getenv("APP2_Name"),
       "api_ga4_url": os.getenv("APP2_API_GA4_URL"),
       "api_ga4_code": os.getenv("APP2_API_GA4_CODE"),
       "api_icm_token": os.getenv("APP2_API_ICM_TOKEN"),
       "api_icm_id": os.getenv("APP2_API_ICM_ID")
    },
    {
       "app_name": os.getenv("APP3_Name"),
       "api_ga4_url": os.getenv("APP3_API_GA4_URL"),
       "api_ga4_code": os.getenv("APP3_API_GA4_CODE"),
       "api_icm_token": os.getenv("APP3_API_ICM_TOKEN"),
       "api_icm_id": os.getenv("APP3_API_ICM_ID")
    },
    {
       "app_name": os.getenv("APP4_Name"),
       "api_ga4_url": os.getenv("APP4_API_GA4_URL"),
       "api_ga4_code": os.getenv("APP4_API_GA4_CODE"),
       "api_icm_token": os.getenv("APP4_API_ICM_TOKEN"),
       "api_icm_id": os.getenv("APP4_API_ICM_ID")
    },
    {
       "app_name": os.getenv("APP5_Name"),
       "api_ga4_url": os.getenv("APP5_API_GA4_URL"),
       "api_ga4_code": os.getenv("APP5_API_GA4_CODE"),
       "api_icm_token": os.getenv("APP5_API_ICM_TOKEN"),
       "api_icm_id": os.getenv("APP5_API_ICM_ID")
    },
]

# Database configurations remain the same
DB_CREDENTIALS = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_DATABASE"),
    "sslmode": os.getenv("DB_SSLMODE")
}

