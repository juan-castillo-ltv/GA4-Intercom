import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv("config.env")
load_dotenv()

UPDATE_INTERVAL = os.getenv("UPDATE_INTERVAL")
TIME_DELAY = os.getenv("TIME_DELAY")
OFFSET_BT_SCRIPTS = os.getenv("OFFSET_BT_SCRIPTS")
OAUTH_FILE = os.getenv("OAUTH_FILE")

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

