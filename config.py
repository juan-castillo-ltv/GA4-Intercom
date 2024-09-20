import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv("config.env")
load_dotenv()

UPDATE_INTERVAL = os.getenv("UPDATE_INTERVAL")
TIME_DELAY = os.getenv("TIME_DELAY")
OFFSET_BT_SCRIPTS = os.getenv("OFFSET_BT_SCRIPTS")
GA4_OAUTH = os.getenv("GA4_OAUTH")

# Configurations for multiple apps
APPS_CONFIG = [
    {
      "app_name": os.getenv("APP1_Name"),
      "api_ga4_url": os.getenv("APP1_API_GA4_URL"),
      "api_ga4_code": os.getenv("APP1_API_GA4_CODE"),
      "api_icm_token": os.getenv("APP1_API_ICM_TOKEN"),
      "api_icm_id": os.getenv("APP1_API_ICM_ID"),
      "app_google_ads_id": os.getenv("PC_GOOGLE_ADS_ID"),
      "app_user_list": os.getenv("PC_USER_LIST"),
      "app_lost_user_list": os.getenv("PC_LOST_USER_LIST"),
      "brevo_active_list": os.getenv("APP1_BREVO_ACTIVE_LIST_ID") ,
      "brevo_paid_list": os.getenv("APP1_BREVO_PAID_LIST_ID"),
      "brevo_free_list": os.getenv("APP1_BREVO_FREE_LIST_ID"),
      "brevo_inactive_list": os.getenv("APP1_BREVO_INACTIVE_LIST_ID")
    },
    {
      "app_name": os.getenv("APP2_Name"),
      "api_ga4_url": os.getenv("APP2_API_GA4_URL"),
      "api_ga4_code": os.getenv("APP2_API_GA4_CODE"),
      "api_icm_token": os.getenv("APP2_API_ICM_TOKEN"),
      "api_icm_id": os.getenv("APP2_API_ICM_ID"),
      "app_google_ads_id": os.getenv("ICU_GOOGLE_ADS_ID"),
      "app_user_list": os.getenv("ICU_USER_LIST"),
      "app_lost_user_list": os.getenv("ICU_LOST_USER_LIST"),
      "brevo_active_list": os.getenv("APP2_BREVO_ACTIVE_LIST_ID"),
      "brevo_paid_list": os.getenv("APP2_BREVO_PAID_LIST_ID"),
      "brevo_free_list": os.getenv("APP2_BREVO_FREE_LIST_ID"),
      "brevo_inactive_list": os.getenv("APP2_BREVO_INACTIVE_LIST_ID")
    },
    {
      "app_name": os.getenv("APP3_Name"),
      "api_ga4_url": os.getenv("APP3_API_GA4_URL"),
      "api_ga4_code": os.getenv("APP3_API_GA4_CODE"),
      "api_icm_token": os.getenv("APP3_API_ICM_TOKEN"),
      "api_icm_id": os.getenv("APP3_API_ICM_ID"),
      "app_google_ads_id": os.getenv("TFX_GOOGLE_ADS_ID"),
      "app_user_list": os.getenv("TFX_USER_LIST"),
      "brevo_active_list": os.getenv("APP3_BREVO_ACTIVE_LIST_ID"),
      "brevo_paid_list": os.getenv("APP3_BREVO_PAID_LIST_ID"),
      "brevo_free_list": os.getenv("APP3_BREVO_FREE_LIST_ID"),
      "brevo_inactive_list": os.getenv("APP3_BREVO_INACTIVE_LIST_ID")
    },
    {
      "app_name": os.getenv("APP4_Name"),
      "api_ga4_url": os.getenv("APP4_API_GA4_URL"),
      "api_ga4_code": os.getenv("APP4_API_GA4_CODE"),
      "api_icm_token": os.getenv("APP4_API_ICM_TOKEN"),
      "api_icm_id": os.getenv("APP4_API_ICM_ID"),
      "app_google_ads_id": os.getenv("COD_GOOGLE_ADS_ID"),
      "app_user_list": os.getenv("COD_USER_LIST"),
      "brevo_active_list": os.getenv("APP4_BREVO_ACTIVE_LIST_ID"),
      "brevo_paid_list": os.getenv("APP4_BREVO_PAID_LIST_ID"),
      "brevo_free_list": os.getenv("APP4_BREVO_FREE_LIST_ID"),
      "brevo_inactive_list": os.getenv("APP4_BREVO_INACTIVE_LIST_ID")
    },
    {
      "app_name": os.getenv("APP5_Name"),
      "api_ga4_url": os.getenv("APP5_API_GA4_URL"),
      "api_ga4_code": os.getenv("APP5_API_GA4_CODE"),
      "api_icm_token": os.getenv("APP5_API_ICM_TOKEN"),
      "api_icm_id": os.getenv("APP5_API_ICM_ID"),
      "app_google_ads_id": os.getenv("COD_GOOGLE_ADS_ID"),
      "app_user_list": os.getenv("COD_USER_LIST"),
      "brevo_active_list": os.getenv("APP5_BREVO_ACTIVE_LIST_ID"),
      "brevo_paid_list": os.getenv("APP5_BREVO_PAID_LIST_ID"),
      "brevo_free_list": os.getenv("APP5_BREVO_FREE_LIST_ID"),
      "brevo_inactive_list": os.getenv("APP5_BREVO_INACTIVE_LIST_ID")
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

LTV_SAAS_GOOGLE_ADS_ID = os.getenv("LTV_SAAS_GOOGLE_ADS_ID")

GOOGLE_ADS_CONFIG = os.getenv("GOOGLE_ADS_CONFIG")

TFX_META_APP_ID = os.getenv('TFX_META_APP_ID')
TFX_META_APP_SECRET = os.getenv('TFX_META_APP_SECRET')
TFX_META_LONG_LIVED_TOKEN = os.getenv('TFX_META_LONG_LIVED_TOKEN')
TFX_META_AD_ACCOUNT_ID = os.getenv('TFX_META_AD_ACCOUNT_ID')
TFX_META_CUSTOM_AUDIENCE_ID = os.getenv('TFX_META_CUSTOM_AUDIENCE_ID')

BREVO_API_TOKEN = os.getenv("BREVO_API_TOKEN")

