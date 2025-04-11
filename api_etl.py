import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from decouple import config


#CONFIGS
DATABASE_CONFIG = {
    'user': config('DB_USER'),
    'password': config('DB_PASSWORD'),
    'host': config('DB_HOST'),
    'port': config('DB_PORT'),
    'database': config('DB_NAME')
}
API_CONFIG = {
    'url': 'https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries',
    'params': {'$top': 10000, '$skip': 0}
}
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ETLLogger")

#EXTRACT
def extract_data(api_url, params):
    logger.info("Starting data ingestion.")
    all_summaries = []

    while True:
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            summaries = data.get('DisasterDeclarationsSummaries', [])

            if not summaries:
                break

            all_summaries.extend(summaries)
            logger.info(f"Fetched {len(summaries)} records.")
            params["$skip"] += params["$top"]

        except Exception as e:
            logger.error(f"Error during data ingestion: {e}")
            break

    logger.info(f"Total records fetched: {len(all_summaries)}.")
    return pd.DataFrame(all_summaries)

#TRANSFORM
def transform_data(df):
    logger.info("Starting data transformation.")
    try:
        if not df.empty:
            df['state'] = df['state'].astype('category')
            df['declarationType'] = df['declarationType'].astype('category')
            date_columns = ['declarationDate', 'incidentBeginDate', 'incidentEndDate', 'disasterCloseoutDate']
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            incident_type_mapping = {
                'Tropical Storm': '4', 'Fire': 'R', 'Severe Storm': 'W', 'Tornado': 'T',
                'Straight-Line Winds': '2', 'Mud/Landslide': 'M', 'Flood': 'F', 'Hurricane': 'H',
                'Biological': 'B', 'Winter Storm': '5', 'Snowstorm': 'S', 'Earthquake': 'E',
                'Coastal Storm': 'C', 'Other': 'Z', 'Severe Ice Storm': 'O', 'Dam/Levee Break': 'K',
                'Typhoon': 'J', 'Volcanic Eruption': 'V', 'Freezing': 'G', 'Toxic Substances': 'X',
                'Chemical': 'L', 'Terrorist': 'I', 'Drought': 'D', 'Human Cause': 'Y',
                'Fishing Losses': 'P', 'Tsunami': 'A'
            }
            df['designatedIncidentTypes'] = df['incidentType'].map(incident_type_mapping)
            df = df.drop(columns=['lastIAFilingDate'], errors='ignore')
            df.columns = df.columns.str.lower()
            return df
        else:
            logger.warning("No data to transform.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        return pd.DataFrame()

#LOAD
def load_data(df, db_config):
    if not df.empty:
        try:
            logger.info("Loading data into the database.")
            engine = create_engine(
                f"postgresql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            df.to_sql('incident_data', engine, if_exists='replace', index=False)
            logger.info(f"Successfully loaded {len(df)} rows into the database.")
        except Exception as e:
            logger.error(f"Error during data loading: {e}")
    else:
        logger.warning("No data to load.")

#ETL
def run_etl():
    logger.info("Starting the ETL process.")
    try:
        data = extract_data(API_CONFIG['url'], API_CONFIG['params'])
        cleaned_data = transform_data(data)
        load_data(cleaned_data, DATABASE_CONFIG)
        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.critical(f"ETL process failed: {e}")


if __name__ == "__main__":
    run_etl()