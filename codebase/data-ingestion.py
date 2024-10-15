import boto3
import requests
from requests.auth import HTTPBasicAuth
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from datetime import datetime, timedelta
import time
import json
import os
from threading import Thread

class APIIngestion:
    
    def __init__(self, config):
        """
        Initializes the ingestion process with a configuration dictionary.
        :param config: Dictionary containing API endpoints, authentication, and other settings.
        """
        self.config = config
        self.endpoints = config['endpoints']


    def fetch_data(self, endpoint_config, params=None):
        """
        Handle authentication for different endpoints based on their authentication methods and Fetches data from the API endpoint.
        :param endpoint_config: Dictionary containing endpoint-specific configuration.
        :param params: Optional parameters for API calls (e.g., for pagination or filtering).
        :return: Parsed API response (JSON).
        """
        url = endpoint_config['url']
        auth_type = endpoint_config['auth_type']
        auth_params = endpoint_config['auth_params']

        if auth_type == 'basic':
            response = requests.get(url, auth=HTTPBasicAuth(auth_params['username'], auth_params['password']))

        elif auth_type == 'oauth2':
            auth = HTTPBasicAuth(auth_params['client_id'], auth_params['client_secret'])
            client = BackendApplicationClient(client_id=auth_params['client_id'])
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(token_url=auth_params['token_url'], auth=auth)
            response = oauth.get(url)

        elif auth_type == 'apikey':
            headers = None
            params = None
            if "header_key" in auth_params:
                header_key = auth_params['header_key']
                token = auth_params['token']
                if header_key == 'Bearer':
                    headers = {'Authorization': f'Bearer {token}'}
                else:
                    headers = {f'{header_key}': f'{token}'}
                
            elif "query_param" in auth_params:
                query_param = auth_params['query_param']
                token = auth_params['token']
                params = {f'{query_param}': f'{token}'}
                
            response = requests.get(url, params=params, headers=headers)

        # Add more authentication methods as needed
        else:
            raise ValueError('Unsupported authentication type')
        
        # response.raise_for_status()  # Raise an exception for HTTP errors
        if response.status_code == 200:
            if endpoint_config.get('format') == 'json':
                return response.json()
            elif endpoint_config.get('format') == 'xml':
                # Implement XML parsing logic here (e.g., using xml.etree.ElementTree)
                return response.text
            # Add more format handling as needed
        else:
            print(f"Error fetching data from {url}: {response.status_code}")
            return None


    def upload_to_s3(self, data, file_name):
        """
        Upload ingested data to S3 Bucket.
        """
        S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
        # AWS S3 Client
        s3_client = boto3.client(
            's3',
            region_name=os.environ["S3_REGION"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"]
        )

        try:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data)
            )
            print(f'Successfully uploaded {file_name} to S3 bucket {S3_BUCKET_NAME}')
        except Exception as e:
            print(f'Error uploading to S3: {str(e)}')


    def process_data(self, endpoint, data):
        """
        Processes the fetched data and save to s3 bucket.
        :param endpoint: The endpoint from which data was fetched.
        :param data: Data to be processed.
        """
        # Implement data processing (e.g., saving to database, sending to message queue, etc.)
        
        # Upload data to S3
        print(f"Processing data from {endpoint}: {len(data)} records")
        file_name = f"{endpoint}/{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        self.upload_to_s3(data, file_name)
    

    def ingest_batch_data(self):
        """
        Ingests batch data from all endpoints. Can be scheduled to run periodically.
        """
        for endpoint in self.endpoints:
            endpoint_config = self.endpoints[endpoint]
            data = self.fetch_data(endpoint_config)
            if data:
                self.process_data(endpoint, data)


    def ingest_historical_data(self):
        """
        Ingests historical data with backfilling, using configurable start/end dates.
        """
        for endpoint in self.endpoints:
            endpoint_config = self.endpoints[endpoint]
            start_date = endpoint_config.get('start_date')
            end_date = endpoint_config.get('end_date', datetime.now().strftime('%Y-%m-%d'))
            
            while start_date <= end_date:
                params = {'date': start_date}
                data = self.fetch_data(endpoint_config, params=params)
                if data:
                    self.process_data(endpoint, data)
                start_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')


    def incremental_ingestion(self):
        """
        Ingests incremental data (since last run).
        """
        for endpoint in self.endpoints:
            endpoint_config = self.endpoints[endpoint]
            last_run = endpoint_config.get('last_run', datetime.now() - timedelta(days=1))
            params = {'since': last_run.strftime('%Y-%m-%dT%H:%M:%S')}
            data = self.fetch_data(endpoint_config, params=params)
            if data:
                self.process_data(endpoint, data)
                endpoint_config['last_run'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')


if __name__ == "__main__":
    # Import API configuration file
    with open("config.json", 'r') as j:
        config = json.loads(j.read())
    

    ingestion = APIIngestion(config)
    
    # Historical data ingestion
    # ingestion.ingest_historical_data()

    # Batch data ingestion
    ingestion.ingest_batch_data()

