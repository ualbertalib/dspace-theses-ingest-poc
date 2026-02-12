# Extend work from test_script_update_bundle.py

import logging
from urllib import response
logging.basicConfig(filename='fix_primary_filename.log', encoding='utf-8', level=logging.DEBUG)
from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Community, Collection, Item, Bundle, Bitstream
import time
import csv
import os

DSPACE_API_ENDPOINT = 'http://localhost:8080/server/api'
DSPACE_API_USERNAME = 'username@ualberta.ca'
DSPACE_API_PASSWORD = 'PASSWORD'
DSPACE_CLIENT_TOKEN_REFRESH = 500

# Configuration from environment variables
URL = os.environ.get('DSPACE_API_ENDPOINT', DSPACE_API_ENDPOINT)
USERNAME = os.environ.get('DSPACE_API_USERNAME', DSPACE_API_USERNAME)
PASSWORD = os.environ.get('DSPACE_API_PASSWORD', DSPACE_API_PASSWORD)

dspace_client = DSpaceClient(api_endpoint=URL, username=USERNAME, password=PASSWORD, fake_user_agent=True)
dspace_client.authenticate()
logger = logging.getLogger(__name__)


with open('data/new_primary_files.csv') as csvfile:
  reader = csv.DictReader(csvfile)
  for count, row in enumerate(reader, start=1):
    print(count, row['current_primary'], row['new_primary'])

    put_url = row['current_primary']
    patch_value = row['new_primary']

    # refresh auth token
    if count % DSPACE_CLIENT_TOKEN_REFRESH == 0:
      dspace_client.authenticate()
      dspace_client.refresh_token()

    response = dspace_client.session.put( # We do this for updating existing primary files
    # response = dspace_client.session.post( # We do this for non existent primary files
      url=put_url,
      headers={"Content-Type": "text/uri-list"},
      data=patch_value
    )

    if response.ok:
      print(f"✅ Primary bitstream updated successfully for bundle {put_url} with {patch_value}")
      logger.info(f"Primary bitstream updated successfully for bundle {put_url} with {patch_value}")
    else:
      print(f"❌ Error {response.status_code}: {response.text}")
      logger.error(f"Error {response.status_code}: {response.text}")
