# XXX Remember to load the environment variables before running this script

import logging
# Load and configure logging module first so it does not get configured by other imports
logging.basicConfig(filename='checksum.log', encoding='utf-8', level=logging.DEBUG)
from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Community, Collection, Item, Bundle, Bitstream
import base64
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

def get_one_item(identifier, dspace_client):
  try:
      return dspace_client.get_item(identifier)
  except Exception as e:
      print(f"Error fetching item {identifier}: {e}")
      return None

def get_one_real_item(identifier, dspace_client):
  iterable = dspace_client.search_objects_iter(query=f"{identifier}", dso_type="item")
  next_iterable = next(iterable, None)
  return next_iterable

def write_report(report_name, report_data):
    with open(report_name, 'w', newline='') as csvfile:
      # We need to add this if statement checking if report_data is not empty
      # because the headers are saved in the directory keys.
      if len(report_data) > 0:
        fieldnames = report_data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in report_data:
            writer.writerow(row)

def prepare_mashed_jupiter_dpsace_checksum():
  jupiter_dspace_checksum = {}
  with open('thumbnail_checksum.csv') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
      b64_checksum = row['checksum']
      raw_bytes = base64.b64decode(b64_checksum)
      hex_checksum = raw_bytes.hex()

      if row['entity_id'] not in jupiter_dspace_checksum:
        jupiter_dspace_checksum[row['entity_id']] = {}
      jupiter_dspace_checksum[row['entity_id']]['checksum'] = hex_checksum

  with open('jupiter_dspace_mapping.csv') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
      if row['jupiter uuid'] not in jupiter_dspace_checksum:
          jupiter_dspace_checksum[row['jupiter uuid']] = {}
      jupiter_dspace_checksum[row['jupiter uuid']]['handle'] = row['scholaris handle']

  
  # Remove duplicate dois item ids
  with open('Duplicate Item_Thesis DOIs - duplicated_doi.csv') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
      item_id = row['item id']
      if item_id in jupiter_dspace_checksum:
        del jupiter_dspace_checksum[item_id]

  # Remove known problems
  with open('known_problems_annotated.csv') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
      item_id = row['entity_id']
      if item_id in jupiter_dspace_checksum:
        del jupiter_dspace_checksum[item_id]

  # prep checksum
  return jupiter_dspace_checksum

mashed_table = prepare_mashed_jupiter_dpsace_checksum()


# Provide report for 3 cases:
# + Both checksum and handle are available
# + Only checksum is available
# + Only handle is available

both_values = []
only_checksum = []
only_handle = []

for entity_id, mappings in mashed_table.items():
   if 'checksum' in mappings and 'handle' in mappings:
     both_values.append({'entity_id': entity_id, 'checksum': mappings['checksum'], 'handle': mappings['handle']})
   elif 'checksum' in mappings:
     only_checksum.append({'entity_id': entity_id, 'checksum': mappings['checksum']})
   elif 'handle' in mappings:
     only_handle.append({'entity_id': entity_id, 'handle': mappings['handle']})

print(f"Total with both checksum and handle: {len(both_values)}")
print(f"Total with only checksum: {len(only_checksum)}")
print(f"Total with only handle: {len(only_handle)}")

write_report('only_checksum.csv', only_checksum)
write_report('only_handle.csv', only_handle)

# At this point ``mashed_table`` has been prepared with checksums and handles.
# We can now proceed to verify the checksums in DSpace for the primary files.

with open('new_primary_files.csv', 'w', newline='', buffering=1) as csvfile:
  headers = ['entity_id', 'dspace_item_id', 'dspace_handle', 'current_primary', 'new_primary']
  csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
  csv_writer.writeheader()

  for count, entity_id in enumerate(mashed_table, start = 1):    
    # refresh auth token
    if count % DSPACE_CLIENT_TOKEN_REFRESH == 0:
      # not sure if both are needed
      dspace_client.authenticate()
      dspace_client.refresh_token()

    checksum, handle = mashed_table[entity_id].values()
    item = dspace_client.resolve_identifier_to_dso(handle)

    logger.info(f"Checking entity_id: {entity_id} ( {count}  ) handle: {handle}")
    print("Checking entity_id:", entity_id, "(", count, ") handle:", handle)

    bundles = dspace_client.get_bundles(item)
    for bundle in bundles:
      bundle_dict = bundle.as_dict()
      if bundle_dict.get('name') == 'ORIGINAL':

        primary_bitstream = bundle.links.get('primaryBitstream')

        primary_file = dspace_client.fetch_resource(primary_bitstream['href'])
        dspace_checksum = None

        # If primary_file is None, then there is no primary file set for the item. Thumbnails will not be generated in
        # DSpace
        if not primary_file is None:
          dspace_checksum = primary_file['checkSum']['value']

        if checksum != dspace_checksum:
          print(f"Checksum8 mismatch for entity_id {entity_id}:")

          bitstreams = dspace_client.get_bitstreams(bundle=bundle)

          for bitstream_obj in bitstreams:
            if bitstream_obj.checkSum['value'] == checksum:
              print("Found matching checksum in another bitstream (as object)!")
              csv_writer.writerow({'entity_id': entity_id, 'dspace_item_id': item.id, 'dspace_handle': handle, 'current_primary': primary_bitstream['href'], 'new_primary': bitstream_obj.links['self']['href']})

              break

        else:
          print(f"Checksum match for entity_id {entity_id}")
    