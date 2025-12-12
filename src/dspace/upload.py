
from pprint import pprint

import json
import os
import sys
import csv

from removeResourcePolicy import remove_resource_policy 

from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Community, Collection, Item, Bundle, Bitstream, ResourcePolicy
from datetime import datetime


CONFIG_FILE = "config.json"

with open(CONFIG_FILE) as f:
        config = json.load(f)
    
USERNAME = config["username"]
PASSWORD = config["password"]
URL = config.get("base_url", "https://ualberta-dev.scholaris.ca/server/api")
COLLECTION_ID = config.get("collection_id")
CSV_FILE = config.get("csv_file") 
PDF_PATH = config.get("pdf_path", "./thesis/")

GROUPUUID = "afad3921-4e86-4672-bd79-8205d5f27539"  # Anonymous users group UUID, Was retrieved from HAL browser: https://ualberta-dev.scholaris.ca/server/

def convert_language_code(lang_code):
    if lang_code == "eng":
        return "en"
    if lang_code == "fra":
        return "fr"
    return lang_code


def convertDate(date_str):      
    try:
        """Convert MM/DD/YYYY → YYYY-MM-DD"""
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None



def create_item_data(thesis_row):
    lang = convert_language_code(thesis_row["Language"])
    
    
    
    """Create item data dictionary from thesis row."""
    item_data = {
        "name": "Thesis Item",
        "metadata": {
             "dc.contributor.author": [
            {
                "value": thesis_row["Author"],          
                "authority": None,
                "confidence": -1
            },
            ],
            "dc.title": [
            {   
                "value": thesis_row["Title"],                
                "authority": None,
                "confidence": -1
            }
            ],
            "dc.title.alternative": [
            {   
                "value": thesis_row["Other Titles"],                
                "authority": None,
                "confidence": -1
            }
            ],
            "dc.language.iso": [
            {
                "value": lang,             
                "authority": None,
                "confidence": -1
            }
            ],
            # split piped Subject values into multiple dc.subject entries
            "dc.subject": [
            {
                "value": s.strip(),
                "language": None,
                "authority": None,
                "confidence": -1
            } for s in (thesis_row.get("Keywords", "") or "").split("|") if s.strip()
            ],
                                 
            "dc.description.abstract": [
            {
                "value": thesis_row["Abstract"],
                "language": lang,
                "authority": None,
                "confidence": -1
            }
            ],
             "dc.relation.isversionof": [
            {
                "value": "",                
                "authority": None,
                "confidence": -1
            }
            ],
           
            "thesis.degree.level": [
            {
                "value": thesis_row["Degree Level"],  
                "authority": None,
                "confidence": -1
            }
            ],
             "thesis.degree.name": [
            {
                "value": thesis_row["Degree"],  
                "authority": None,
                "confidence": -1
            }
            ],
             "ual.department": [
            {
                "value": thesis_row["Department"],  
                "authority": None,
                "confidence": -1
            }
            ],
            "thesis.degree.discipline": [
            {
                "value": thesis_row["Specialization"],  
                "authority": None,
                "confidence": -1
            }
            ],
            "ual.date.graduation": [
            {
                "value": convertDate(thesis_row["Graduation Date"]),  
                "authority": None,
                "confidence": -1
            }
            ],
            "dc.date.issued": [
            {
                "value": convertDate(thesis_row["Date of Embargo"]),  
                "authority": None,
                "confidence": -1
            }
            ],
            "dc.contributor.advisor": [
            {
                "value": thesis_row["Supervisor Info"],  
                "authority": None,
                "confidence": -1
            }
            ],

            
            "dc.rights.uri": [
            {
                "value": "",  # ???? Not sure what this maps to
                "language": None,
                "authority": None,
                "confidence": -1
            }
            ],
            "dc.rights": [
            {
                "value": "This thesis is made available by the University of Alberta Library with permission of the copyright owner solely for non-commercial purposes. This thesis, or any portion thereof, may not otherwise be copied or reproduced without the written consent of the copyright owner, except to the extent permitted by Canadian copyright law.",  # ???? Not sure what this maps to
                "language": None,
                "authority": None,
                "confidence": -1
            }
            ],
            "local.embargo.terms": [
            {
                "value": convertDate(thesis_row["Date of Embargo"]),
                "confidence": -1
            }
            ],
          
            "ual.owner": [
            {
                "value": thesis_row["Email"],
                "authority": None,
                "confidence": -1
            }
            ],
             "dc.date.submitted": [
            {
                "value": convertDate(thesis_row["Submitted Date"]),
                "authority": None,
                "confidence": -1
            }
            ],
            "dcterms.dateAccepted": [
            {
                "value": convertDate(thesis_row["Approved Date"]),
                "authority": None,
                "confidence": -1
            }
            ],           
             "dc.type": [
            {
                "value": "http://purl.org/coar/resource_type/c_46ec",          # Confirm what this is      
                "authority": None,
                "confidence": -1
            }
            ],
             "thesis.degree.grantor": [
            {
                "value": "University of Alberta",   # Will this always be the same?
                "authority": None,
                "confidence": -1
            }
            ],


        },
        "inArchive": True,
        "discoverable": True,
        "withdrawn": False,
        "type": "item"

        }
      
    return item_data 


# The data was based on the hal browser here: https://ualberta-dev.scholaris.ca/server/#https://ualberta-dev.scholaris.ca/server/api/authz/resourcepolicies/1716271

def set_resource_policies(thesis_row):
    # Convert MM/DD/YYYY → YYYY-MM-DD
    embargo_date = convertDate(thesis_row.get("Date of Embargo") or "")   # example: "2027-01-01"
    rp = ResourcePolicy(api_resource={})
    rp.name = None
    rp.description = None
    rp.policyType = "TYPE_CUSTOM"
    rp.action = "READ"
    rp.startDate = (embargo_date + "T07:00:00Z") if embargo_date else None
    rp.endDate = None
    return rp




def read_csv_file(csv_path):
    """Read thesis data from CSV file."""
    print (f"Reading CSV file: {csv_path}")
    theses = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            theses.append(row)
    return theses




# Instantiate DSpace client

client = DSpaceClient(api_endpoint=URL, username=USERNAME, password=PASSWORD, fake_user_agent=True)

# Authenticate against the DSpace client
authenticated = client.authenticate()
if not authenticated:
    print('Error logging in! Giving up.')
    sys.exit(1)


theses = read_csv_file(CSV_FILE)
#print(f"Found {len(theses)} theses to process")

# Process each thesis

for i, thesis_row in enumerate(theses, 1):
    print(f"\n{'='*60}")
    print(f"Processing thesis {i} of {len(theses)}")
    print(f"{'='*60}")
    
    item_data = create_item_data(thesis_row)

    print ("Creating item...\n")
    
    # Instantiate an empty Item, passing the full API resource data to the constructor.
    item = Item(item_data)
    try:
        new_item = client.create_item(parent=COLLECTION_ID, item=item)

        if isinstance(new_item, Item) and new_item.uuid is not None:
            print(f'New item created! Handle: {new_item.handle}')
        else:
            print('Error! Giving up.')   


        #Create Bundle and upload item

        new_bundle = client.create_bundle(parent=new_item, name='ORIGINAL')               
        
        if isinstance(new_bundle, Bundle) and new_bundle.uuid is not None:
            print(f'New bundle created! UUID: {new_bundle.uuid}')
        else:
            print('Error! Giving up.')
            sys.exit(1)

        # Create and upload a new bitstream
        # Set bitstream metadata
        # See https://github.com/DSpace/RestContract/blob/main/bitstreams.md
        bitstream_metadata = {
            'dc.description':
                [{"value": "", 'language': 'en',
                'authority': None, 'confidence': -1, 'place': 0}]
        }

        # Set the mime type (using mimetypes.guess_type is recommended for real uploads if you
        # don't want to set manually)
        FILE_MIME = thesis_row["File Type"]      
        # Create the bitstream and upload the file
        print ("Uploading bitstream...", thesis_row["filename"])
        new_bitstream = client.create_bitstream(
            bundle=new_bundle,
            name=thesis_row["filename"],
            path=os.path.join(PDF_PATH, thesis_row["filename"]),
            mime=FILE_MIME,
            metadata=bitstream_metadata,
        )
        

        resourcePolicies = client.get_resource_policies_iter(parent=new_item.uuid,action="READ")

               

        print("Resource policies on item before adding embargo:")      
        for policy in resourcePolicies:
            print("Policy ID:", policy.id)
            # Run the remove policy function here
            # Note that dSpace returns a 201 code which the client library treats as an error. But it's not really an error.
            remove_resource_policy(policy.id, session_obj=client.session, base_url=URL)


        print("Creating embargo resource policy...  =============================\n")

        

        policy_data = set_resource_policies(thesis_row)     
        
        created_policy = client.create_resource_policy(resource_policy=policy_data,parent=new_item.uuid,group=GROUPUUID)
        

    except Exception as e:
        print("❌ Exception while creating item:")
        pprint(e)
        # If the exception has a response body (e.g., from requests), show it:
        if hasattr(e, 'response') and e.response is not None:
            print("\nHTTP status:", e.response.status_code)
            try:
                print("Response body:")
                pprint(e.response.json())
            except Exception:
                print(e.response.text)
        fail_count += 1
