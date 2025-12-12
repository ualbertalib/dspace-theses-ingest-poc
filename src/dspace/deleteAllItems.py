from dspace_rest_client.client import DSpaceClient
import json
import sys
import requests
from pathlib import Path
session = requests.Session()



print("\n" + "="*70)
print("   !!! WARNING !!!")
print("   This script will DELETE ALL ITEMS in the configured collection.")
print("   To proceed, remove the exit command from the code and re-run the script.")
print("="*70 + "\n")

sys.exit()




CONFIG_FILE = "config.json"

with open(CONFIG_FILE) as f:
    config = json.load(f)

USERNAME = config["username"]
PASSWORD = config["password"]
URL = config.get("base_url", "https://ualberta-dev.scholaris.ca/server/api")
COLLECTION_ID = config.get("collection_id")




def get_csrf_token():
    url = f"{URL}/security/csrf"
    try:
        r = session.get(url)
        r.raise_for_status()
        token = (r.headers.get('DSPACE-XSRF-TOKEN') or 
                 r.headers.get('dspace-xsrf-token') or 
                 r.headers.get('X-CSRF-Token'))
        print("CSRF Token Response Headers:", r.headers)
        if token:
            session.headers.update({"X-XSRF-TOKEN": token})
            print("CSRF Token Set:", token)
        else:
            print("No CSRF token found in headers.")
        return token
    except requests.RequestException as e:
        print(f"CSRF token request failed: {e}")
        return None

def authenticate(username: str, password: str) -> bool:
    get_csrf_token()
    login_url = f"{URL}/authn/login"
    payload = {
        "user": username,
        "password": password
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    if "X-XSRF-TOKEN" in session.headers:
        headers["X-XSRF-TOKEN"] = session.headers["X-XSRF-TOKEN"]
    
    session.headers.pop("Authorization", None)
    try:
        resp = session.post(login_url, data=payload, headers=headers)
        resp.raise_for_status()
        token = resp.headers.get("Authorization")
        if token:
            session.headers["Authorization"] = token
            print("Login successful. JWT token set.")
            return True
        else:
            print("XSRF Token:", session.headers.get("X-XSRF-TOKEN"))
            print("Login failed: No token returned.")
            print("Response body:", resp.text)
            return False
    except requests.RequestException as e:
        print(f"Login failed: {e}")
        print("Response body:", resp.text if resp else "No response")
        return False
    


# Initialize DSpaceClient for fetching items
client = DSpaceClient(api_endpoint=URL, username=USERNAME, password=PASSWORD, fake_user_agent=True)

if not client.authenticate():
    print("❌ Error logging in.")
    sys.exit(1)

# Manually authenticate to get Bearer token

authenticate(USERNAME, PASSWORD)



# Fetch items from the collection using DSpaceClient
items = client.search_objects(query="*:*", scope=COLLECTION_ID, dso_type="item")

print(f"Found {len(items)} items in collection.")

deleted = 0
failed = 0


for item in items:
    uuid = item.uuid
    name = item.name
    print(f"Deleting: {uuid} ({name})")

    try:
        # Direct DELETE request with manual token
        get_csrf_token()
        response = session.delete(f"{URL}/core/items/{uuid}")
        
        if response.status_code == 204:
            print("✅ Deleted successfully.")
            deleted += 1
        else:
            raise Exception(f"Unexpected status code: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Failed to delete {uuid}: {e}")
        if 'response' in locals() and response is not None:
            print("Response:", response.status_code, response.text)
        failed += 1

print(f"\nDone. Deleted: {deleted}, Failed: {failed}")