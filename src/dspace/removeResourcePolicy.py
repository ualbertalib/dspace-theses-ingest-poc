import requests
import json
from pathlib import Path

 



def remove_resource_policy(policy_id: str, session_obj: requests.Session = None, base_url: str = None) -> bool:
    """Remove a resource policy by id using the provided session and base_url.

    If session_obj is omitted the module-level `session` and `BASE_URL` are used.
    """
    s = session_obj or session
    base = base_url or globals().get("BASE_URL")
    url = f"{base}/authz/resourcepolicies/{policy_id}"
    try:
        resp = s.delete(url)
        resp.raise_for_status()
        print(f"Resource policy {policy_id} removed successfully.")
        return True
    except requests.RequestException as e:
        print(f"Failed to remove resource policy {policy_id}: {e}")
        try:
            print("Response body:", resp.text if resp is not None else "No response")
        except Exception:
            pass
        return False
    
