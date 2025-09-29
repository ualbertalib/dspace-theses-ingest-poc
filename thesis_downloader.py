#!/usr/bin/env python3
import csv
import os
import sqlite3
import requests
import re
import getpass
import hashlib
from requests.auth import HTTPBasicAuth

# ========================
# Configurable variables
# ========================
CSV_FILE = r"/full/path/to/csvfile.csv"          # path to your CSV file
DB_FILE = r"/full/path/to/thesis_data.sqlite"  # SQLite database filename
DOWNLOAD_DIR = r"/download/directory"         # folder to store downloaded PDFs
OFFSET = 0                      # starting row offset
LIMIT = 10                      # number of rows to process at a time
# ========================

# Alfresco credentials (read from env for security)
USERNAME = "username"
PASSWORD = getpass.getpass("Enter Alfresco password: ")
# ========================


def normalize_header(header: str) -> str:
    """Convert column names to lowercase_with_underscores"""
    header = header.strip().lower()
    header = re.sub(r"\s+", "_", header)  # replace spaces with underscores
    return header


def init_db(conn, headers):
    """Create table if it does not exist"""
    cols = []
    for h in headers:
        if h == "dbid":
            cols.append("dbid TEXT PRIMARY KEY")
        else:
            cols.append(f"{h} TEXT")
    # Add extra MD5_Valid field
    cols.append("md5_valid INTEGER")
    columns = ", ".join(cols)
    conn.execute(f"CREATE TABLE IF NOT EXISTS thesis ({columns});")
    conn.commit()


def save_to_db(conn, headers, row, md5_valid):
    """Insert a row into the database with MD5_Valid check"""
    headers_with_md5 = headers + ["md5_valid"]
    placeholders = ", ".join(["?"] * len(headers_with_md5))
    sql = f"INSERT OR REPLACE INTO thesis ({', '.join(headers_with_md5)}) VALUES ({placeholders})"
    values = [row.get(h, "") for h in headers] + [md5_valid]
    conn.execute(sql, values)


def download_pdf(url, filename):
    """Download PDF file from given URL with Basic Auth"""
    try:
        response = requests.get(url, stream=True, timeout=30, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        response.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"[X] Failed to download {url}: {e}")
        return False


def calculate_md5(filepath):
    """Compute MD5 hash of a file"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        # Normalize headers
        headers = [normalize_header(h) for h in reader.fieldnames]

        # Connect to SQLite
        conn = sqlite3.connect(DB_FILE)
        init_db(conn, headers)

        # Get total number of rows minues the header
        total = sum(1 for _ in open(CSV_FILE, encoding="utf-8")) - 1
        csvfile.seek(0)  # reset file pointer
        reader = csv.DictReader(csvfile)

        for idx, row in enumerate(reader):
            if idx < OFFSET:
                continue
            if idx >= OFFSET + LIMIT:
                break

            # convert row keys to match db headers
            normalized_row = {normalize_header(k): v for k, v in row.items()}

            # Download PDF
            url = normalized_row.get("download_link", "").strip()
            safe_name = normalized_row.get("dbid") or f"file_{idx}"
            filename = os.path.join(DOWNLOAD_DIR, f"{safe_name}.pdf")
            md5_valid = 0

            if url:
                if not os.path.exists(filename):
                    success = download_pdf(url, filename)
                else:
                    success = True

                if success and os.path.exists(filename):
                    expected_md5 = (normalized_row.get("md5") or "").lower()
                    actual_md5 = calculate_md5(filename).lower()
                    if expected_md5 and expected_md5 == actual_md5:
                        md5_valid = 1
                        print(f"[OK] {filename} MD5 valid")
                    else:
                        print(f"[!] {filename} MD5 mismatch (expected {expected_md5}, got {actual_md5})")

            # Save to database with md5_valid flag
            save_to_db(conn, headers, normalized_row, md5_valid)

        conn.commit()
        conn.close()
        print("✅ Processing complete.")


if __name__ == "__main__":
    main()