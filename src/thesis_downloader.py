#!/usr/bin/env python3
import configparser
import os
import csv
import requests
import re
import getpass
import hashlib
from requests.auth import HTTPBasicAuth

config = configparser.ConfigParser()
read_files = config.read('config.ini')
if not read_files:
    raise FileNotFoundError("config.ini not found")

section = config['thesis']

# Configurable variables
# ========================
CSV_FILE = os.path.expanduser(section.get('csv_file'))
OUTPUT_CSV = os.path.expanduser(section.get('output_csv'))
DOWNLOAD_DIR = os.path.expanduser(section.get('download_dir'))
OFFSET = section.getint('offset', fallback=0)
LIMIT = section.getint('limit', fallback=0) # 0 means no limit

# Alfresco credentials (prompt for username and password)
USERNAME = input("Enter Alfresco username: ")
PASSWORD = getpass.getpass("Enter Alfresco password: ")
# ========================

# Degree and language maps (added from user)
degree_map = {
    'Master of Nursing': "MN",
    'Master of Arts': "MA",
    'Master of Laws': "LLM",
    'Doctor of Education': "DEd",
    'Doctor of Music': "DM",
    'Master of Arts/Master of Library and Information Studies': "MAMLIS",
    'Doctor of Philosophy': "PhD",
    'Master of Science': "MSc",
    'Master of Education': "MEd"
}
language_map = {'eng': "english", 'fre': "french"}






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
        headers = reader.fieldnames
        total = sum(1 for _ in open(CSV_FILE, encoding="utf-8")) - 1
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)

        with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as outcsv:
            writer = csv.writer(outcsv)
            writer.writerow(["filename"] + headers + ["md5_valid"])

            for idx, row in enumerate(reader):
                
                if idx < OFFSET:
                    continue
                if LIMIT > 0:
                    if idx >= OFFSET + LIMIT:
                        break

                item_index = {h: i for i, h in enumerate(headers)}
                line = [row.get(h, "") for h in headers]
                import unidecode
                names = line[item_index.get("Author","")][:].strip()
                if ',' not in names:
                    names = names.split()
                    names.insert(0, names.pop() + ',')
                    names = " ".join(names)
                names = names.replace(',', ', ').replace('  ', ' ')
                author = names.replace(', ', ' ').replace(' ', '_').replace('.', '')
                # Date and filename logic
                submitted_date = line[item_index.get("Submitted Date","")]
                if submitted_date and '/' in submitted_date:
                    m, d, y = submitted_date.split('/')
                    degree_val = line[item_index.get('Degree','')]
                    filename = f"{author}_{y}{int(m):02d}_{degree_map.get(degree_val,'UNK')}.pdf".replace('__', '_')
                    filename = unidecode.unidecode(filename)
                    line[item_index.get("Submitted Date","")] = f"{m}/{d}/{y}"
                else:
                    filename = f"file_{idx}.pdf"
                # Approved Date
                approved_date = line[item_index.get("Approved Date","")]
                if approved_date and '/' in approved_date:
                    m, d, y = approved_date.split('/')
                    line[item_index.get("Approved Date","")] = f"{m}/{d}/{y}"
                # Date of Embargo
                embargo_date = line[item_index.get("Date of Embargo","")]
                if embargo_date and '/' in embargo_date:
                    m, d, y = embargo_date.split('/')
                    line[item_index.get("Date of Embargo","")] = f"{m}/{d}/{y}"
                # Field replacements
                if item_index.get("Abstract", None) is not None:
                    line[item_index["Abstract"]] = line[item_index["Abstract"]].replace('\\', '\\\\')
                if item_index.get("Title", None) is not None:
                    line[item_index["Title"]] = line[item_index["Title"]].replace('\\', '\\\\')
                if item_index.get("Other Titles", None) is not None:
                    line[item_index["Other Titles"]] = line[item_index["Other Titles"]].replace('\\', '\\\\')
                if item_index.get("Keywords", None) is not None:
                    line[item_index["Keywords"]] = line[item_index["Keywords"]].replace('|#|', '|').replace(', ', '|')
                if item_index.get("Supervisor Info", None) is not None:
                    line[item_index["Supervisor Info"]] = line[item_index["Supervisor Info"]].replace('|#|', '|')

                url = line[item_index.get("Download Link","")].strip() if item_index.get("Download Link","") is not None else ""
                file_path = os.path.join(DOWNLOAD_DIR, filename)
                md5_valid = 0

                if url:
                    if not os.path.exists(file_path):
                        success = download_pdf(url, file_path)
                    else:
                        success = True

                    if success and os.path.exists(file_path):
                        expected_md5 = (line[item_index.get(" MD5","")] or "").lower()
                        actual_md5 = calculate_md5(file_path).lower()
                        if expected_md5 and expected_md5 == actual_md5:
                            md5_valid = 1
                            print(f"[OK] {filename} MD5 valid")
                        else:
                            print(f"[!] {filename} MD5 mismatch (expected {expected_md5}, got {actual_md5})")

                writer.writerow([filename] + line )
        print("✅ Processing complete.")

if __name__ == "__main__":
    main()

