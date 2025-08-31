#!/usr/bin/env python3
import os
import glob
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import gspread
import requests
import base64

# Google API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# owner = "AntonioHazman8855"
# repo = "Histogram-Pictures"
# path = ""   # path inside repo, leave empty for root
# url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

# ---------- Config (can be overridden by env vars in GitHub Actions) ----------
JSON_GLOB = os.environ.get("JSON_GLOB", "data/*.json")   # glob pattern to find JSON files in repo
API_URLS = os.environ.get("API_URLS", "https://api.github.com/repos/AntonioHazman8855/Histogram-Pictures/contents/")
OUTPUT_NAME = os.environ.get("OUTPUT_NAME", "processed_data.csv")
SHEET_FILE_NAME = os.environ.get("SHEET_FILE_NAME", "1Pbur3A3ClQp2BKY8Iwtmub946SA6pH3GTiSw47f3CxI")  # used for Google Sheet name (no ext)
SA_FILE = os.environ.get("SA_FILE", "sa.json")  # service account json file path (written by the workflow)
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")  # optional: put file into this folder
MAKE_PUBLIC = os.environ.get("MAKE_PUBLIC", "true").lower() in ("1", "true", "yes")
# ------------------------------------------------------------------------------

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return pd.json_normalize(data)
    # If data is dict: try to find the first list-of-dicts inside
    if isinstance(data, dict):
        list_candidates = [v for v in data.values() if isinstance(v, list)]
        for candidate in list_candidates:
            if candidate and isinstance(candidate[0], dict):
                return pd.json_normalize(candidate)
        return pd.json_normalize([data])
    return pd.DataFrame()

def find_json_files(glob_pattern):
    return [
        p for p in glob.glob(glob_pattern, recursive=True)
        if p.lower().endswith(".json") and os.path.basename(p) != os.path.basename(SA_FILE)
    ]

def fetch_json_from_api(url):
    print(f"Fetching from {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    headers = {}

    collected = []
    def normalize_json_to_df(jsondata):
        # Normalize into a dataframe consistently
        if isinstance(jsondata, list):
            return pd.json_normalize(jsondata)
        if isinstance(jsondata, dict):
            # try to find first list-of-dicts inside dict
            list_candidates = [v for v in jsondata.values() if isinstance(v, list)]
            for candidate in list_candidates:
                if candidate and isinstance(candidate[0], dict):
                    return pd.json_normalize(candidate)
            # otherwise wrap single dict
            return pd.json_normalize([jsondata])
        return pd.DataFrame()

    # Case: top-level is a list -> probably repo contents metadata
    if isinstance(data, list):
        for entry in data:
            # only files, only .json
            if entry.get("type") != "file":
                continue
            name = entry.get("name", "")
            if not name.lower().endswith(".json"):
                continue
            dl = entry.get("download_url")
            if dl:
                resp = requests.get(dl, headers=headers, timeout=30)
                resp.raise_for_status()
                try:
                    jsondata = resp.json()
                except ValueError:
                    # maybe plain text — try loads
                    jsondata = json.loads(resp.text)
            else:
                # fallback: fetch via file API url asking for raw
                api_file_url = entry.get("url")
                hdr = dict(headers)
                hdr["Accept"] = "application/vnd.github.v3.raw"
                resp = requests.get(api_file_url, headers=hdr, timeout=30)
                resp.raise_for_status()
                try:
                    jsondata = resp.json()
                except ValueError:
                    jsondata = json.loads(resp.text)

            df = normalize_json_to_df(jsondata)
            if not df.empty:
                df["_source_file"] = entry.get("path", name)
                collected.append(df)

    # Case: top-level is a dict -> could be a single file metadata or the actual JSON content
    elif isinstance(data, dict):
        # file metadata (has 'type' and 'name')
        if data.get("type") == "file" and data.get("name","").lower().endswith(".json"):
            dl = data.get("download_url")
            if dl:
                resp = requests.get(dl, headers=headers, timeout=30)
                resp.raise_for_status()
                try:
                    jsondata = resp.json()
                except ValueError:
                    jsondata = json.loads(resp.text)
                df = normalize_json_to_df(jsondata)
                if not df.empty:
                    df["_source_file"] = data.get("path", data.get("name"))
                    collected.append(df)
        else:
            # maybe the URL returned the JSON content itself (e.g., you passed raw file url)
            df = normalize_json_to_df(data)
            if not df.empty:
                collected.append(df)

    if not collected:
        return pd.DataFrame()
    return pd.concat(collected, ignore_index=True, sort=False)

def preprocess_df(main_df):
    main_df['net_amount_stay'] = main_df['net_amount_stay'] / 100
    main_df = main_df.drop(columns='id')
    
    main_df['booking_date'] = pd.to_datetime(main_df['booking_date'], dayfirst=True)
    main_df['check_in'] = pd.to_datetime(main_df['check_in'], dayfirst=True)
    main_df['check_out'] = pd.to_datetime(main_df['check_out'], dayfirst=True)

    main_df['lead_days'] = (main_df['check_in'] - main_df['booking_date']).dt.days

    main_df['booking_day'] = main_df['booking_date'].dt.day
    main_df['booking_month'] = main_df['booking_date'].dt.month
    main_df['booking_year'] = main_df['booking_date'].dt.year

    main_df['check_in_day'] = main_df['check_in'].dt.day
    main_df['check_in_month'] = main_df['check_in'].dt.month
    main_df['check_in_year'] = main_df['check_in'].dt.year
    main_df['check_in_weekday'] = main_df['check_in'].dt.day_name() 

    main_df['check_out_day'] = main_df['check_out'].dt.day
    main_df['check_out_month'] = main_df['check_out'].dt.month
    main_df['check_out_year'] = main_df['check_out'].dt.year

    main_df['stay_days'] = (main_df['check_out'] - main_df['check_in']).dt.days
    main_df['price_per_night'] = main_df['net_amount_stay'] / main_df['stay_days']

    no_net = []
    for i in range(len(main_df['net_amount_stay'])):
      if main_df.iloc[i, 5] == 0:
          no_net.append(0)
      else:
          no_net.append(1)
    main_df['net_amount_avail'] = no_net
    main_df['is_confirmed'] = main_df['is_confirmed'].replace({'t': True, 'f': False})
    main_df = main_df.loc[(main_df['lead_days'] >= 0) & 
                       (main_df['net_amount_avail'] == 1) &
                       (main_df['price_per_night'] <= 18000000), :]
    return main_df
    
def upload_csv_to_sheet(sa_file, local_csv_path, sheet_id, worksheet_name="Sheet1"):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = service_account.Credentials.from_service_account_file(sa_file, scopes=SCOPES)
    client = gspread.authorize(creds)

    # Open the existing sheet
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # Create the worksheet if it doesn’t exist
        ws = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")

    df = pd.read_csv(local_csv_path)
    # Clear old contents
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}"
    print(f"✅ Uploaded CSV to Google Sheet: {sheet_url}")
    return sheet_url

    
# def upload_csv_to_drive(sa_file, local_csv_path, sheet_name, folder_id=None, make_public=True):
#     SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
#     creds = service_account.Credentials.from_service_account_file("sa.json", scopes=SCOPES)
#     drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)

#     # If a file with same name exists, delete it first (keeps things simple)
#     q = f"name = '{sheet_name}' and trashed = false"
#     res = drive_service.files().list(q=q, fields="files(id, name, mimeType)").execute()
#     for f in res.get("files", []):
#         try:
#             drive_service.files().delete(fileId=f["id"]).execute()
#             print(f"Deleted old file {f['name']} ({f['id']})")
#         except Exception as e:
#             print("Warning: couldn't delete old file:", e)

#     # Upload CSV and convert to Google Sheet
#     file_metadata = {'name': sheet_name, 'mimeType': 'application/vnd.google-apps.spreadsheet'}
#     if folder_id:
#         file_metadata['parents'] = [folder_id]

#     media = MediaFileUpload(local_csv_path, mimetype='text/csv', resumable=True)
#     file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#     file_id = file.get('id')
#     print("Uploaded and converted file id:", file_id)

#     # Set permission so Looker Studio can access. If you prefer privacy, skip this and share manually.
#     # if make_public:
#     #     try:
#     #         drive_service.permissions().create(
#     #             fileId=file_id,
#     #             body={'type': 'anyone', 'role': 'reader'},
#     #             fields='id',
#     #         ).execute()
#     #         print("Set file to 'anyone with link' reader")
#     #     except Exception as e:
#     #         print("Warning: couldn't set public permission:", e)

#     sheet_url = f"https://docs.google.com/spreadsheets/d/{file_id}"
#     return sheet_url

# def main():
#     print("Finding JSON files with pattern:", JSON_GLOB)
#     files = find_json_files(JSON_GLOB)
#     if not files:
#         print("No JSON files found — exiting.")
#         sys.exit(1)
#     print(f"Found {len(files)} JSON files. Example: {files[:5]}")

#     dfs = []
#     for p in files:
#         print("Loading:", p)
#         try:
#             dfi = load_json_file(p)
#             if dfi.empty:
#                 print(" -> produced empty dataframe (skipping).")
#                 continue
#             # add provenance column
#             dfi['_source_file'] = os.path.basename(p)
#             dfs.append(dfi)
#         except Exception as e:
#             print("Error loading", p, e)

#     if not dfs:
#         print("No dataframes to concat — exiting.")
#         sys.exit(1)

#     df = pd.concat(dfs, ignore_index=True, sort=False)
#     print("Combined dataframe shape:", df.shape)

#     df = preprocess_df(df)
#     print("After preprocess shape:", df.shape)

#     # Write CSV to local file
#     out_path = OUTPUT_NAME
#     df.to_csv(out_path, index=False)
#     print("Wrote CSV to", out_path)

#     # Upload to Drive (convert to Google Sheet)
#     if not os.path.exists(SA_FILE):
#         print("Service account file not found:", SA_FILE)
#         print("Make sure the Actions workflow writes the SA JSON to this path.")
#         sys.exit(1)

#     # sheet_url = upload_csv_to_sheet(SA_FILE, out_path, SHEET_FILE_NAME, folder_id=DRIVE_FOLDER_ID or None, make_public=MAKE_PUBLIC)
#     sheet_url = upload_csv_to_sheet(SA_FILE, out_path, "1Pbur3A3ClQp2BKY8Iwtmub946SA6pH3GTiSw47f3CxI", worksheet_name="Sheet1")
#     print("Sheet created at:", sheet_url)

def main():
    dfs = []
    if API_URLS:
        urls = [u.strip() for u in API_URLS.split(",") if u.strip()]
        for url in urls:
            try:
                dfi = fetch_json_from_api(url)
                if not dfi.empty:
                    dfi["_source_api"] = url
                    dfs.append(dfi)
            except Exception as e:
                print("Error fetching", url, e)
    else:
        print("Finding JSON files with pattern:", JSON_GLOB)
        files = find_json_files(JSON_GLOB)
        if not files:
            print("No JSON files found — exiting.")
            sys.exit(1)
        for p in files:
            print("Loading:", p)
            try:
                dfi = load_json_file(p)
                if not dfi.empty:
                    dfi["_source_file"] = os.path.basename(p)
                    dfs.append(dfi)
            except Exception as e:
                print("Error loading", p, e)

    if not dfs:
        print("No data to process — exiting.")
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True, sort=False)
    print("Combined dataframe shape:", df.shape)
    df = preprocess_df(df)
    print("After preprocess shape:", df.shape)
    out_path = OUTPUT_NAME
    df.to_csv(out_path, index=False)
    print("Wrote CSV to", out_path)

    if not os.path.exists(SA_FILE):
        print("Service account file not found:", SA_FILE)
        sys.exit(1)

    sheet_url = upload_csv_to_sheet(SA_FILE, out_path, "1Pbur3A3ClQp2BKY8Iwtmub946SA6pH3GTiSw47f3CxI", worksheet_name="Sheet1")
    print("Sheet created at:", sheet_url)

if __name__ == "__main__":
    main()
