#!/usr/bin/env python3
import os
import glob
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Google API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------- Config (can be overridden by env vars in GitHub Actions) ----------
JSON_GLOB = os.environ.get("JSON_GLOB", "data/*.json")   # glob pattern to find JSON files in repo
OUTPUT_NAME = os.environ.get("OUTPUT_NAME", "processed_data.csv")
SHEET_FILE_NAME = os.environ.get("SHEET_FILE_NAME", "processed_data")  # used for Google Sheet name (no ext)
SA_FILE = os.environ.get("SA_FILE", "sa.json")  # service account json file path (written by the workflow)
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")  # optional: put file into this folder
MAKE_PUBLIC = os.environ.get("MAKE_PUBLIC", "true").lower() in ("1", "true", "yes")
# ------------------------------------------------------------------------------

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # If data is a list of records
    if isinstance(data, list):
        return pd.json_normalize(data)
    # If data is dict: try to find the first list-of-dicts inside
    if isinstance(data, dict):
        # common patterns: {"items": [ ... ]} or {"data": [ ... ]}
        list_candidates = [v for v in data.values() if isinstance(v, list)]
        for candidate in list_candidates:
            if candidate and isinstance(candidate[0], dict):
                return pd.json_normalize(candidate)
        # fallback: make single-row df
        return pd.json_normalize([data])
    # Other types -> create empty df
    return pd.DataFrame()

def find_json_files(glob_pattern):
    return [
        p for p in glob.glob(glob_pattern, recursive=True)
        if p.lower().endswith(".json") and os.path.basename(p) != os.path.basename(SA_FILE)
    ]

def preprocess_df(main_df):
    main_df['net_amount_stay'] = main_df['net_amount_stay'] / 100
    main_df = main_df.drop(columns='id')
    
    main_df['booking_date'] = pd.to_datetime(main_df['booking_date'], dayfirst=True)
    main_df['check_in'] = pd.to_datetime(main_df['check_in'], dayfirst=True)
    main_df['check_out'] = pd.to_datetime(main_df['check_out'], dayfirst=True)

    main_df['lead_days'] = (main_df['check_in'] - main_df['booking_date']).dt.days

    # Booking date features
    main_df['booking_day'] = main_df['booking_date'].dt.day
    main_df['booking_month'] = main_df['booking_date'].dt.month
    main_df['booking_year'] = main_df['booking_date'].dt.year

    # Check-in date features
    main_df['check_in_day'] = main_df['check_in'].dt.day
    main_df['check_in_month'] = main_df['check_in'].dt.month
    main_df['check_in_year'] = main_df['check_in'].dt.year
    main_df['check_in_weekday'] = main_df['check_in'].dt.day_name() 

    # Check-out date features
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

import gspread
import pandas as pd
from google.oauth2 import service_account

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

    # Read CSV into DataFrame
    df = pd.read_csv(local_csv_path)

    # Clear old contents
    ws.clear()

    # Upload new data (including header)
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

def main():
    print("Finding JSON files with pattern:", JSON_GLOB)
    files = find_json_files(JSON_GLOB)
    if not files:
        print("No JSON files found — exiting.")
        sys.exit(1)
    print(f"Found {len(files)} JSON files. Example: {files[:5]}")

    dfs = []
    for p in files:
        print("Loading:", p)
        try:
            dfi = load_json_file(p)
            if dfi.empty:
                print(" -> produced empty dataframe (skipping).")
                continue
            # add provenance column
            dfi['_source_file'] = os.path.basename(p)
            dfs.append(dfi)
        except Exception as e:
            print("Error loading", p, e)

    if not dfs:
        print("No dataframes to concat — exiting.")
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True, sort=False)
    print("Combined dataframe shape:", df.shape)

    df = preprocess_df(df)
    print("After preprocess shape:", df.shape)

    # Write CSV to local file
    out_path = OUTPUT_NAME
    df.to_csv(out_path, index=False)
    print("Wrote CSV to", out_path)

    # Upload to Drive (convert to Google Sheet)
    if not os.path.exists(SA_FILE):
        print("Service account file not found:", SA_FILE)
        print("Make sure the Actions workflow writes the SA JSON to this path.")
        sys.exit(1)

    sheet_url = upload_csv_to_drive(SA_FILE, out_path, SHEET_FILE_NAME, folder_id=DRIVE_FOLDER_ID or None, make_public=MAKE_PUBLIC)
    print("Sheet created at:", sheet_url)

if __name__ == "__main__":
    main()
