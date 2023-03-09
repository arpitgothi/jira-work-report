import argparse
import csv
from bs4 import BeautifulSoup
import os
from jira.client import JIRA
import time
from pathlib import Path
from getpass import getpass, getuser
from urllib.parse import parse_qs, urlparse
from jira.client import JIRA
import pandas as pd
from datetime import datetime as dt
from google.oauth2 import service_account
from googleapiclient.discovery import build
from requests import HTTPError

AD_USER = getuser()

JIRA_IDS = []
CSV_FILE_PATH = ""

now = time.time()
token_path = os.path.join("/Users", AD_USER, ".jira","token")
try:
    file_size = os.stat(token_path).st_size
except:
    file_size = 0
if file_size != 0:
    global AD_TOKEN
    f=open(token_path, "r")
    AD_TOKEN = f.read()
    f.close()
    print("AD Token found!\n##########")
else:
    print("AD Token")
    AD_TOKEN = getpass(prompt='Enter your AD_TOKEN: ', stream=None) 
    f = open(token_path,"w+")
    f.write(AD_TOKEN)
    f.close()


options = {'server': "https://splunk.atlassian.net"}

try:
    jira = JIRA(options=options, basic_auth=(AD_USER + '@splunk.com', AD_TOKEN))
except Exception as e:
        raise RuntimeError(f'Unable to logged in into "JIRA" ({e})')

def suffix(d):
    return 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')

def custom_strftime(format, t):
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))

def find_TO():
    query = 'project = "TO" AND filter=81746 AND issuetype = Change AND status = "NEEDS PRE-CHECK" OR status = Pre-Check AND "Change type[Dropdown]" = Impacting AND "Epic Link" = EMPTY AND component != termination'
    issues = jira.search_issues(query)
    for issue in issues:
        JIRA_IDS.append(str(issue))
        
    return JIRA_IDS

def write_csv(jira, data, update=False):
    if update == True:
        updated_row_data = {
            'Key': data[0],
            'Summary': data[1],
            'Components': data[2],
            'Assignee': data[3],
            'Reporter': data[4],
            'Status': data[5],
            'Resolution': data[6],
            'Created': data[7],
            'Updated': data[8],
            'Stack ID': data[9],
        }
        
        df=pd.read_csv(CSV_FILE_PATH)
        match_mask = df["Key"] == jira
        matching_row_index = df.index[match_mask][0]
        # Update the matching row with the new data
        df.loc[matching_row_index] = updated_row_data
        df.to_csv(CSV_FILE_PATH, index=False)


    else:
        with open(CSV_FILE_PATH, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(data)
        

def update_sheet():
    rows = []
    if os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, newline='') as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)


    sheet_name= custom_strftime('{S} %b', dt.now())
    print(sheet_name)
    
    
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # If modifying these scopes, delete the file token.json.

    creds = None
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = '1_VWMNZmrXqTqvgZTnO43nPzafTEAZRHKd2-4DSnM35o'
    service = build('sheets', 'v4', credentials=creds)
    # Call the Sheets API
    sheet = service.spreadsheets()

    body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }]
    }
    
    # Check if the sheet already exists
    try:
        sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheet_exists = any(sheet_name == sheet['properties']['title'] for sheet in sheet_metadata['sheets'])
    except HTTPError as error:
        print(f"An error occurred: {error}")
        sheet_exists = False

    if not sheet_exists:
        # Create the sheet if it doesn't exist
        try:
            response = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            print(f"Spreadsheet ID: {(response.get('spreadsheetId'))}")
        except HTTPError as error:
            print(f"An error occurred: {error}")



    range_name = f'{sheet_name}!A1:K{len(rows)}'
    body = {
        'range': range_name,
        'values': rows,
        'majorDimension': 'ROWS',
    }
    result = sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption='USER_ENTERED',
        body=body,
    ).execute()
    


    

def main():
    
    find_TO()
    global CSV_FILE_PATH
    
    CSV_FILE_PATH = "jira-work-report_" + custom_strftime('{S}_%b', dt.now()) + ".csv"

    for JIRA_ID in JIRA_IDS:
        meta_jira=jira.issue(JIRA_ID).raw["fields"]
        meta = []
        meta.append(JIRA_ID)
        meta.append(meta_jira["summary"])
        meta.append(meta_jira["components"][0]["name"] if meta_jira.get("components") else None)
        meta.append(meta_jira["assignee"]["displayName"] if meta_jira.get("assignee") else None)
        meta.append(meta_jira["reporter"]["displayName"] if meta_jira.get("reporter") else None)
        meta.append(meta_jira["status"]["name"] if meta_jira.get("status") else None)
        meta.append(meta_jira["resolution"] if meta_jira.get("resolution") else None)
        meta.append(meta_jira["created"] if meta_jira.get("created") else None)
        meta.append(meta_jira["updated"] if meta_jira.get("updated") else None)
        meta.append(', '.join(meta_jira["customfield_13602"]) if isinstance(meta_jira.get("customfield_13602"), list) else meta_jira["customfield_13602"] if meta_jira.get("customfield_13602") else None)

        
        print(meta)

    
        csv_path = Path.cwd().joinpath(CSV_FILE_PATH)
        try: csv_size = os.stat(csv_path).st_size
        except: csv_size = 0
        
        if csv_size == 0:
            with open(CSV_FILE_PATH, 'w') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Key','Summary','Components','Assignee','Reporter','Status','Resolution','Created','Updated','Stack ID']) # 'Issue Type',

        df=pd.read_csv(CSV_FILE_PATH, usecols=["Key"])
        jira_key = df.Key.tolist()
        if JIRA_ID not in jira_key:
            write_csv(JIRA_ID, meta)
        else:
            write_csv(JIRA_ID, meta, True)
    
    update_sheet()

main()