import csv
import io
import os
from jira.client import JIRA
import time
from pathlib import Path
from getpass import getpass, getuser
from jira.client import JIRA
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from requests import HTTPError
import concurrent.futures

AD_USER = getuser()

JIRA_IDS = []
CSV_FILE_PATH = ""
SPREADSHEET_ID = ""
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
    print("AD Token found!\n")
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
    if t.time() < datetime.strptime('06:30:00', '%H:%M:%S').time():
        # If the time is before 6:30 AM, use yesterday's date
        t -= timedelta(days=1)
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))

def find_TO():
    # query = 'project = "TO" AND filter=81746 AND issuetype = Change AND status = "NEEDS PRE-CHECK" OR status = Pre-Check AND "Change type[Dropdown]" = Impacting AND "Epic Link" = EMPTY AND component != termination'
    query = 'project = "TO" AND filter=81746 AND issuetype in standardIssueTypes() AND ("Cloud Environment" not in ("Gov Cloud", Fedramp) OR "Cloud Environment" is EMPTY) AND (resolution != Cancelled OR resolution is EMPTY) AND resolution = Unresolved AND status in ("NEEDS PRE-CHECK",Pre-Check) ORDER BY status DESC'

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

def update_sheet(sheet, sheet_id):
    rows = []
    if os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, newline='') as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)


    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'startColumnIndex': 0
                },
                'rows': [{
                    'values': [{'userEnteredValue': {'stringValue': cell}} for cell in row]
                } for row in rows],
                'fields': 'userEnteredValue'
            }
        },
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': len(rows[0])
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'bold': True
                        },
                        'backgroundColor': {
                            'red': 0.85,
                            'green': 0.85,
                            'blue': 0.85
                        }
                    }
                },
                'fields': 'userEnteredFormat(textFormat,backgroundColor)'
            }
        }
    ]

    sheet.batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': requests}
    ).execute()



def process_jira(jira_id):
    meta_jira=jira.issue(jira_id).raw["fields"]
    meta = []
    meta.append(jira_id)
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
    try:
        csv_size = os.stat(csv_path).st_size
    except:
        csv_size = 0

    if csv_size == 0:
        with open(CSV_FILE_PATH, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Key','Summary','Components','Assignee','Reporter','Status','Resolution','Created','Updated','Stack ID'])

    df=pd.read_csv(CSV_FILE_PATH, usecols=["Key"])
    jira_key = df.Key.tolist()
    if jira_id not in jira_key:
        write_csv(jira_id, meta)
    else:
        write_csv(jira_id, meta, True)

def main():
    find_TO()
    global CSV_FILE_PATH
    global SPREADSHEET_ID
    
    CSV_FILE_PATH = "jira-work-report_" + custom_strftime('{S}_%b', datetime.now()) + ".csv"
    sheet_name = custom_strftime('{S} %b', datetime.now())
    
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # If modifying these scopes, delete the file token.json.

    creds = None
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = '1_VWMNZmrXqTqvgZTnO43nPzafTEAZRHKd2-4DSnM35o'
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    
    # Check if the sheet already exists
    try:
        sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheet_exists = any(sheet_name == sheet['properties']['title'] for sheet in sheet_metadata['sheets'])
    except HTTPError as error:
        print(f"An error occurred: {error}")
        sheet_exists = False
    body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }]
    }
    
    if not sheet_exists:
        # Create the sheet if it doesn't exist
        try:
            response = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            print(f"Spreadsheet ID: {(response.get('spreadsheetId'))}")
        except HTTPError as error:
            print(f"An error occurred: {error}")
    else:
        # Download the data from the sheet
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:ZZ'
        ).execute()

        # Save the data to a CSV file
        rows = result.get('values', [])
        if not rows:
            print('No data found.')
        else:
            with io.open(CSV_FILE_PATH, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for row in rows:
                    writer.writerow(row)
    for sheet in sheet_metadata['sheets']:
        if sheet['properties']['title'] == sheet_name:
            sheet_id = sheet['properties']['sheetId']
            break
        
    
    print("############## Fetching details of TO Tickets ##############\n")
    # Threading loop for checking jira meta
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_jira_id = {executor.submit(process_jira, jira_id): jira_id for jira_id in JIRA_IDS}
        for future in concurrent.futures.as_completed(future_to_jira_id):
            jira_id = future_to_jira_id[future]
            try:
                _ = future.result()
            except Exception as exc:
                print(f'JIRA ID {jira_id} generated an exception: {exc}')
    
    # Update the sheet
    update_sheet(service.spreadsheets(), sheet_id)
    print(f"\n\nhttps://docs.google.com/spreadsheets/d/{SPREADSHEET_ID} is Updated with latest details")


main()