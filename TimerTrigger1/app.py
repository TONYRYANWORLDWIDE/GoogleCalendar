import pyodbc
from pathlib import Path
import json
import urllib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
from datetime import timedelta
import dateutil
from dateutil import parser
import pickle
import os.path
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging


class googleCalendar():
    
    def __init__(self):
        home = str(Path.home())
        self.credentials_file = os.path.join(home,'Desktop' ,'GoogleCalendar' ,'credentials.json')
        self.server = ''
        self.user = ''
        self.password = ''
        self.database = ''
        self.userid = ''
        
    def getCredentials(self):
            # with open(self.credentials_file) as json_file:
            #     data = json.load(json_file)
        # self.server =   data['connectionstring']['server']
        self.server = os.environ['server']
        # self.user =     data['connectionstring']['user']
        self.user = os.environ['user']

        # self.password = data['connectionstring']['password']
        self.password = os.environ['password']
        # self.database =  data['connectionstring']['database']               
        self.database = os.environ['database']
        self.params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                        "SERVER=" + self.server + ";"
                        "DATABASE=" + self.database + ";"
                        "UID=" + self.user + ";"
                        "PWD=" + self.password)
        return self
        
    def getPhotoEvents(self):
        SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
        creds = None
            # The file token.pickle stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('calendar', 'v3', credentials=creds)
        # Call the Calendar API
        now = datetime.datetime.utcnow()
        now = now - timedelta(days=30)
        now = now.isoformat() + 'Z'
        events_result = service.events().list(calendarId='primary', timeMin=now,
                                            maxResults=200, singleEvents=True,
                                            orderBy='startTime').execute()
        events = events_result.get('items', [])
        photoevents = {}
        if not events:
            print('No upcoming events found.')
        for event in events:
            if 'colorId' in event:
                if event['colorId'] == '4':
                    print('')
                    etag = event['etag']
                    etag=etag.replace('"','')          

                    if 'dateTime' in event['start'].keys():
                        start_base = event['start']['dateTime']
                        start = str(dateutil.parser.parse(start_base).date())
                    elif 'date' in event['start'].keys():
                        start = event['start']['date']        
                    if 'description' in event.keys():
                        description = event['description']
                        money = re.findall(r'\$([0-9]+)', description)                
                        moneyints = [int(item) for item in money]
                        moneyints.sort(reverse=True)
                        if len(moneyints) > 0:
                            revenue = moneyints[0]
                            photoevents[etag] ={'Summary' :event['summary'], 'Date' : start,'Revenue' : revenue}
        photojson = str(json.dumps(photoevents))
        return photojson
    
    def updateGoogleCalendar(self):
        photojson = self.getPhotoEvents()
        credentials = self.getCredentials()     
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(self.params))
        command = 'Exec cmp.mergeGoogleCalendar @json =' + "'" + photojson + "'"
        cxn = engine.raw_connection()
        cur = cxn.cursor()
        cur.execute(command)
        cur.close()
        cxn.commit()
        
    def mergeIntoOneOffBills(self):
        credentials = self.getCredentials()
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(self.params))
        command = 'Exec mergeGoogleCalendar_OneOffBills'
        cxn = engine.raw_connection()
        cur = cxn.cursor()
        cur.execute(command)
        cur.close()
        cxn.commit()
  

     
def main():
    logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
    a = googleCalendar()
    a.updateGoogleCalendar()
    a.mergeIntoOneOffBills()
if __name__ == '__main__':
    main()

