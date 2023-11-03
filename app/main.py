import os
import pytz
import logging
import unidecode
import traceback
import pandas as pd
import datetime as dt
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

#twilio imports
from twilio.rest import Client as TwilioClient

#Google Dependencies
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.service_account import Credentials


load_dotenv()
#### GLOBALS ####
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
RESPONSE_SHEET_RANGE = "A:I"
COUNTRY_CODE = "+1"
MASTER_ALERT_NUM = os.getenv("MASTER_ALERT_NUM")
MASTER_SHEET_URL = os.getenv("MASTER_SHEET_URL")
MASTER_SHEET_ID = os.getenv("MASTER_SHEET_ID")
RESPONSE_SHEET_GID = "0" # main sheet with all monthly progress data
PROGRESS_SHEET_RANGE = "A:D" # These columns contain history of all links and forms per month
PUBS_SHEET_GID = os.getenv("PUBS_SHEET_GID") # sheet GID for volunteer map
PUBS_SHEET_RANGE = "pubs!A:I"
DBWH_SHEET = os.getenv("DBWH_SHEET")
DBWH_SHEET_GID='0'
SCRIPT_STOP_DAY= int(os.getenv("SCRIPT_STOP_DAY",5))
ADMIN_NAME = os.getenv("ADMIN_NAME", "George Cruz")
APP_LEVEL = os.getenv("APP_LEVEL", "dev") # prod or dev (default if missing env var)


def create_service_account_creds() -> Credentials:
    creds = {"type": "service_account",
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n","\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("CLIENT_CERT_URL")}

    if missing_creds := [var for var, val in creds.items() if val is None]:
        raise KeyError(f"Missing Environment variable(s): {','.join(missing_creds)}")

    auth = Credentials.from_service_account_info(creds)
    assert not auth.expired, "Google Auth expired" # error will raise exception, auth will return None
    return auth


def get_worksheet_data(sheets_service, sheet_id, range) -> pd.DataFrame:
    """Return spreadsheet data based on sheet_id and range

    Args:
        sheet_id str: sheet_id found in url
        service obj: Google sheets API service built with creds - ex) sheets api service
        range str: A1-style ranges. Refer to sheename by '<sheetname>!<range>'

    Returns:
        Pandas DataFrame: tabular style column/row object - Dataframe
    """
    # Call the Sheets API
    sheet = sheets_service.spreadsheets()
    
    result = sheet.values().get(spreadsheetId=sheet_id,range=range).execute()
    values = result.get('values', [])

    if not values:
        print(f'No data found for sid {sheet_id}.')
        return

    df = pd.DataFrame(values, columns=None)
    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header
    df.reset_index(drop=True, inplace=True) # reset index count after dropping header
    
    if 'Horas' in df.columns:
        # BUG: This will fail upon using partial hours!
        # casting to a float may cause issues when serializing to JSON obj for Sheets API
        df.Horas = df.Horas.astype(int) 
        # df.Horas = df.Horas.astype(float)

    # ## debugging
    # for row in values:
    #     print('Timestamp, name')
    #     # Print columns A and E, which correspond to indices 0 and 4.
    #     print('%s, %s' % (row[0], row[1]))
    #     # print(row)
    # ##
    
    return df


def clean_informes_data(df) -> None:
    """Cleans data in dataframe"""
    def trim_values(input_val):
        """Remove any extra spaces and return string in Title Case"""
        val = str(input_val)
        try:
            return val.replace("  "," ").strip().title()
        except:
            return val

    def replace_spec_chars(accented_string):
        """Remove non-Unicode characters from a string"""
        try:
            unaccented_string = unidecode.unidecode(accented_string)
        except:
            return accented_string
        return unaccented_string
        
    def replace_double_space(name_string:str) -> str:
        """Return string without consecutive spaces

        Args:
            name_string (str)
        """
        try:
            if "  " in name_string:
                return name_string.replace("  ", " ")
            elif "   " in name_string:
                return name_string.replace("   ", " ")
            else:
                return name_string
        except:
            return name_string
        
    df['¿Cual es su nombre?'] = df['¿Cual es su nombre?'].apply(lambda x: trim_values(x))
    df['¿Cual es su nombre?'] = df['¿Cual es su nombre?'].apply(lambda x: replace_double_space(x))
    df['¿Cual es su nombre?'] = df['¿Cual es su nombre?'].apply(lambda x: replace_spec_chars(x))
    # return df # required?


def get_missing_reports(df, volunteers) -> list:
    """Returns a list of volunteers that have not reported time!
    NOTE: This is slightly slower than using built-in pandas functions
    """
    return [
        name
        for name in volunteers
        if not df['¿Cual es su nombre?'].isin([name]).any()
    ]


def send_twilio_message(contact_list_dict, error=None, error_message=False) -> tuple:
    """Template for sending notifications
    Returns list of failed messages {person: traceback}
    """
    # debugging
    # return None, None
    #
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_num = os.getenv('TWILIO_NUM')
    client = TwilioClient(account_sid, auth_token)
    logging.info(f"twilio logging level: {client.http_client.logger.level}")
    message_stats = []
    failed_messages = []

    if error_message:
        message = client.messages \
                    .create(
                        body=error,
                        from_=twilio_num,
                        to=COUNTRY_CODE + MASTER_ALERT_NUM
                    )
        message_stats.append(message)

    else:
        for contact_dict in contact_list_dict:
            if contact_dict is None or contact_dict['name'] == "" or contact_dict['number'] == "":
                print('No contact name - skipping')
            try:
                msg_body =  f"¡Hola! Este es un mensaje automatizado de parte de {ADMIN_NAME}\n" \
                            f"Por favor de entregar el informe para {contact_dict['name'].split(' ')[0]}.\n" \
                            f"Si tiente alguna pregunta, favor de contactar a {ADMIN_NAME.split(' ')[0]} directamente.\n" \
                            f"Muchas gracias.\n" \
                            f"Enlace para informe: {contact_dict['form_link']}"

                message = client.messages.create(
                        body=msg_body,
                        from_=twilio_num,
                        to=contact_dict['number'].strip('\u202c').strip()
                    )
                
                message_stats.append(message)
            except Exception as e:
                logging.error(f"Failed msg for {contact_dict['name'].split(' ')[0]}", exc_info=True)
                failed_messages.append({contact_dict['name']:traceback.format_exc()})

    return failed_messages, message_stats


def update_sheets_range(sheets_service, sheet_id, range_to_update, new_value):
    """Update sheet range. Must use A1 Style. Append sheet name if >1 sheets. 

    Args:
        sheets_service: obj
        sheet_id: str
        range_to_update: str - A1 style range: "A1" or "A1:C1"

    Returns:
        str: API response
    """
    value_input_option = 'USER_ENTERED'

    _range = range_to_update

    value_range_body = {
        "majorDimension": "ROWS",
        "values": [[new_value]] # any update MUST be list of lists!
    }

    request = sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id, 
        range=_range,
        valueInputOption=value_input_option, 
        body=value_range_body)
    # pprint(response) # debugging
    return request.execute()


def update_master_db(sheets_service, sheet_id, df, sheet_range):
    """Modify sheet data"""

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'
    json_val_list = []

    # _range = f'J1:J{len(df) + 1}' # must add column header, but index doesn't count in len
    _range = sheet_range

    for ix,_ in df.iterrows():
        data_to_serialize = [
            int(val) 
            if not isinstance(val,str) 
            else val 
            for val in df.iloc[ix].fillna('').to_list()]
        json_val_list.append(data_to_serialize) # will fill Empty values as strings

    value_range_body = {
        "majorDimension": "ROWS",
        "values": json_val_list
    }

    request = sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id, 
        range=_range,
        valueInputOption=value_input_option, 
        body=value_range_body)
    return request.execute()


def add_last_name_to_report(google_service, sheet_id, current_month_df):
    """Append the last name to the report for sorting.

    Args:
        sheet_id (str): [description]
        google_service (google ): [description]
        current_month_df ([type]): [description]

    Returns:
        [type]: [description]
    """

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # TODO: Update placeholder value.
    json_val_list = []

    _range = f'J1:J{len(current_month_df) + 1}' # must add column header, but index doesn't count in len

    for row in range(len(current_month_df) + 1):
        if row == 0:
            json_val_list.append(["Last_Name"])
        else:
            current_row = row + 1
            json_val_list.append([f"=TRIM(PROPER(MID(B{current_row},SEARCH(\" \",B{current_row})+1,LEN(B{current_row}))))"])

    value_range_body = {
        "majorDimension": "ROWS",
        "values": json_val_list
    }

    request = google_service.spreadsheets().values().update(
        spreadsheetId=sheet_id, 
        range=_range,
        valueInputOption=value_input_option, 
        body=value_range_body)
    # pprint(response) # debugging
    return request.execute()


def sort_sheet(sheet_service, spreadsheet_id, sheet_gid, column_to_sort="J", sort_type="DESCENDING"):
    sort_success = False
    try:
        requests = {
            "requests": [
                {
                    "sortRange": {
                            "range": {
                                "sheetId": sheet_gid,
                                "startRowIndex": 1, # DON'T start at 0, this is the header row.
                                "startColumnIndex": 0 # MUST start at 0 to sort rows or else data corruptioon occurs
                            },
                            "sortSpecs": [
                                {
                                    "dataSourceColumnReference": {
                                        "name": column_to_sort # column by letter to sort! J = Last Name
                                    },
                                    "sortOrder": sort_type # ASC/DESC - Spelled out in CAPS
                                }
                            ]
                    }
                }
            ]
        }
        response = sheet_service.spreadsheets().batchUpdate(body=requests, spreadsheetId=spreadsheet_id).execute()
        if response:
            sort_success = True
        return sort_success
        
    except Exception as e:
        print(e)
        send_twilio_message({},error=e,error_message=True)
        return sort_success


def parse_sheet_and_gid_from_url(url):
    """Return sheet_name and gid from Google Sheets URL"""
    sheet_name, sheet_gid = None,None
    for row,url_text in enumerate(url.split('/')):
        # print(row,url_text) # Debugging
        if row == 5 and url_text and len(url_text) > 5:
            sheet_name = url_text.strip()
        if row == 6 and '=' in url_text:
            sheet_gid = url_text.split("=")[-1].strip()
    return sheet_name, sheet_gid


def update_datawarehouse(sheets_service, current_report_df, current_report_month, range='A:J'):
    '''get current data from master DW sheet. Append data that is NEW for specified month'''
    
    data_warehouse_df = get_worksheet_data(sheets_service, DBWH_SHEET, range=range)
    dw_cur_month_df = data_warehouse_df[data_warehouse_df['Year-Month'] == current_report_month]

    temp_df = current_report_df.rename(columns={'Timestamp':'Year-Month'})
    temp_df.reset_index(drop=True, inplace=True)
    temp_df['Year-Month'] = current_report_month
    temp_df = temp_df[ # drop those that are already in datawarehouse. No duplicates!
        ~(temp_df['¿Cual es su nombre?'].isin(dw_cur_month_df['¿Cual es su nombre?'].to_list()))
        ]

    if temp_df.empty:
        logging.info("No new data for Data Warehouse!")
        return

    temp_df = pd.concat([data_warehouse_df,temp_df])
    temp_df.reset_index(drop=True, inplace=True)

    dw_update_response = update_master_db(sheets_service, DBWH_SHEET, temp_df, f'A2:J{len(temp_df) + 1}')

    if dw_update_response:
        logging.info(f'Updated Master Sheet with {dw_update_response["updatedRows"]}')
    else:
        logging.error(f"Possible Warehouse Update error: {dw_update_response}")
        raise Exception("Data warehouse update error")


def generate_alert_list(current_form_url, missing_reports_df, volunteer_map_df):
    '''Generate list of alerts by person based on contact rules. (ie. Escalation rules)'''
    
    format_number = lambda x: \
        f"{COUNTRY_CODE}{x.replace('(','').replace(')','').replace(' ','').replace('  ','').replace('-','').strip()}" \
        if x is not None or x != "" \
        else MASTER_ALERT_NUM

    twilio_message_list = []

    for (ix,row) in missing_reports_df.iterrows():
        permissions = row['permission_to_contact?']
        contact_delegation = ''

        if row['Active?'] == 'n':
            print(f'Skipping inactive volunteer {row["full_name"]}')
            continue # skip inactive volunteers

        elif permissions == 'y':
            twilio_message_list.append(
                {
                    'name': row['full_name'],
                    'number': format_number(row['Cell']).replace('\u202d','').replace('\u202c','') , 
                    'form_link': current_form_url
                }
            )

        elif permissions == 'n' and not pd.isnull(row['delegate_notification_to']):
            contact_delegation = \
                volunteer_map_df[
                    volunteer_map_df.row_id == row['delegate_notification_to']
                    ]['Cell'].item()
            if isinstance(contact_delegation,list):
                contact_delegation = contact_delegation[0]
            if contact_delegation == "":
                raise Exception(f"{row['full_name']} should delegate to id {contact_delegation} - but is Null")

            twilio_message_list.append(
                {
                    'name': row['full_name'],
                    'number': format_number(contact_delegation).replace('\u202d','').replace('\u202c',''), 
                    'form_link': current_form_url
                }
            )
    return twilio_message_list


def append_day_suffix(day) -> str:
    '''Simple function to return proper suffix for day in date. 
    current_date: int
    returns: str
    '''
    match str(day)[-1]: # get first item @ end of str
        case '1':
            suffix = 'st'
        case '2':
            suffix = 'nd'
        case '3':
            suffix = 'rd'
        case _:
            suffix = 'th'
    # print(f'{current_date.day}{suffix}')
    return f'{day}{suffix}'
            
### end of func defs



def run():

    # APP_ENV is either prod or dev. If missing var, run as dev
    if APP_LEVEL != "prod":
        from pprint import pprint
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO

    logging.basicConfig(format='[%(levelname)s] %(asctime)s | %(message)s', datefmt='%m/%d/%Y %I:%M:%S%p', level=logging_level)


    today = dt.datetime.now(tz=pytz.timezone('US/Central'))
    tomorrow = today + dt.timedelta(days=1)

    # run on last date of the month up until max day in month. 
    # example: runs on 28th if tomorrow is the 1st. (Will consider leap years!)
    #          AND runs on 1st up until the 7th (SCRIPT_STOP_DAY)
    if today.day >= SCRIPT_STOP_DAY and tomorrow.day != 1 and APP_LEVEL != "dev":
        logging.info(f"It's past the {append_day_suffix(SCRIPT_STOP_DAY)} - Manual intervention required!")
        return


    try:
        creds = create_service_account_creds()
        sheets_service = build('sheets', 'v4', credentials=creds)

        # 1 get last sheet in progress_master sheet
        progress_df = get_worksheet_data(sheets_service, MASTER_SHEET_ID, PROGRESS_SHEET_RANGE)

        # get last row from sheet
        # should we use a date parser to sort by date instead? - This would be more "fail safe"
        # DO NOT modify the index, it will be used to update the status 
        progress_df = progress_df.iloc[len(progress_df)-1:]
        current_report_month = progress_df.year_month.unique().item()

        if len(progress_df[progress_df['status'] == 'completed']):
            # no need to continue, all volunteer data has been collected!
            # print(f"Exiting... All volunteer data has been collected for {current_report_month}.")
            logging.info(f"Exiting... All volunteer data has been collected for {current_report_month}")
            return


        # get volunteer data 
        volunteer_map_df = get_worksheet_data(sheets_service, MASTER_SHEET_ID, PUBS_SHEET_RANGE)

        # get current month's data
        report_sheet_id,report_sheet_gid = parse_sheet_and_gid_from_url(progress_df['response_sheet_url'].item())

        current_report_df = get_worksheet_data(
            sheets_service=sheets_service,
            sheet_id=report_sheet_id,
            range=RESPONSE_SHEET_RANGE)

        """ If there is any data:
            1. Clean data
            2. Add last names
            3. Sort Sheet by Last Name
            4. Drop Duplicates by Name, if any
            5. Create Missing Reports DataFrame, if any
            6. Update Tracking sheet if ALL volunteers have reported time
            7. OR send message to proper contact 
            8. Update Datawarehouse 
        """
        if len(current_report_df):
            # clean data - Passed by ref, so this modifies object
            # upon new month with empty df, do not make all these api calls
            clean_informes_data(current_report_df)

            # append last name via Google Sheets API call
            add_name_reponse = add_last_name_to_report(sheets_service,sheet_id=report_sheet_id,current_month_df=current_report_df)

            # check if proper # of rows were updated: len(df) + header row "Last_Name"
            assert add_name_reponse['updatedCells'] == (len(current_report_df)+1), "Updated rows does not match length of report df"

            sort_response = sort_sheet(
                sheet_service=sheets_service,
                spreadsheet_id=report_sheet_id,
                sheet_gid=report_sheet_gid,
                column_to_sort="J",
                sort_type="ASCENDING")

            assert sort_response, "Sorting report failed"

            duplicates_df = current_report_df[current_report_df['¿Cual es su nombre?'].duplicated()]
            if len(duplicates_df):
                ### do something with this... Alert?
                logging.warning(f"There are duplicates!{duplicates_df['¿Cual es su nombre?'].to_list()}")
            del duplicates_df

        #### If current_report_df is empty, we need to begin sending reminder messages 

        # Find those who haven't submitted their report
        # Add full_name field
        volunteer_map_df['full_name'] = volunteer_map_df.apply(lambda row: f"{row['First_Name']} {row['Last_Name']}",axis=1)

        # get df of missing reports, if any. 
        missing_reports_df = volunteer_map_df[~volunteer_map_df['full_name'].isin(current_report_df['¿Cual es su nombre?'])]

        # drop inactive volunteers
        missing_reports_df.drop(index=missing_reports_df.loc[ lambda df: df['Active?'] == 'n' ].index,inplace=True)

        if missing_reports_df.empty:
            # Collection has been Completed!

            # Update the progress_sheet to complete if there are no more to collect!
            # index starts at 0 & header doesn't count so +2 to index. Column D is progress
            try:
                cell_to_update = f"D{progress_df.index.to_list()[0] + 2}"
                progress_completion_update = update_sheets_range(sheets_service,
                                                MASTER_SHEET_ID,
                                                range_to_update=cell_to_update, 
                                                new_value='complete')
                ###TODO: Add code to email secretary!
                logging.info(f"Report collections for {progress_df['year_month'].item()} Complete!")

            except:
                logging.error(traceback.format_exc())
                raise Exception(f"Error updating progress sheet! {progress_completion_update}")
        else:
            # IF there are any missing reports, contact volunteer

            current_form_url = progress_df['form_url'].item()
            twilio_message_list = generate_alert_list(current_form_url, missing_reports_df, volunteer_map_df)
            errors_from_twilio, message_stats = send_twilio_message(twilio_message_list,None)
            if message_stats:
                logging.info(f"Sent {len(message_stats)} messages")

            if errors_from_twilio:
                raise Exception(f"Message Send Failure(s): {len(errors_from_twilio)}")

        # Finally, copy formatted volunteer data to datawarehouse
        if len(current_report_df):
            update_datawarehouse(sheets_service, current_report_df, current_report_month, range='A:J')

    except Exception as e:
        logging.error(e)
        send_twilio_message({}, traceback.format_exc(), error_message=True)

    logging.info("DONE")


if __name__=='__main__':
    
    ## debugging
    ## TODO: use pytest to create unit tests
    # current_form_link = "https://forms.gle/79BW1bGsJDMJiuAQ6"
    # send_twilio_message([{"name": "Test Person", "number": MASTER_ALERT_NUM, "form_link": current_form_link}], None)
    ##

    print("Running upon deployment...")
    run()


    # Schedule this script to run at a specific cadence
    scheduler = BlockingScheduler()
    #24 hr format: Runs at 6pm CST
    scheduler.add_job(func=run, trigger='cron', hour=18, timezone='US/Central')
    print('Starting scheduler...')
    scheduler.start()

