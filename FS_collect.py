import pandas as pd
import os
import pathlib
import time
from tenacity import *
from datetime import datetime
from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import gspread
from google.oauth2 import service_account
from pydrive import auth, drive
pd.options.mode.chained_assignment = None

# COMMON FUNCTION
# ==================================================================
def connect_drive(bi_key, auth, drive, gspread):
    # Setup GDrive
    gauth = auth.GoogleAuth()
    scope = ["https://www.googleapis.com/auth/drive"]
    gauth.credentials = auth.ServiceAccountCredentials.from_json_keyfile_dict(
        bi_key, scope)
    drive = drive.GoogleDrive(gauth)
    gc = gspread.authorize(gauth.credentials)
    print("Connected to DRIVE!")
    return gc, drive

# HCO COLLECT RESPONSE TOOL
# ==================================================================
def get_li_files(drive, parents_id):
    return drive.ListFile(
        {'q' : f"'{parents_id}' in parents and trashed=false"}
    ).GetList()
def read_gsheet(gc, sheet_id, ws_name):
    sheet = gc.open_by_key(sheet_id)
    worksheet = sheet.worksheet(ws_name)
    return pd.DataFrame(worksheet.get_all_records())
def collect_responses(drive, done_export):
    print("Collecting files from shipper's folders. It might take a while...")
    report = pd.DataFrame()
    dict = {}

    cur_date = datetime.today().strftime("%d-%m-%Y")

    for folder_id in list(set(done_export['f_id'])):
        try:
            li_files = drive.ListFile(
                {'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
            # dict = {file['title'][-21:-11]: file['id'] for file in li_files}
            dict = {f"{file['title'][-15:-5]}": file['id'] for file in li_files}
        except Exception as e:
            print(f'{e}')

        try:
            file = drive.CreateFile({'id': dict[f'{cur_date}']})
            file.GetContentFile(file['title'])
            rp = pd.read_excel(file['title'], usecols=[i for i in range(12)])
            report = pd.concat([report, rp])
        except Exception as e:
            print(e)
            print(f'Không tìm thấy file trong folder {folder_id}')
    print("Done collect response!")
    return report, dict
def del_local_files(dir):
    for file_path in dir.rglob("CO*"):
        try:
            os.remove(file_path)
        except Exception as e:
            print(e)

def remove_today_file(drive, parents_id):
    cur_date = datetime.today().strftime("%d-%m-%Y")

    li_files = get_li_files(drive, parents_id)
    li_files = [file for file in li_files if file['title'][:10] == cur_date]
    for file in li_files:
        del_file = drive.CreateFile({'id': file['id']})
        del_file.Delete()
        print(f'Deleted "{file["title"]}"')

def export_responses(gc, drive, responses, res_folder_id):
    # Remove file of today to prevent duplicated response file
    remove_today_file(drive, res_folder_id)

    # Get list of existed files in folder Shipper response
    hco_files = drive.ListFile(
        {'q': f"'{res_folder_id}' in parents and trashed=false"}).GetList()
    hco_dict = {file['title'][:10]: file['id'] for file in hco_files}
    # print(f'HCO dict = {hco_dict}')

    # Setup file name
    hco_date = datetime.today().strftime("%d-%m-%Y")

    hco_filename = hco_date + "_HCO_shipper_response"
    print("HCO shipper response file name: ", hco_filename)

    # Convert all column values into string
    for col in responses.columns:
        responses[col] = responses[col].astype(str)

    print("Create new shipper response gsheet: ", hco_filename)
    gsheet = gc.create(hco_filename, folder_id = res_folder_id)
    sheet = gc.open_by_key(gsheet.id)
    worksheet = sheet.get_worksheet(0)
    worksheet.clear()
    worksheet.update([responses.columns.values.tolist()] + responses.values.tolist())

path = pathlib.Path().absolute()
directory = pathlib.Path(path)
start_time = time.time()
bi_key = {
    "type": "service_account",
    "project_id": "vn-bi-337205",
    "private_key_id": "eb6a24203230f317195a1a11e50d726233418b1c",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDbsnfTJjI3FzjE\ni8/6TfLPTSURKxfyf7fLHrh2SF4LbPwkXplTmFMJTKG4+JtREOUd/Q4ImSuTUO9H\nDlHVgnCLPzgBNvhLJGxOyPqQ5FSuf/BwMDExW43iRiPKAvyg7g5zd5BUix1iIfa+\nk7vbL3kBvQyYfPjiNwXY57uW33qR3ivv/peboQ6bJ6wCgY0dqPwt85jIaCdp7VAl\nvknmHPY+8t03bXOVTZgllDaLzbQHI7RbXaRwTEfE1PeL/durxaEsxsQ4V/TtIsxW\nbfLyL4LbEOF8/7SmNS8C4iMd0odbC12d7IoAu4zwwYf3IbGnXLAkCNitZT6e2pEX\nf77AF9oBAgMBAAECggEAD0Ul0qLJLblq+YT1lTe9bzmKSy8GxepTpjDB8H1nhVLI\nvxG7hYBvu788Tw9/OCYSTsB2gml8X7Zp6IiZw9ss+GNnVfTx/W261h+zrsYiuVLs\n2Cy5VpgUUgxpqL0GvdFzZq+CSXthNPTDDOGsK81uaI0GfEN/cbW9AyOu5GCEc/FE\nzxGee0A/Infr4AC7ulOS7kXdMIIGIZz9R8eu0WN0ZKXqh+qNmjioMa0YoXNsA2fT\n6qmSVR/6cFehmKBnhA8eTBXhU0PovhBVddStScPowZQP6usJji31tN5cQYKbFzCz\n94z1lZXDTegBTa1e++uhpCN/BnGGDir76wOVBG2cawKBgQD1RYSQLyTpsb6nr32N\nnlGTx9LfaDWsw35Sex0Li1deDvJBtN+87XlEHjH+M2ieJl+bRciV8hGLvK1qgelz\nsO1lh0BmE4lIXv2ZdZjJQTNmKJjRPfOf4yQNhKHhLCcEWSUXiwKjgfO6TIJarez5\nxJqLSgqpbw3ONjE/lXBuR2i/0wKBgQDlTpM7WbXxX6uhyC+1Z2Igpk0KNTOdL8eF\nZvxnWTE6xwgZn2XsQ6XYprtKAhZqvX2iHnZmXCeh3xsbKfHtyz7CU92kUedpM3/6\nn0u87dQwshX3e2FdsBujIyWtajeaZeZu+SGkosQ2qJghLIslDA8PN9bMRP04NR+1\nQ1TSkHVuWwKBgQCK+EmlHGEw3FonTsK31CVqs0Ti+nu3GMlhWIpbeScWjydqXV6M\nAI2+L9Gps10qhpmTM9X0R8TzRQOJHnS07WGFLj4p2BXn4JKWOdBI7918m7IClLSO\nOje6RRUnrUmqKAxhK41iMYZ0X1dYo8Vqu5/JHjmuSNMsrP34hea/RUyvqwKBgBc8\ndBzyhUZVTH4TXApE1KO8VlgvdfB6s/wnqHIMjrmHC6IVDe34GOkwVBA2z/C3DPEN\npy/OLNHxWrzQ7kwLaWultfL9vbZiWpiZ5cHaU8MveJxga0hkHRjV6e59se66XrRm\nGGundryXLvtFmUACzc/b968xWnuEnpbuoLzJmKy7AoGBALljZNOACK43BeKYfdR5\nLLth8Aj13lWGcF4fPVPzY8TNfIfIamUuCTi83IRY11e0h3Gb+eFxRmFyq6jdduS7\nMpjS9OEBW3zj8oY9Ms8P0TZ0v2gPOG++xYbSld5M/fNrpsbQP+66j71uvrdj4RgM\n3rj4ub2ec7TIMF6cd5Yoc0tI\n-----END PRIVATE KEY-----\n",
    "client_email": "vn-bi-6th@vn-bi-337205.iam.gserviceaccount.com",
    "client_id": "117534717412498184193",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/vn-bi-6th%40vn-bi-337205.iam.gserviceaccount.com"
}

# Connect Drive
gc, drive = connect_drive(bi_key,auth,drive,gspread)

def main():
    # Read sheet done_export into dataframe

    # output_sheet_id = "16Old5szbBUNVZ6lwRoY9O4sl_6FVHwXOO0a5jKg4Em4" #test
    output_sheet_id = "1JvkWaECyz6FVdvm8kOkYJD1z7utS97UPs_Hp1KXfJ0c"
    output_sheet_name = "done_export"
    # response_folder_id = "1YFsmmzhZA9PRY4yHFLFwZAMsfaVJ3ozq" #test
    response_folder_id = "1GyGxP-E15EZVKTYrwhNbSRaL9YHrigOy"

    done_export = read_gsheet(gc, output_sheet_id, output_sheet_name)

    # This collect response tool will be run in the following day of reported day
    hco_date = datetime.today().strftime("%d-%m-%Y")
    print("HCO date: ",hco_date)

    # Collect responses by Timeslot
    response_df, dict = collect_responses(drive, done_export)

    # Delete local collected files
    del_local_files(directory)

    # Fill na value by "-""
    response_df.fillna('-', inplace=True)
    print(response_df.shape)

    # Export to Googlesheet
    export_responses(gc, drive, response_df, response_folder_id)

    print(f'Execution time: {time.time() - start_time}')


if __name__ == '__main__':
    main()