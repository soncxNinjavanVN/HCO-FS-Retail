import pandas as pd
from tenacity import *
from datetime import datetime
from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Alignment
from openpyxl.worksheet.dimensions import ColumnDimension, RowDimension
from openpyxl.worksheet.views import Pane

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

def get_li_files(drive, parents_id):
    return drive.ListFile(
        {'q' : f"'{parents_id}' in parents and trashed=false"}
    ).GetList()

def del_file_drive(drive, parents_id):
    try:
        li_files = drive.ListFile({'q': f"'{parents_id}' in parents and trashed=false"}).GetList()
        for file in li_files:
            file.Delete()
            print(f'Deleted "{file["title"]}"')
    except Exception as e:
        print(e)
        print("Error when deleting file")

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
    parents_id = "1Auh1YD8esPeC7KtAtz2gQbBH9hcyuhgI"
    del_file_drive(drive, parents_id)

if __name__ == "__main__":
    main()