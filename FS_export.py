import pandas as pd
import numpy as np
import json
import requests
import os
import pathlib
import unidecode
import time
import zipfile
import logging
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

# HCO EXPORT TOOL
# ==================================================================

# Call to Redash API
@retry(wait=wait_fixed(10), stop=stop_after_attempt(7))
def redash_refresh(query_id, api_key, params={}) -> str:
    """
    Send POST request to refresh Redash-vn query data
    Use @retry decorator to refresh 3 times; if still error, raise ConnectionError

    Input:
    - query_id
    - api_key
    - params

    Output: job_id of query
    """
    # Define request's params
    _url = f'https://redash-vn.ninjavan.co/api/queries/{query_id}/results'
    _header = {'Authorization': f'Key {api_key}'}
    _body = f'{{"max_age": 0, "parameters": {json.dumps(params, ensure_ascii=False)}}}'

    _r = requests.post(url=_url, headers=_header, data=_body.encode('utf-8'))

    if not _r.ok:
        raise ConnectionError
    return _r.json()['job']['id']
@retry(wait=wait_fixed(10), retry=retry_if_result(lambda x: x is None))
def redash_job_status(job_id, api_key) -> str:
    """
    Send GET request to check job status
    Use @retry decorator to refresh; there will be 4 possible results:
    - 
    - status 1, 2: return None, will activate retry condition
    - status 3: return result_id
    - status 4, 5: raise ConnectionError

    Input:
    - job_id
    - api_key

    Output: result_id of query
    """
    _url = f'https://redash-vn.ninjavan.co/api/jobs/{job_id}'
    _header = {'Authorization': f'Key {api_key}'}

    _r = requests.get(url=_url, headers=_header)
    job_status = _r.json()['job']['status']

    if job_status == 3:
        return _r.json()['job']['query_result_id']
    elif (job_status == 1) or (job_status == 2):
        return None
    else:
        raise ConnectionError
def redash_result(result_id, api_key) -> pd.DataFrame:
    """
    Send GET request to get query result

    Input:
    - result_id
    - api_key

    Output: dataframe of query result
    """
    _url = f'https://redash-vn.ninjavan.co/api/query_results/{result_id}'
    _header = {'Authorization': f'Key {api_key}'}

    _r = requests.get(url=_url, headers=_header)
    if _r.ok:
        return pd.DataFrame(_r.json()['query_result']['data']['rows'])
    else:
        raise ConnectionError
@retry(wait=wait_fixed(5))
def redash_query(query_id, api_key, params={}) -> pd.DataFrame:
    """
    Combination of 3 funtions above
    Order of execution: refresh -> check job status -> get result

    Input:
    - query_id
    - api_key
    - params

    Output: dataframe of query result
    """
    job_id = redash_refresh(query_id, api_key, params)
    print('Query request sent. Waiting for result...')

    result_id = redash_job_status(job_id, api_key)
    print('Query completed!')

    return redash_result(result_id, api_key)
def running_redash(li_tracking_id, query_id, api_key):
    li_arrays = np.array_split(li_tracking_id, (len(li_tracking_id)//1000)+1)
    report = pd.DataFrame()
    for k, df in enumerate(li_arrays):
        print(k)
        _query_params = {'tracking_id': f"""'{"', '".join(df.tracking_id)}'"""}
        rp = redash_query(query_id, api_key, _query_params)
        report = pd.concat([report, rp], ignore_index=True)
        # report.to_csv('0901_fs.csv')
    print("Done extract data from redash")
    return report

# Using Openpyxl lib to create spreadsheet
def set_col_width(worksheet, col, size):
    worksheet.column_dimensions[col].width = size
    return
def set_style(worksheet, cell):
    worksheet[cell].font = Font(bold=True)
    worksheet[cell].alignment = Alignment(
        horizontal="center", vertical="center")
    return
def add_data_to_sheet(report_full, path):

    # This function will add data by shipper to sheet
    # and store the file in the path of local dir

    workbook = Workbook()
    worksheet = workbook.active

    # import dataframe
    for row in dataframe_to_rows(report_full, index=False):
        worksheet.append(row)

    # data validation
    li_options = '"Giao lại, Hoàn hàng, Khách nhận rồi"'
    options = DataValidation(type="list", formula1=li_options)
    options.add('J2:J1048576')
    worksheet.add_data_validation(options)

    # change column width and font style
    for i, j in zip(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'], [20, 20, 30, 25, 40, 40, 40, 20, 14, 20, 20]):
        set_style(worksheet, f'{i}1')
        set_col_width(worksheet, f'{i}', j)

    # save WorkBook
    workbook_name = list(report_full['Tên đối tác'])[0]
    workbook.save(f'{path}/{workbook_name}.xlsx')
    return workbook_name

# Import shipper info
def read_shipper_info(sheet):
    temp = sheet.worksheet('shipper_info')
    shipper_info_data = temp.get('A2:D')

    shipper_info = pd.DataFrame(columns=[
                                'shipper_id', 'shipper_name', 'shipper_name_rut_gon', 'status'], data=shipper_info_data)
    shipper_info.dropna(subset=[
                        'shipper_id', 'shipper_name', 'shipper_name_rut_gon'], how='any', inplace=True)
    shipper_info = shipper_info[shipper_info['status'] == 'ongoing']
    shipper_info.drop_duplicates(inplace=True)
    shipper_info.drop_duplicates(
        subset=['shipper_id'], keep='last', inplace=True)
    print(f'Done - imported {shipper_info.shape[0]} shippers info')
    return shipper_info

# Import tracking id
def read_tracking_id(sheet):
    temp = sheet.worksheet('tracking_id')
    tracking_id_data = temp.get('A2:A')

    li_tracking_id = pd.DataFrame(
        columns=['tracking_id'], data=tracking_id_data)
    li_tracking_id.dropna(how='any', inplace=True)
    li_tracking_id.drop_duplicates(inplace=True)
    print(f'Done - imported {li_tracking_id.shape[0]} tracking id')
    return li_tracking_id

# Import folder drive
def import_shipper_folder(drive, co_tong_folder_id, sheet):

    # These SHIPPER FOLDERs will be edited by shipper
    print("Accessing folder drive... It might take a while..")
    li_files = drive.ListFile(
        {'q': f"'{co_tong_folder_id}' in parents and trashed=false"}).GetList()

    f_id = [i['id'] for i in li_files]
    f_name = [i['title'].strip() for i in li_files]
    co_tong_folder = pd.DataFrame(
        data=zip(f_id, f_name), columns=['f_id', 'f_name'])
    print("Done connect to", co_tong_folder.shape[0], "shipper's folder")

    # Upload folder's info
    created_date = [i['createdDate'][:10] for i in li_files]
    owner = [i['ownerNames'][0] for i in li_files]
    folder_link = [
        f"https://drive.google.com/drive/u/0/folders/{i}" for i in list(co_tong_folder.f_id)]
    shipper_folder = pd.DataFrame(columns=['folder_link', 'folder_name', 'created_date', 'owner_name'], data=zip(
        folder_link, f_name, created_date, owner))

    _folder = sheet.worksheet('shipper_folder')
    _folder.update([shipper_folder.columns.values.tolist()] +
                   shipper_folder.values.tolist())
    return co_tong_folder
def create_internal_folder(drive, internal_folder_id):
    internal_date = datetime.today().strftime("%Y-%m-%d")
    # li_existed_files = drive.ListFile({'q' : f"'{internal_folder_id}' in parents and trashed=false"}).GetList()
    # flag_existed = li_existed_files.contain
    internal_new_folder = str(f'CO TONG {internal_date}')

    # (ALWAYS) create new internal folder to store daily HCO report
    # This internal folder only viewed by internal team

    sub_folder = drive.CreateFile({
        'title': internal_new_folder,
        'parents': [{'id': internal_folder_id}],
        'mimeType': 'application/vnd.google-apps.folder'
    })
    sub_folder.Upload()
    hco_internal_id = sub_folder['id']
    return hco_internal_id

# Merge report with shipper info and drive info
def merge_report(report, shipper_info, co_tong_folder):
    # Merge report with shipper
    report['shipper_id'] = [str(i.shipper_id) for i in report.itertuples()]
    temp = report.merge(shipper_info, how='inner', on='shipper_id')
    temp.drop_duplicates(inplace=True)

    # Merge report with drive info
    temp['f_name'] = [
        "CO " + unidecode.unidecode(i.shipper_name_rut_gon.strip()).upper() for i in temp.itertuples()]
    # temp = temp.astype({'f_name': 'object'})
    final = temp.merge(co_tong_folder, how='inner', on='f_name')

    # Format column
    final['Ngày tạo đơn'] = [datetime.strptime(i, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in list(final['Ngày tạo đơn'])]
    cur_date = datetime.today().strftime("%d-%m-%Y")

    # ***************** These codes below will impact on FILENAME *************************

    final['Tên đối tác'] = ["CO " + unidecode.unidecode(i.shipper_name_rut_gon.strip()) 
                            + " " + cur_date for i in final.itertuples()]
    final.rename(columns={'Instruction': 'Hướng dẫn giao hàng'}, inplace=True)
    final['Kết quả'], final['Ghi chú'] = '', ''
    print("Done - tracking id had matched shipper info and drive info")
    return temp, final
def split_report(final):
    # Split report into reports by shipper
    report_dict = {i: final[final['f_name'] == i]
                   for i in set(list(final['f_name']))}
    print("Done - splited report into dictionary of reports by shipper")
    return report_dict

# PROCESS FILES IN DRIVE
def del_file_drive(drive, folder_id):
    # delete old file in drive to prevent duplicated file

    cur_date = datetime.today().strftime("%d-%m-%Y")

    try:
        existed_files = drive.ListFile(
            {'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
        del_files = [file for file in existed_files if (file['title'][-15:-5] == cur_date)]

        for file in del_files:
            del_file = drive.CreateFile({'id': file['id']})
            del_file.Delete()
            print(f'Deleted "{file["title"]}"')
    except Exception as e:
        print(e)
        print("Error when deleting file")
def del_file_zip_drive(drive, li_del_files):
    # delete old file in drive to prevent duplicated file
    try:
        for file in li_del_files:
            del_file = drive.CreateFile({'id': file['id']})
            del_file.Delete()
            print(f'Deleted "{file["title"]}"')
    except Exception as e:
        print(e)
        print("Error when deleting file")
def upload_file_drive(drive, dir, folder_id, title, done_export, report_dict, index, num_exported_folder):
    try:
        file_name = os.path.join(dir, f'{title}.xlsx')
        shipper_report = drive.CreateFile({
            'parents': [{'id': folder_id}],
            'title': f'{title}.xlsx'
        })
        shipper_report.SetContentFile(file_name)
        shipper_report.Upload()

        done_export = pd.concat([done_export, report_dict[index]])
        num_exported_folder += 1
    except Exception as e:
        print(e)
    return done_export, num_exported_folder

# - TYPE 1: file that contains shipper info and folder info
# - TYPE 2: file that ONLY contains shipper info - need to create
# new folder for the shipper in CO SHIPPER TONG
def upload_type_1(drive, report_full, report_dict, done_export, cant_export, num_exported_folder, path, dir):
    print('UPLOADING FILE TYPE 1 ...')

    k = 1
    flag_upload = 0
    flag_cant_export = 0

    for i in list(set(report_full['f_name'])):
        temp = report_dict[i][["Mã", "Tên khách hàng", "Tên đối tác", "Số điện thoại",
                               "Địa chỉ", "Hướng dẫn giao hàng", "Lý do", "Ngày tạo đơn",
                               "Số lần giao", "Kết quả", "Ghi chú"]].copy(deep=True)
        temp = temp.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)
        try:
            # format report, create excel file

            # "title" will be the name of report

            title = add_data_to_sheet(temp, path)

            folder_id = list(report_dict[i]['f_id'])[0]
            # print(folder_id)

            # delete file & upload file
            
            del_file_drive(drive, folder_id)
            done_export, num_exported_folder = upload_file_drive(
                drive, dir, folder_id, title, done_export, report_dict, i, num_exported_folder)

            flag_upload += 1
        except Exception as e:
            print(e)
            cant_export = pd.concat([cant_export, report_dict[i]])
            flag_cant_export += 1

        k += 1
        if k % 3 == 0:
            time.sleep(1)
    print(f'No. uploaded file: {flag_upload}')
    print(f'No. cant export file: {flag_cant_export}')
    return done_export, cant_export, num_exported_folder
    # if k % 100 == 0:
    #     gc, drive = connect_drive(bi_key)
    #     print("Drive reconnected")
def upload_type_2(drive, report_shipper, shipper_folder, shipper_folder_id, path, dir, done_export, cant_export):
    print('UPLOADING FILE TYPE 2 ...')

    flag_create = 0
    flag_cant_export = 0

    folder_drive_shortage = pd.merge(report_shipper, shipper_folder, on=[
                                     'f_name'], how='outer', indicator=True).query('_merge=="left_only"')

    # format columns
    cur_date = datetime.today().strftime("%d-%m-%Y")
    folder_drive_shortage['Ngày tạo đơn'] = [datetime.strptime(
        i, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in list(folder_drive_shortage['Ngày tạo đơn'])]
    folder_drive_shortage['Tên đối tác'] = ["CO " + unidecode.unidecode(
        i.shipper_name_rut_gon) + " " + cur_date for i in folder_drive_shortage.itertuples()]
    folder_drive_shortage.rename(
        columns={'Instruction': 'Hướng dẫn giao hàng'}, inplace=True)
    folder_drive_shortage["Kết quả"], folder_drive_shortage["Ghi chú"] = '', ''

    # Split file
    report_dict_no_folder = {i: folder_drive_shortage[folder_drive_shortage['f_name'] == i] for i in set(
        folder_drive_shortage['f_name'])}

    li_new_shipper_id, li_new_folder_name, li_new_folder_link = [], [], []

    k = 1
    for i in list(set(folder_drive_shortage['f_name'])):
        final_report = report_dict_no_folder[i][["Mã", "Tên khách hàng", "Tên đối tác", "Số điện thoại", "Địa chỉ",
                                                 "Hướng dẫn giao hàng", "Lý do", "Ngày tạo đơn", "Số lần giao", "Kết quả", "Ghi chú"]].copy(deep=True)
        final_report = final_report.applymap(
            lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)
        try:
            # format report, create excel file
            title = add_data_to_sheet(final_report, path)

            # create and upload new shipper folder to drive
            new_folder_name = "CO " + \
                unidecode.unidecode(list(report_dict_no_folder[i]['shipper_name_rut_gon'])[
                                    0]).upper().strip()
            new_folder = drive.CreateFile({
                'title': new_folder_name,
                'parents': [{'id': shipper_folder_id}],
                'mimeType': 'application/vnd.google-apps.folder'})
            new_folder.Upload()
            folder_id = new_folder['id']

            # upload shipper report to drive shipper folder
            file_name = os.path.join(dir, f'{title}.xlsx')
            shipper_report = drive.CreateFile({
                'parents': [{'id': folder_id}],
                'title': f'{title}.xlsx'
            })
            shipper_report.SetContentFile(file_name)
            shipper_report.Upload()
            flag_create += 1

            # add folder id to output sheet to get shipper response
            report_dict_no_folder[i]['f_id'] = folder_id

            li_new_shipper_id.append(
                list(report_dict_no_folder[i]['shipper_id'])[0])
            li_new_folder_name.append(new_folder_name)
            li_new_folder_link.append(
                f"https://drive.google.com/drive/u/0/folders/{folder_id}")
            done_export = pd.concat([done_export, report_dict_no_folder[i]])
            k += 1
        except Exception as e:
            print(e)
            cant_export = pd.concat([cant_export, report_dict_no_folder[i]])
            flag_cant_export += 1

        if k % 20 == 0:
            time.sleep(1)
            #     gc, drive = connect_drive(bi_key)
            #     print("Drive reconnected")
    print(f'No. create folder: {flag_create}')
    print(f'No. cant export file: {flag_cant_export}')
    return li_new_shipper_id, li_new_folder_name, li_new_folder_link, done_export, cant_export

# Re-upload file that cannot export
def reup_cant_export_file(drive, cant_export, dir, done_export, report_dict, num_exported_folder, bi_key):
    error_export = pd.DataFrame()
    flag = True
    if cant_export.shape[0] > 0:
        print("Error data exists")
        cant_export_dict = {
            i: cant_export[cant_export['f_name'] == i] for i in set(cant_export['f_name'])}
        # k = 1
        for i in list(set(cant_export['f_name'])):
            final_report = cant_export_dict[i][["Mã", "Tên khách hàng", "Tên đối tác", "Số điện thoại", "Địa chỉ", "Hướng dẫn giao hàng",
                                                "Lý do", "Ngày tạo đơn", "Số lần giao", "Kết quả", "Ghi chú"]].copy(deep=True)
            final_report = final_report.applymap(
                lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)

            try:
                # format report, save as excel file, shipper_folder_id
                title = add_data_to_sheet(final_report)
                folder_id = list(cant_export_dict[i]['f_id'])[0]
                print(folder_id)

                # delete file & upload file
                del_file_drive(drive, folder_id)
                upload_file_drive(drive, dir, folder_id, title,
                                  done_export, report_dict, i, num_exported_folder)
            except Exception as e:
                print(e)
                error_export = pd.concat([error_export, cant_export_dict[i]])
                flag = False
            # k += 1
            # if k % 100 == 0:
            #     gc, drive = connect_drive(bi_key)
            #     print("Drive reconnected")
    else:
        print("Not any error data exist")
        pass
    return error_export, flag

# Zip file and upload to Internal folder
def find_duplicated_zipfile(drive, find_date, parents_id):
    li_files = []
    folder_id = ''
    cur_date = datetime.today().strftime("%d-%m-%Y")
    li_folders = get_li_files(drive, parents_id)
    for folder in li_folders:
        if folder['title'][-10:] == find_date:
            temp = get_li_files(drive, folder['id'])
            li_files = [file for file in temp if file['title'][:-4] == cur_date]
            folder_id = folder['id']
    return li_files, folder_id
def upload_zip_to_internal_folder(path, dir, drive, internal_folder_id):
    cur_date = datetime.today().strftime("%d-%m-%Y")

    find_date = datetime.today().strftime("%Y-%m-%d")

    li_duplicated_files, upload_folder_id = find_duplicated_zipfile(drive, find_date, internal_folder_id)
    
    if len(li_duplicated_files) > 0:
        del_file_zip_drive(drive, li_duplicated_files)


    with zipfile.ZipFile(f"{path}/{cur_date}.zip", mode="w") as archive:
        for file_path in dir.rglob("*.xlsx"):
            archive.write(file_path, arcname=file_path.relative_to(dir))

    for zip_file in dir.rglob(f"*.zip"):
        file_name = os.path.join(dir, zip_file)
        if upload_folder_id == '':
            upload_folder_id = create_internal_folder(drive, internal_folder_id)
        
        internal_report = drive.CreateFile({
            'parents': [{'id': upload_folder_id}],
            'title': f'{cur_date}'
        })
        internal_report.SetContentFile(file_name)
        internal_report.Upload()

# Delete file from directory
def del_file_in_directory(dir, file_type):
    for file_path in dir.rglob(file_type):
        try:
            os.remove(file_path)
        except Exception as e:
            print(e)

# OUTPUT
def output(output_sheet, li_new_shipper_id, li_new_shipper_name, li_new_shipper_folder_link, done_export, report, shipper_info, error_export):
    # New shipper
    new_shipper = pd.DataFrame(columns=['shipper_id', 'folder_name', 'folder_link'],
                               data=zip(li_new_shipper_id, li_new_shipper_name, li_new_shipper_folder_link))

    # Done exporting data
    done_export = done_export[['Lý do', 'Mã', 'Ngày tạo đơn', 'Số lần giao', 'Hướng dẫn giao hàng',
                               'Tên khách hàng', 'shipper_id', 'Tên đối tác', 'Địa chỉ',
                               'Số điện thoại', 'shipper_name', 'shipper_name_rut_gon', 'status',
                               'f_name', 'f_id']].copy(deep=True)
    done_export.fillna('-', inplace=True)

    # No missing shipper info
    shipper_info_shortage = pd.merge(
        report, shipper_info, on='shipper_id', how='outer', indicator=True).query('_merge=="left_only"')
    shipper_info_shortage.drop(columns=['_merge'], inplace=True)
    shipper_info_shortage = shipper_info_shortage[shipper_info_shortage.columns[0:10]].copy(
        deep=True)

    # Result
    result = {'No. of TrackingID done exported': done_export.shape[0],
              'No. of Report done exported': len(done_export['shipper_id'].unique()),
              'No. of Shipper have no info': len(set(shipper_info_shortage['shipper_id'])),
              'No. of New Shipper': new_shipper.shape[0]
              }

    # Update result into Sheet "Output Check"

    # Define worksheets
    # sheet = output_sheet.worksheet("result")
    # last_runtime = sheet.get("B2")[0][0][:10]

    output1 = output_sheet.worksheet('done_export')
    output2 = output_sheet.worksheet('no_shipper_info')
    output3 = output_sheet.worksheet('new_shipper')
    output4 = output_sheet.worksheet('result')
    output5 = output_sheet.worksheet('error_export')

    # Done export
    result_date = output4.get("B2")[0][0][:10]
    is_cur_date_data = True if result_date == datetime.today().strftime("%d-%m-%Y") else False  #ternary operator
    
    if is_cur_date_data:
        output1.clear()
        output1.update([done_export.columns.values.tolist()] +
                   done_export.values.tolist())
    else:
        # for row in done_export.values.tolist():
        #     output1.append_row(row)
        output1.append_rows(done_export.values.tolist())

    output2.clear()
    output2.update([shipper_info_shortage.columns.values.tolist()
                    ] + shipper_info_shortage.values.tolist())

    output3.clear()
    output3.update([new_shipper.columns.values.tolist()] +
                   new_shipper.values.tolist())

    output4.update("B2", str(
        f'{datetime.today().strftime("%d-%m-%Y")} {datetime.now().strftime("%H:%M:%S")}'))
    output4.update("A5:D5", [list(result.values())])

    output5.clear()
    output5.update([error_export.columns.values.tolist()] +
                   error_export.values.tolist())

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
    #sheet Output
    # output_sheet = gc.open_by_key("16Old5szbBUNVZ6lwRoY9O4sl_6FVHwXOO0a5jKg4Em4") #test
    output_sheet = gc.open_by_key("1JvkWaECyz6FVdvm8kOkYJD1z7utS97UPs_Hp1KXfJ0c")

    #sheet Input (shipper info + tracking_id)
    # input = gc.open_by_key('1nsIYsze2SWaDT6WxWkwCsXiK_E7RHfx-5dLpNaXtwbw') #test
    input = gc.open_by_key('1NOPFnRDrVwZW9rZvpHNkFa_r5edMYNX_hc2O-SBeS8k')

    #CO TONG FOLDER
    # shipper_folder_id = '1X12fJDQngElzhrWXvkCijasOlR8VJ0W3' #test
    shipper_folder_id = '1-BVDcQPpTLCRFiqgqJIzhGq5iZDF3cLu'

    #CO TONG INTERNAL FOLDER 
    # internal_folder_id = '16pptwWUnTjzeaWCIrER1zlVJLhBEhLzp' #test
    internal_folder_id = '1-3fWSXSa50-H0g9H5PfOzyKstC9ohz69'

    #REDASH API key
    api_key = 'zCIauz80VN7WzfbldWzoPg1uCPGOuSpnJm1NflMN'
    # Query ID
    query_id = 171

    success_flag = True
    shipper_info = read_shipper_info(input)
    li_tracking_id = read_tracking_id(input)
    shipper_folder = import_shipper_folder(drive, shipper_folder_id, input)
    report = running_redash(li_tracking_id, query_id, api_key)

    report_shipper, report_full = merge_report(report, shipper_info, shipper_folder)
    report_dict = split_report(report_full)

    done_export = pd.DataFrame()
    cant_export = pd.DataFrame()
    num_exported_folder = 0

    # Upload files belong to new shipper whose folder haven't existed in DRIVE
    li_new_shipper_id, li_new_folder_name, li_new_folder_link, done_export, cant_export = upload_type_2(
        drive, report_shipper, shipper_folder, shipper_folder_id, path, directory, done_export, cant_export)

    # Upload files that belong to old shipper
    done_export, cant_export, num_exported_folder = upload_type_1(drive, report_full, report_dict, done_export, 
                    cant_export, num_exported_folder, path, directory)

    # Upload files that have error when uploading
    error_export, success_flag = reup_cant_export_file(drive, cant_export, directory, done_export, report_dict, num_exported_folder, bi_key)

    # Upload zip file to internal folder
    upload_zip_to_internal_folder(path, directory, drive, internal_folder_id)
    
    # Remove created files in local storage
    excel_file = "*.xlsx"
    zip_file = "*.zip"
    del_file_in_directory(directory, excel_file)
    del_file_in_directory(directory, zip_file)

    # Check whether exporting process have an error
    if success_flag:
       output(output_sheet, li_new_shipper_id, li_new_folder_name, li_new_folder_link,
       done_export, report, shipper_info, error_export)
    else:
       print(f'!!! ERROR WHEN UPLOADING FILE, PLEASE RE-RUNNING TOOL !!!')
    print(f'Execution time: {time.time() - start_time}')


if __name__ == '__main__':
    input("Press ENTER to run tool!")
    main()
    input("Press ENTER to close tool!")