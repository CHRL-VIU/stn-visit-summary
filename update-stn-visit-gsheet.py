import gspread
import pandas as pd
import numpy as np
import re
from natsort import natsorted
from update_config import credentials

def looks_like_header(row):
    """
    Returns True if a row looks like a header row:
    - Mostly non-empty strings
    - Few numeric values
    """
    if not row:
        return False
    non_empty = sum(1 for x in row if str(x).strip() != '')
    numeric_count = sum(1 for x in row if str(x).strip().replace('.', '', 1).isdigit())
    # Customize thresholds as needed:
    if non_empty > len(row) * 0.6 and numeric_count < len(row) * 0.2:
        return True
    return False

# Connect to Google Sheets
gc = gspread.service_account_from_dict(credentials)
sh = gc.open("Weather Station Visit Form")

# Get worksheet names
worksheet_objs = sh.worksheets()
ws_names = [ws.title for ws in worksheet_objs]

# Remove merged worksheet from list
if np.isin('Weather Station Visit MERGED', ws_names):
    merged_exists = True
    ws_names.remove('Weather Station Visit MERGED')
else:
    merged_exists = False

# Sort worksheets by version (newest first)
ws_names_sorted = natsorted(ws_names, reverse=True)

# Read existing merged sheet submissions if exists
existing_submissions = set()
if merged_exists:
    ws_merged = sh.worksheet('Weather Station Visit MERGED')
    merged_data = ws_merged.get_all_records()
    if merged_data:
        df_merged_existing = pd.DataFrame(merged_data)
        if 'submissionid' in df_merged_existing.columns:
            existing_submissions = set(df_merged_existing['submissionid'].astype(str).str.strip())

df_merge_list = []

for ws_name in ws_names_sorted:
    ws = sh.worksheet(ws_name)
    data = ws.get_all_values()

    # Find header row index
    header_row_idx = None
    for idx, row in enumerate(data):
        if looks_like_header(row):
            header_row_idx = idx
            break
    if header_row_idx is None:
        print(f"⚠️ Could not find a valid header row in sheet '{ws_name}', skipping this sheet.")
        continue

    header = data[header_row_idx]
    records = data[header_row_idx + 1:]
    df_ws = pd.DataFrame(records, columns=header)

    # Diagnostics: duplicates and empties
    duplicates = df_ws.columns[df_ws.columns.duplicated()].tolist()
    if duplicates:
        print(f"⚠️ Duplicate columns in sheet '{ws_name}': {duplicates}")
    empty_cols = [i for i, col in enumerate(df_ws.columns) if col.strip() == '']
    if empty_cols:
        print(f"⚠️ Empty column names in sheet '{ws_name}' at positions: {empty_cols}")

    # Apply fieldname corrections
    fld_ws = df_ws.columns.tolist()
    fld_ws = [x.replace('Course_Job.', 'Course.') for x in fld_ws]
    fld_ws = [x.replace('Enter_Snow_Core_Data.', 'Add_Snow_Core.') for x in fld_ws]
    fld_ws = [re.sub('Volume_Added$', 'Volume_Added_ml', x) for x in fld_ws]
    fld_ws = [x.replace('Snow_Course.Add_Snow_Core.Mass_Final__g_', 'Snow_Course.Add_Snow_Core.Total_Mass__g_') for x in fld_ws]
    fld_ws = [x.replace('Snow_Course.Add_Snow_Core.SWE', 'Snow_Course.Add_Snow_Core.SWE__cm_') for x in fld_ws]

    if np.isin('General_Maintenance_Notes_', df_ws.columns):
        df_ws['General_Notes'] = df_ws['General_Maintenance_Notes_'].fillna('') + df_ws['General_Notes'].fillna('')
        df_ws = df_ws.drop('General_Maintenance_Notes_', axis=1)
        fld_ws = [f for f in fld_ws if f != 'General_Maintenance_Notes_']

    df_ws.columns = fld_ws

    # Filter out rows with duplicate submissionids already in merged sheet
    if 'submissionid' in df_ws.columns:
        df_ws['submissionid'] = df_ws['submissionid'].astype(str).str.strip()
        unique_submissions = set(df_ws['submissionid'])
        new_submissions = unique_submissions - existing_submissions
        df_new = df_ws[df_ws['submissionid'].isin(new_submissions)]
        n_new = len(new_submissions)
        if n_new > 0:
            print(f"{n_new} new submissions added from sheet '{ws_name}'")
            df_merge_list.append(df_new)
            # Update existing submissions set
            existing_submissions.update(new_submissions)
        else:
            print(f"No new submissions found in sheet '{ws_name}'")
    else:
        # If no submissionid column, add all (you might want to change this behavior)
        print(f"⚠️ No 'submissionid' column found in sheet '{ws_name}'. Adding all rows.")
        df_merge_list.append(df_ws)

# Concatenate all new dataframes
if df_merge_list:
    df_merged_new = pd.concat(df_merge_list, ignore_index=True)
else:
    df_merged_new = pd.DataFrame()

if merged_exists:
    # Combine existing with new
    if not df_merged_new.empty:
        df_merged = pd.concat([df_merged_existing, df_merged_new], ignore_index=True)
    else:
        df_merged = df_merged_existing
else:
    df_merged = df_merged_new

if not df_merged.empty:
    # Convert Job_Start_Time to datetime
    if 'Job_Start_Time' in df_merged.columns:
        df_merged['Job_Start_Time'] = pd.to_datetime(df_merged['Job_Start_Time'], errors='coerce')
        # Sort descending
        df_merged_sorted = df_merged.sort_values(by='Job_Start_Time', ascending=False)
        df_merged_sorted['Job_Start_Time'] = df_merged_sorted['Job_Start_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        df_merged_sorted = df_merged

    df_merged_sorted.fillna('', inplace=True)
    fld_merged_sorted = df_merged_sorted.columns.tolist()

    if not merged_exists:
        rows, cols = df_merged_sorted.shape
        ws_merged = sh.add_worksheet(title="Weather Station Visit MERGED", rows=str(rows + 1), cols=str(cols), index=0)
        ws_merged.insert_row(fld_merged_sorted, 1)
    else:
        ws_merged = sh.worksheet("Weather Station Visit MERGED")
        # Clear before update
        ws_merged.clear()

    # Update merged sheet with headers + data
    ws_merged.update([fld_merged_sorted] + df_merged_sorted.values.tolist())

else:
    print("⚠️ No data to write to merged sheet.")

print('Google Sheet "Weather Station Visit Form" has been updated.')
