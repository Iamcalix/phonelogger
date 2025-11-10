import gspread
from google.oauth2.service_account import Credentials
import time

# ------------------ Google Sheets Setup ------------------
SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CREDS_FILE = 'credentials.json'
SHEET_ID = '1lLE2Tt9_4pnnjJIC0GDuaqYP7XV2jB9jJZwIaqq7EXE'  # Replace if needed

# ------------------ Connect to Google Sheets ------------------
try:
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPE)
    client = gspread.authorize(creds)

    sheet1 = client.open_by_key(SHEET_ID).sheet1               # Source Sheet
    sheet2 = client.open_by_key(SHEET_ID).worksheet('AcceptedData')  # Destination Sheet

    print(f"✅ Connected to Spreadsheet: {client.open_by_key(SHEET_ID).title}")
except Exception as e:
    print(f"❌ Error connecting to Google Sheets: {str(e)}")
    exit()


# ------------------ Move Accepted Rows Function ------------------
def move_accepted_rows():
    try:
        # Fetch all data from source sheet
        sheet1_data = sheet1.get_all_values()
        if not sheet1_data or len(sheet1_data) <= 1:
            print("⚠️ No data to process in Sheet1.")
            return

        headers = sheet1_data[0]
        data_rows = sheet1_data[1:]

        # Column index detection
        try:
            comments_index = headers.index('Comments')
            processed_index = headers.index('Processed')
            phone_index = headers.index('phone')
        except ValueError as e:
            print(f"❌ Column not found: {e}")
            return

        # Fetch data from destination sheet
        sheet2_data = sheet2.get_all_values()
        first_empty_row = len(sheet2_data) + 1 if sheet2_data else 2

        # Collect existing phone numbers in AcceptedData to avoid duplicates
        sheet2_phones = set()
        if sheet2_data and 'phone' in [h.lower() for h in sheet2_data[0]]:
            phone_col_index = [h.lower() for h in sheet2_data[0]].index('phone')
            for row in sheet2_data[1:]:
                if len(row) > phone_col_index and row[phone_col_index].strip():
                    sheet2_phones.add(row[phone_col_index].strip())

        rows_moved = 0

        # Loop through each row in sheet1
        for i, row in enumerate(data_rows, start=2):  # 2 = row after header
            if len(row) <= max(comments_index, processed_index, phone_index):
                continue

            phone_value = row[phone_index].strip()
            is_processed = str(row[processed_index]).strip().lower() == "yes"
            comment_value = str(row[comments_index]).strip().lower()

            if "accepted" in comment_value and not is_processed and phone_value and phone_value not in sheet2_phones:
                try:
                    # Insert row into AcceptedData
                    sheet2.insert_row(row, first_empty_row)
                    first_empty_row += 1

                    # Mark row as processed in sheet1
                    sheet1.update_cell(i, processed_index + 1, "Yes")

                    sheet2_phones.add(phone_value)
                    rows_moved += 1

                    print(f"➡️ Moved row {i} to AcceptedData at row {first_empty_row - 1}")
                    time.sleep(1)  # Avoid API rate limit
                except Exception as e:
                    print(f"❌ Error processing row {i}: {str(e)}")

        if rows_moved == 0:
            print("✅ No new rows to move.")

    except Exception as e:
        print(f"❌ Error in move_accepted_rows: {str(e)}")


# ------------------ Run Script Continuously ------------------
while True:
    move_accepted_rows()
    time.sleep(2)
