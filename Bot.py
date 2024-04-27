import gspread
import sqlite3
import time
from oauth2client.service_account import ServiceAccountCredentials

PATH_TO_CREDS = 'creds.json'
DB = 'prices.db'

def ingest_data(data, db: str = DB):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    ts = int(time.time())

    query = """
    INSERT INTO pricesTS (timestamp, set_name, pack_sold, current_price)
    VALUES (?, ?, ?, ?);
    """

    for key in data:
        cursor.execute(query, (ts, key, data[key]['sold'], data[key]['current']))
    
    conn.commit()
    conn.close()


def read_sheet(url, worksheet):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(PATH_TO_CREDS, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(url)
    sheet_instance = sheet.get_worksheet(worksheet) #TODO
    cells = sheet_instance.get_all_values()
    packs_data = dict()
    for cell in cells[2:-2]: 
        packs_data[cell[1]] = {
            "sold":cell[3],
            'current': cell[4]
        }   
    return packs_data


def main():
    n = 0
    while True:
        data = read_sheet("https://docs.google.com/spreadsheets/d/1S1iIyqQdKLjjj-KMzD7QXWTB12M8RsTCmpIp2LHI8mI/edit#gid=1014611448", 1)
        ingest_data(data, DB)
        print(f"Data colection Run #{n}, ts: {time.time()}")
        n+=1
        time.sleep(300)


if __name__=="__main__":
    main()

