import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from pymongo import MongoClient
import sys
import os

# ================================
# ⚙️ CONFIG
# ================================
MONGO_URI = os.getenv("MONGO_URI")
PHPSESSID = "p8hfl9pthhqirp08jm0khmituj"

DB_NAME = "atms"
COLLECTION_NAME = "vehicle_daily"

URL = "https://www.mena-atms.com/report/print.out/print.excel/type/vehicle.daily.transaction"


# ================================
# 🧠 FUNCTION: GET LAST WORKING DAY
# ================================
def get_last_working_day():
    d = datetime.now() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%d/%m/%Y")


# ================================
# 📥 FUNCTION: DOWNLOAD + PROCESS
# ================================
def fetch_data(session, t_date, fleet_group_id):
    print(f"🚚 Fetch fleet_group_id: {fleet_group_id}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://www.mena-atms.com/report/excel/index.excel/type/vehicle.daily.transaction?t_date={t_date}"
    }

    data = {
        "fleet_group_id": str(fleet_group_id),
        "fleet_id": "",
        "t_date": t_date,
        "num_of_day": "1",
        "submit": "พิมพ์",
        "display_type": "multiple-day",
        "report_type": "vehicle.daily.transaction"
    }

    response = session.post(
        URL,
        headers=headers,
        data=data,
        verify=False,
        stream=True
    )

    content = b''.join(response.iter_content(8192))

    if not content.startswith(b"PK"):
        print(f"❌ Not Excel (fleet {fleet_group_id})")
        return pd.DataFrame()

    df = pd.read_excel(BytesIO(content), skiprows=2, engine="openpyxl")

    df.columns = df.columns.str.strip()
    df = df.dropna(how="all")

    df = df[['Unnamed: 1', 'Unnamed: 3', 'ชื่อ', 'เบอร์รถ', 'ทะเบียน', 'สถานะ', 'คนขับ', 'รหัส.1', 'ชื่อ.1']]

    df = df.rename(columns={
        'Unnamed: 1': 'ฟลีท',
        'Unnamed: 3': 'ลูกค้า',
        'ชื่อ': 'แพล้นท์'
    })

    # 👇 เพิ่ม source fleet
    df["fleet_group_id"] = fleet_group_id

    return df


# ================================
# 🚀 MAIN ETL
# ================================
def run():
    print("🚀 Start ETL...")

    t_date = get_last_working_day()
    print(f"📅 Using date: {t_date}")

    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    # ================================
    # 🔁 LOOP FLEET
    # ================================
    fleet_ids = [1, 2]
    all_df = []

    for fleet_id in fleet_ids:
        df = fetch_data(session, t_date, fleet_id)

        if not df.empty:
            all_df.append(df)

    if len(all_df) == 0:
        print("❌ No data from all fleets")
        sys.exit(1)

    # ================================
    # 📊 CONCAT
    # ================================
    df = pd.concat(all_df, ignore_index=True)

    # ================================
    # 🧠 ADD DATE
    # ================================
    df["t_date"] = t_date

    print(f"📊 Final shape: {df.shape}")

    # ================================
    # 🔗 MONGO CONNECT
    # ================================
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # ================================
    # 🔥 TRUNCATE
    # ================================
    print("🔥 Dropping collection...")
    collection.drop()

    collection = db[COLLECTION_NAME]

    # ================================
    # ⚡ INDEX
    # ================================
    collection.create_index([("t_date", 1)])
    collection.create_index([("ทะเบียน", 1)])
    collection.create_index([("fleet_group_id", 1)])

    # ================================
    # 🚀 INSERT
    # ================================
    records = df.to_dict("records")

    if records:
        collection.insert_many(records)
        print(f"✅ Inserted: {len(records)}")
    else:
        print("⚠️ No data")

    print("🎉 ETL SUCCESS")


# ================================
# ▶️ RUN
# ================================
if __name__ == "__main__":
    run()