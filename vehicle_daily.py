import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import certifi
from pymongo import MongoClient
import sys




# ================================
# 🧠 FUNCTION: GET LAST WORKING DAY
# ================================
def get_last_working_day():
    d = datetime.now() - timedelta(days=1)

    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= timedelta(days=1)

    return d.strftime("%d/%m/%Y")


# ================================
# 🚀 MAIN ETL
# ================================
def run():
    print("🚀 Start ETL...")

    t_date = get_last_working_day()
    print(f"📅 Using date: {t_date}")

    # ================================
    # 🌐 SESSION
    # ================================
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://www.mena-atms.com/report/excel/index.excel/type/vehicle.daily.transaction?t_date={t_date}"
    }

    data = {
        "fleet_group_id": "1",
        "fleet_id": "",
        "t_date": t_date,
        "num_of_day": "1",
        "submit": "พิมพ์",
        "display_type": "multiple-day",
        "report_type": "vehicle.daily.transaction"
    }

    # ================================
    # 📥 DOWNLOAD
    # ================================
    response = session.post(
        URL,
        headers=headers,
        data=data,
        verify=False,
        stream=True
    )

    content = b''.join(response.iter_content(8192))

    if not content.startswith(b"PK"):
        print("❌ Not Excel file")
        print(content[:500])
        sys.exit(1)

    print("✅ Excel downloaded")

    # ================================
    # 📊 READ EXCEL
    # ================================
    df = pd.read_excel(BytesIO(content), skiprows=2, engine="openpyxl")

    df.columns = df.columns.str.strip()
    df = df.dropna(how="all")

    df = df[['เบอร์รถ', 'ทะเบียน', 'สถานะ', 'คนขับ', 'รหัส.1', 'ชื่อ.1']]

    # ================================
    # 🧠 ADD DATE
    # ================================
    df["t_date"] = t_date

    print(f"📊 Data shape: {df.shape}")

    # ================================
    # 🔗 MONGO CONNECT
    # ================================
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # ================================
    # 🔥 TRUNCATE (DROP)
    # ================================
    print("🔥 Dropping collection...")
    collection.drop()

    collection = db[COLLECTION_NAME]

    # ================================
    # ⚡ CREATE INDEX
    # ================================
    collection.create_index([("t_date", 1)])
    collection.create_index([("ทะเบียน", 1)])

    # ================================
    # 🚀 INSERT
    # ================================
    records = df.to_dict("records")

    if len(records) > 0:
        collection.insert_many(records)
        print(f"✅ Inserted: {len(records)}")
    else:
        print("⚠️ No data to insert")

    print("🎉 ETL SUCCESS")


# ================================
# ▶️ RUN
# ================================
if __name__ == "__main__":
    run()