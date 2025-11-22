import os, time, requests, pandas as pd, psycopg2
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

class MinimalParisWiFiExtractor:
    def __init__(self, db_params, max_batch_size=100):
        self.base_url = (
            "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
            "paris-wi-fi-utilisation-des-hotspots-paris-wi-fi/records"
        )
        self.db_params = db_params
        self.conn = None
        self.max_batch_size = int(max_batch_size)
        self.raw_dir = Path.cwd() / "data" / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print("DB connected"); return True
        except Exception as e:
            print("DB connection error:", e); return False

    def create_table(self):
        q = """
        DROP TABLE IF EXISTS wifi_sessions CASCADE;
        CREATE TABLE wifi_sessions (
            id SERIAL PRIMARY KEY,
            code_site TEXT,
            datetime TIMESTAMP,
            duration INTEGER,
            nom_site TEXT,
            cp TEXT,
            device_portal_format TEXT,
            bytesin BIGINT,
            bytesout BIGINT,
            latitude NUMERIC,
            longitude NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_datetime ON wifi_sessions(datetime);
        """
        try:
            cur = self.conn.cursor(); cur.execute(q)
            self.conn.commit(); cur.close()
            print("Table created"); return True
        except Exception as e:
            print("Create table error:", e); return False

    def fetch_data_batch(self, limit=100, offset=0, max_retries=3):
        if limit > self.max_batch_size: limit = self.max_batch_size
        params = {"limit": limit, "offset": offset}
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(self.base_url, params=params, timeout=(4,10))
                if resp.status_code == 429:
                    wait = 10 * attempt; print(f"Rate limited, sleeping {wait}s")
                    time.sleep(wait); continue
                resp.raise_for_status(); return resp.json()
            except requests.exceptions.Timeout:
                wait = 2 * attempt
                print(f"Timeout {attempt}/{max_retries}, sleeping {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                print(f"Request error {attempt}/{max_retries}: {e}")
                if attempt < max_retries: time.sleep(2 * attempt)
                else: return None
        return None

    def prepare_record_for_db(self, fields):
        g = fields.get
        gp = g("geo_point_2d") or g("geo_point")
        lat = lon = None
        if isinstance(gp, (list,tuple)) and len(gp)>=2: lat, lon = gp
        elif isinstance(gp, dict):
            lat = gp.get("lat") or gp.get("latitude")
            lon = gp.get("lon") or gp.get("longitude")
        else:
            lat = g("latitude") or g("lat")
            lon = g("longitude") or g("lon")
        return {
            "code_site": g("code_site") or g("codeSite") or g("code"),
            "datetime": g("datetime"),
            "duration": g("duration"),
            "nom_site": g("nom_site") or g("nomSite") or g("nom"),
            "cp": g("cp"),
            "device_portal_format": g("device_portal_format"),
            "bytesin": g("bytesin"),
            "bytesout": g("bytesout"),
            "latitude": lat,
            "longitude": lon
        }

    def insert_records_batch(self, records, batch_size=500):
        if not records: return
        q = """
        INSERT INTO wifi_sessions
        (code_site, datetime, duration, nom_site, cp, device_portal_format,
         bytesin, bytesout, latitude, longitude)
        VALUES (%(code_site)s, %(datetime)s, %(duration)s, %(nom_site)s,
                %(cp)s, %(device_portal_format)s, %(bytesin)s, %(bytesout)s,
                %(latitude)s, %(longitude)s)
        """
        cur = self.conn.cursor()
        prepared = [
            self.prepare_record_for_db(r.get("fields") or r)
            for r in records
        ]
        total = len(prepared)
        for i in range(0, total, batch_size):
            batch = prepared[i:i+batch_size]
            try:
                cur.executemany(q, batch); self.conn.commit()
                print(f"Inserted {min(i+batch_size,total)}/{total} rows", end="\r")
            except Exception as e:
                self.conn.rollback(); print("Insert error:", e)
        cur.close(); print()

    def fetch_strategic_sample(self, target_size=200000):
        print(f"Fetching {target_size} rows (batch {self.max_batch_size})...")
        if not self.fetch_data_batch(limit=1, offset=0):
            print("API test failed"); return []
        offsets = list(range(0,10000,self.max_batch_size)) or [0]
        batches = (target_size + self.max_batch_size - 1)//self.max_batch_size
        collected, oi, start = [], 0, time.time()
        for b in range(batches):
            offset = offsets[oi]; oi = (oi+1)%len(offsets)
            raw = self.fetch_data_batch(limit=self.max_batch_size, offset=offset)
            if raw: collected.extend(raw)
            else: time.sleep(1)
            time.sleep(0.8)
            print(f"Batch {b+1}/{batches} fetched — collected {len(collected)} rows", end='\r')
            if len(collected)>=target_size: break
            if (b+1)%100==0:
                print(f"Batch {b+1}/{batches} — {len(collected)} rows — {int(time.time()-start)}s")
        print(f"Finished fetching: collected {len(collected)} rows")
        return collected[:target_size]

    def export_to_csv(self, filename=None):
        filename = filename or f"paris_wifi_{datetime.now():%Y%m%d_%H%M%S}.csv"
        path = self.raw_dir / filename
        df = pd.read_sql("SELECT * FROM wifi_sessions", self.conn)
        df.to_csv(path, index=False)
        print("CSV exported to", path); return path

    def run_full_pipeline(self, target_size=200000, export_csv=False):
        if not self.connect_db(): return False
        if not self.create_table(): return False
        items = self.fetch_strategic_sample(target_size)
        if not items: print("No items fetched"); return False
        self.insert_records_batch(items, batch_size=500)
        if export_csv: self.export_to_csv()
        return True

    def close(self):
        if self.conn: self.conn.close()

if __name__=="__main__":
    SAMPLE_SIZE = 200000
    EXPORT_CSV = True
    extractor = MinimalParisWiFiExtractor(DB_CONFIG)
    try:
        ok = extractor.run_full_pipeline(SAMPLE_SIZE, EXPORT_CSV)
        print("Pipeline completed" if ok else "Pipeline failed")
    finally:
        extractor.close()