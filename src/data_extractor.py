# data_extractor_minimal.py
import os
import time
import requests
import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432))
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
        # where CSV will be written if requested
        self.project_root = Path.cwd()
        self.raw_dir = self.project_root / "data" / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print("DB connected")
            return True
        except Exception as e:
            print("DB connection error:", e)
            return False

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
            cur = self.conn.cursor()
            cur.execute(q)
            self.conn.commit()
            cur.close()
            print("Table created")
            return True
        except Exception as e:
            print("Create table error:", e)
            return False

    def fetch_data_batch(self, limit=100, offset=0, max_retries=3):
        """Fetch a single batch with connect/read timeouts and retries."""
        if limit > self.max_batch_size:
            limit = self.max_batch_size

        params = {"limit": limit, "offset": offset}
        for attempt in range(1, max_retries + 1):
            try:
                # connect timeout 4s, read timeout 10s
                resp = requests.get(self.base_url, params=params, timeout=(4, 10))
                if resp.status_code == 429:
                    # simple backoff
                    wait = 10 * attempt
                    print(f"Rate limited, sleeping {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                wait = 2 * attempt
                print(f"Timeout attempt {attempt}/{max_retries}, sleeping {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                print(f"Request error attempt {attempt}/{max_retries}: {e}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                else:
                    return None
        return None

    def _extract_results_list(self, raw_json):
        """Return list of record dicts in a flexible way."""
        if raw_json is None:
            return []
        # user earlier used 'results'; fall back to common keys
        if isinstance(raw_json, dict):
            if 'results' in raw_json:
                return raw_json['results']
            if 'records' in raw_json:
                return raw_json['records']
            # some endpoints return 'data' or 'rows'
            if 'data' in raw_json:
                return raw_json['data']
        return []

    def _get_fields_from_item(self, item):
        """
        Accept different shapes: item may be a dict with .get('record') or .get('fields'),
        or already be the fields mapping.
        """
        if not isinstance(item, dict):
            return {}
        if 'record' in item and isinstance(item['record'], dict):
            # some APIs: {'record': {'fields': {...}}}
            rec = item['record']
            if 'fields' in rec:
                return rec['fields']
            return rec
        if 'fields' in item:
            return item['fields']
        # if the item already looks like fields (contains typical keys)
        return item

    def prepare_record_for_db(self, fields):
        # safe-get helpers
        def g(k):
            return fields.get(k)
        # latitude/longitude may be under 'geo_point_2d' or 'latitude'/'longitude'
        lat = lon = None
        gp = fields.get('geo_point_2d') or fields.get('geo_point')
        if isinstance(gp, (list, tuple)) and len(gp) >= 2:
            lat, lon = gp[0], gp[1]
        elif isinstance(gp, dict):
            lat = gp.get('lat') or gp.get('latitude')
            lon = gp.get('lon') or gp.get('longitude')
        else:
            lat = fields.get('latitude') or fields.get('lat')
            lon = fields.get('longitude') or fields.get('lon')

        return {
            'code_site': g('code_site') or g('codeSite') or g('code'),
            'datetime': g('datetime'),
            'duration': g('duration'),
            'nom_site': g('nom_site') or g('nomSite') or g('nom'),
            'cp': g('cp'),
            'device_portal_format': g('device_portal_format'),
            'bytesin': g('bytesin'),
            'bytesout': g('bytesout'),
            'latitude': lat,
            'longitude': lon
        }

    def insert_records_batch(self, records, batch_size=500):
        """Insert prepared records (list of dicts) into DB in batches."""
        if not records:
            return
        insert_q = """
        INSERT INTO wifi_sessions
        (code_site, datetime, duration, nom_site, cp, device_portal_format, bytesin, bytesout, latitude, longitude)
        VALUES (%(code_site)s, %(datetime)s, %(duration)s, %(nom_site)s, %(cp)s, %(device_portal_format)s, %(bytesin)s, %(bytesout)s, %(latitude)s, %(longitude)s)
        """
        cur = self.conn.cursor()
        prepared = [self.prepare_record_for_db(self._get_fields_from_item(r)) for r in records]
        total = len(prepared)
        for i in range(0, total, batch_size):
            batch = prepared[i:i + batch_size]
            try:
                cur.executemany(insert_q, batch)
                self.conn.commit()
                print(f"Inserted {min(i+batch_size, total)}/{total} rows", end='\r')
            except Exception as e:
                self.conn.rollback()
                print("Insert error:", e)
        cur.close()
        print()  # newline after progress

    def fetch_strategic_sample(self, target_size=200000):
        """
        Cycle offsets in 0..9999 (API hard limit) to accumulate target_size rows.
        Returns list of raw items (not prepared).
        """
        print(f"Fetching {target_size} rows (batch {self.max_batch_size}, max offset=10000)...")
        # test API
        info = self.fetch_data_batch(limit=1, offset=0)
        if info is None:
            print("API test failed")
            return []

        # build offset list (0..9999 step batch_size)
        max_offset = 10000
        offsets = list(range(0, max_offset, self.max_batch_size))
        if not offsets:
            offsets = [0]

        total_batches_needed = (target_size + self.max_batch_size - 1) // self.max_batch_size
        collected = []
        oi = 0
        start = time.time()

        for b in range(total_batches_needed):
            offset = offsets[oi]
            oi = (oi + 1) % len(offsets)
            raw = self.fetch_data_batch(limit=self.max_batch_size, offset=offset)
            items = self._extract_results_list(raw)
            if items:
                collected.extend(items)
            else:
                # if we got nothing, small backoff
                time.sleep(1)
            # throttle lightly to avoid immediate ban
            time.sleep(0.8)
            if len(collected) >= target_size:
                break
            if (b + 1) % 100 == 0:
                elapsed = time.time() - start
                print(f"Progress batches {b+1}/{total_batches_needed} — collected {len(collected)} rows — elapsed {elapsed:.0f}s")
        print(f"Finished fetching: collected {len(collected)} rows")
        return collected[:target_size]

    def export_to_csv(self, filename=None):
        if filename is None:
            filename = f"paris_wifi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path = self.raw_dir / filename
        q = "SELECT * FROM wifi_sessions"
        df = pd.read_sql(q, self.conn)
        df.to_csv(path, index=False)
        print("CSV exported to", path)
        return path

    def run_full_pipeline(self, target_size=200000, export_csv=False):
        if not self.connect_db():
            return False
        if not self.create_table():
            return False
        items = self.fetch_strategic_sample(target_size=target_size)
        if not items:
            print("No items fetched")
            return False
        # Insert in chunks but use the existing insert_records_batch which prepares rows
        self.insert_records_batch(items, batch_size=500)
        if export_csv:
            self.export_to_csv()
        return True

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    SAMPLE_SIZE = 200000
    EXPORT_CSV = True   # set False if you don't want CSV
    extractor = MinimalParisWiFiExtractor(DB_CONFIG)
    try:
        ok = extractor.run_full_pipeline(target_size=SAMPLE_SIZE, export_csv=EXPORT_CSV)
        if ok:
            print("Pipeline completed")
        else:
            print("Pipeline failed")
    finally:
        extractor.close()
