import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
# 1. Load environment vars
load_dotenv()
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432))
}
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
# 2. Load CSV
csv_path = r"c:/Users/yasmi/projects/paris-wifi-analysis/data/raw/paris_wifi_20251120_170502.csv"
df = pd.read_csv(csv_path)
# 3. Drop table if exists
drop_sql = """
DROP TABLE IF EXISTS wifi_sessions;
"""
create_sql = """
CREATE TABLE wifi_sessions (
    id BIGINT,
    code_site TEXT,
    datetime TIMESTAMP,
    endtime_or_dash TIMESTAMP,
    duration BIGINT,
    temps_de_sessions_en_minutes DOUBLE PRECISION,
    nom_site TEXT,
    incomingzonelabel TEXT,
    incomingnetworklabel TEXT,
    cp TEXT,
    arc_adresse TEXT,
    device_portal_format TEXT,
    device_constructor_name TEXT,
    device_operating_system_name_version TEXT,
    device_browser_name_version TEXT,
    bytesin BIGINT,
    donnee_entrante_go DOUBLE PRECISION,
    bytesout BIGINT,
    donnee_sortante_gigaoctet DOUBLE PRECISION,
    packetsin BIGINT,
    packetsout BIGINT,
    userlanguage TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    nombre_de_borne_wifi DOUBLE PRECISION,
    etat2 TEXT,
    created_at TIMESTAMP
);
"""
with engine.begin() as conn:
    conn.execute(text(drop_sql))
    conn.execute(text(create_sql))
# 4. Insert CSV into SQL
df.to_sql("wifi_sessions",engine,if_exists="append",index=False,method="multi",chunksize=5000)
print(f"\nImport complete! Table now contains: {len(df):,} rows")