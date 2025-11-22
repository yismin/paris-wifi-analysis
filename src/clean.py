import os
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from scipy import stats
warnings.filterwarnings("ignore")
load_dotenv()

print("Paris WiFi — Data Cleaning\n")
# 1-LOAD DATA
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

df = pd.read_sql("SELECT * FROM wifi_sessions", engine)
print(f"Loaded {len(df):,} rows")
# 2-COLUMN SELECTION & BASIC CLEANING
keep = [
    'datetime','endtime_or_dash','duration','temps_de_sessions_en_minutes',
    'nom_site','cp','arc_adresse','latitude','longitude','code_site',
    'device_portal_format','device_operating_system_name_version',
    'device_browser_name_version','donnee_entrante_go','donnee_sortante_gigaoctet',
    'incomingzonelabel','nombre_de_borne_wifi','userlanguage'
]
df = df[keep].copy()

# Convert datetimes
for col in ['datetime', 'endtime_or_dash']:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Convert categoricals
categoricals = ['nom_site','cp','device_portal_format','incomingzonelabel','userlanguage','code_site']
df[categoricals] = df[categoricals].astype("category")

print(f"Reduced to {len(df.columns)} columns")

# Missing values
df.dropna(subset=['datetime','nom_site','latitude','longitude'], inplace=True)
df[['temps_de_sessions_en_minutes','donnee_entrante_go','donnee_sortante_gigaoctet']] = \
    df[['temps_de_sessions_en_minutes','donnee_entrante_go','donnee_sortante_gigaoctet']].fillna(0)

print(f" Cleaned: {len(df):,} rows retained")
# 3-FEATURE ENGINEERING
# Time features
df['hour'] = df['datetime'].dt.hour
df['day_of_week'] = df['datetime'].dt.dayofweek
df['day_name'] = df['datetime'].dt.day_name()
df['month'] = df['datetime'].dt.month
df['is_weekend'] = df['day_of_week'].isin([5, 6])

df['time_of_day'] = pd.cut(
    df['hour'],
    bins=[-1,6,9,12,14,18,22,24],
    labels=['Night','Early Morning','Morning','Lunch','Afternoon','Evening','Late Night']
)

# Device classification
def device_type(x):
    x = str(x).lower()
    if any(k in x for k in ["smart", "mobile"]): return "Mobile"
    if any(k in x for k in ["desktop", "ordinateur"]): return "Desktop"
    return "Other"

df['device_category'] = df['device_portal_format'].apply(device_type)

# Geography
df['arrondissement'] = df['cp'].astype(str).str[-2:].astype('category')

# Data usage
df['total_data_mb'] = df['donnee_entrante_go'] + df['donnee_sortante_gigaoctet']
df['data_per_minute'] = df['total_data_mb'] / (df['temps_de_sessions_en_minutes'] + 0.01)

# Flags
df['is_extreme_duration'] = df['temps_de_sessions_en_minutes'] > 1440
df['is_heavy_user'] = df['donnee_entrante_go'] > 1000

print(" Added engineered features")
# 4- LOCATION CLASSIFICATION
def classify_location(row):
    name = str(row['nom_site']).lower()
    arr = str(row['arrondissement'])

    checks = [
        (['hugo','musee','musée','crypte','tour saint jacques','invalides','pantheon','panthéon'],
         'Cultural-Tourist'),
        (['hdv','parvis','mairie','hotel'], 'High-Traffic'),
        (['bib','bibliotheque','bibliothèque'], 'Library-Service')
    ]

    for keywords, label in checks:
        if any(k in name for k in keywords):
            return label

    if any(k in name for k in ['jard','parc','berges','seine','pont']) and arr in ['01','04','07','08']:
        return 'Tourist-Park'

    if arr in ['01','04','08','16','18']:
        return 'Mixed-Tourist-Arrond'

    return 'Residential'

df['location_type'] = df.apply(classify_location, axis=1)

print("\nLOCATION TYPE DISTRIBUTION:")
for k,v in df['location_type'].value_counts().items():
    print(f"   {k:25s}: {v:7,} ({v/len(df)*100:5.1f}%)")

# 5-SAVE RESULTS
Path("data/processed").mkdir(parents=True, exist_ok=True)
df.to_csv("data/processed/wifi_cleaned.csv", index=False)
print("\nCLEANING COMPLETE!")