import pandas as pd
import numpy as np

# === Chargement des données sources === #

weather_df = pd.read_csv("weather_meteo_by_airport.csv")
airports_df = pd.read_csv("airports_geolocation.csv")
flights_df_1 = pd.read_csv("maj us flight - january 2024.csv")
flights_df_2 = pd.read_csv("Cancelled_Diverted_2023.csv")

# === Nettoyage et préparation des fichiers source === #

# WEATHER_DIMENSION
weather_df = weather_df[['time', 'tavg', 'tmin', 'tmax', 'snow', 'airport_id']].dropna()
weather_df['time'] = pd.to_datetime(weather_df['time'])

# Création d'un Weather_ID unique (hash simple)
weather_df['Weather_ID'] = weather_df.apply(lambda row: hash((row['time'], row['airport_id'])), axis=1)
weather_dim = weather_df.drop_duplicates(subset=['Weather_ID'])

# AIRPORT_DIMENSION
airport_dim = airports_df.rename(columns={
    'IATA_CODE': 'Airport_ID',
    'AIRPORT': 'Airport_Name',
    'CITY': 'CityName',
    'STATE': 'State',
    'COUNTRY': 'Country',
    'LATITUDE': 'Latitude',
    'LONGITUDE': 'Longitude'
}).dropna(subset=['Airport_ID'])

# FLIGHTS (fusion des deux fichiers de vol)
flights = pd.concat([flights_df_1, flights_df_2], ignore_index=True)

# Sélection et renommage des colonnes
flights = flights[['FlightDate', 'Day_Of_Week', 'Airline', 'Tail_Number',
                   'Dep_Airport', 'Dep_Delay', 'Dep_Delay_Type',
                   'Arr_Airport', 'Arr_Delay', 'Flight_Duration',
                   'Cancelled', 'Diverted']].copy()

flights.columns = ['FlightDate', 'Day_Of_Week', 'Airline', 'Tail_Number',
                   'Dep_Airport', 'Dep_Delay', 'Dep_Delay_Type',
                   'Arr_Airport', 'Arr_Delay', 'Flight_Duration',
                   'Cancelled', 'Diverted']

flights['FlightDate'] = pd.to_datetime(flights['FlightDate'])
flights['Cancelled'] = flights['Cancelled'].fillna(0).astype(bool)
flights['Diverted'] = flights['Diverted'].fillna(0).astype(bool)

# === Création des DIMENSIONS === #

# DATE_DIMENSION
date_dim = flights[['FlightDate', 'Day_Of_Week']].drop_duplicates().copy()
date_dim['Weekday'] = date_dim['FlightDate'].dt.day_name()
date_dim = date_dim.rename(columns={'FlightDate': 'Date'})

# DELAY_DIMENSION
def get_delay_range(delay):
    if pd.isna(delay): return 'Unknown'
    if delay <= 15: return '0-15'
    elif delay <= 30: return '15-30'
    elif delay <= 60: return '30-60'
    else: return '60+'

flights['Delay_range'] = flights['Dep_Delay'].apply(get_delay_range)
delay_dim = flights[['Dep_Delay_Type', 'Delay_range']].drop_duplicates().copy()
delay_dim['Type_id'] = delay_dim.reset_index().index + 1

# AIRLINE_DIMENSION
airline_dim = flights[['Airline']].drop_duplicates().copy()
airline_dim['Airline_ID'] = airline_dim.reset_index().index + 1

# AIRCRAFT_ID
aircraft_dim = flights[['Tail_Number']].drop_duplicates().copy()
aircraft_dim['Aircraft_ID'] = aircraft_dim.reset_index().index + 1

# === Jointures pour construire la table de faits === #

# Ajout Airline_ID
flights = flights.merge(airline_dim, on='Airline', how='left')

# Ajout Aircraft_ID
flights = flights.merge(aircraft_dim, on='Tail_Number', how='left')

# Ajout Type_id
flights = flights.merge(delay_dim, on=['Dep_Delay_Type', 'Delay_range'], how='left')

# Ajout Weather_ID (via date + dep airport)
weather_df_short = weather_dim[['Weather_ID', 'time', 'airport_id']]
weather_df_short.columns = ['Weather_ID', 'FlightDate', 'Dep_Airport']
flights = flights.merge(weather_df_short, on=['FlightDate', 'Dep_Airport'], how='left')

# Ajout Dep_Airport_ID et Arr_Airport_ID à partir du nom (on garde les IATA tels quels)
flights['Dep_Airport_ID'] = flights['Dep_Airport']
flights['Arr_Airport_ID'] = flights['Arr_Airport']

# Ajout Flight_ID
flights['Flight_ID'] = flights.reset_index().index + 1

# === Table de faits : Flight_Fact === #

fact_cols = ['Flight_ID', 'Type_id', 'Weather_ID',
             'Dep_Airport_ID', 'Arr_Airport_ID', 'Airline_ID', 'Aircraft_ID',
             'FlightDate', 'Dep_Delay', 'Arr_Delay',
             'Flight_Duration', 'Cancelled', 'Diverted']

flight_fact = flights[fact_cols].copy()

# === EXPORT FINAL (vers CSV pour Power BI) === #


flight_fact.to_csv("DWH_Flight_Fact.csv", index=False)
date_dim.to_csv("DWH_Date_Dimension.csv", index=False)
delay_dim[['Type_id', 'Delay_range', 'Dep_Delay_Type']].rename(columns={'Dep_Delay_Type': 'Delay_cause'}).to_csv("DWH_Delay_Dimension.csv", index=False)
airport_dim.to_csv("DWH_Airport_Dimension.csv", index=False)
airline_dim.to_csv("DWH_Airline_Dimension.csv", index=False)
weather_dim.to_csv("DWH_Weather_Dimension.csv", index=False)
aircraft_dim.to_csv("DWH_Aircraft_Dimension.csv", index=False)

print("✅ ETL terminé. Les fichiers sont prêts pour Power BI.")
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# --- Connexion à PostgreSQL --- #
try:
    # Crée une connexion avec SQLAlchemy pour gérer la base PostgreSQL
    engine = create_engine('postgresql+psycopg2://postgres:maria1234@localhost:5432/flights_dwh')
    with engine.connect() as connection:
        connection.execute(text("SELECT version();"))
        print("✅ Connexion réussie à PostgreSQL.")
except Exception as e:
    print("❌ Erreur de connexion à PostgreSQL :", e)
    exit()

# --- Chargement des CSV générés par l’ETL --- #
try:
    date_dim = pd.read_csv("DWH_Date_Dimension.csv")
    delay_dim = pd.read_csv("DWH_Delay_Dimension.csv")
    airport_dim = pd.read_csv("DWH_Airport_Dimension.csv")
    airline_dim = pd.read_csv("DWH_Airline_Dimension.csv")
    aircraft_dim = pd.read_csv("DWH_Aircraft_Dimension.csv")
    weather_dim = pd.read_csv("DWH_Weather_Dimension.csv")
    flight_fact = pd.read_csv("DWH_Flight_Fact.csv")
    print("📄 Fichiers CSV chargés avec succès.")
except Exception as e:
    print("❌ Erreur de chargement des fichiers CSV :", e)
    exit()

# --- Création des tables --- #
try:
    with engine.connect() as conn:
        conn.execute(text(""" 
        DROP TABLE IF EXISTS flight_fact CASCADE;
        DROP TABLE IF EXISTS date_dimension CASCADE;
        DROP TABLE IF EXISTS delay_dimension CASCADE;
        DROP TABLE IF EXISTS airport_dimension CASCADE;
        DROP TABLE IF EXISTS airline_dimension CASCADE;
        DROP TABLE IF EXISTS weather_dimension CASCADE;
        DROP TABLE IF EXISTS aircraft_dimension CASCADE;

        CREATE TABLE date_dimension (
            date DATE PRIMARY KEY,
            day_of_week INT,
            weekday TEXT
        );

        CREATE TABLE delay_dimension (
            type_id INT PRIMARY KEY,
            delay_range TEXT,
            delay_cause TEXT
        );

        CREATE TABLE airport_dimension (
            airport_id TEXT PRIMARY KEY,
            airport_name TEXT,
            cityname TEXT,
            state TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT
        );

        CREATE TABLE airline_dimension (
            airline_id INT PRIMARY KEY,
            airline TEXT
        );

        CREATE TABLE aircraft_dimension (
            aircraft_id INT PRIMARY KEY,
            tail_number TEXT
        );

        CREATE TABLE weather_dimension (
            weather_id BIGINT PRIMARY KEY,
            time DATE,
            airport_id TEXT,
            tavg FLOAT,
            tmin FLOAT,
            tmax FLOAT,
            snow FLOAT
        );

        CREATE TABLE flight_fact (
            flight_id INT PRIMARY KEY,
            type_id INT,
            weather_id BIGINT,
            dep_airport_id TEXT,
            arr_airport_id TEXT,
            airline_id INT,
            aircraft_id INT,
            flightdate DATE,
            dep_delay INT,
            arr_delay INT,
            flight_duration INT,
            cancelled BOOLEAN,
            diverted BOOLEAN,
            FOREIGN KEY (type_id) REFERENCES delay_dimension(type_id),
            FOREIGN KEY (weather_id) REFERENCES weather_dimension(weather_id),
            FOREIGN KEY (dep_airport_id) REFERENCES airport_dimension(airport_id),
            FOREIGN KEY (arr_airport_id) REFERENCES airport_dimension(airport_id),
            FOREIGN KEY (airline_id) REFERENCES airline_dimension(airline_id),
            FOREIGN KEY (aircraft_id) REFERENCES aircraft_dimension(aircraft_id)
        );
        """))
        print("🛠️ Tables créées avec succès dans la base PostgreSQL locale.")
except Exception as e:
    print("❌ Erreur lors de la création des tables :", e)
    exit()

# --- Insertion des données dans les tables PostgreSQL --- #
try:
    date_dim.to_sql("date_dimension", engine, if_exists="append", index=False)
    delay_dim.to_sql("delay_dimension", engine, if_exists="append", index=False)
    airport_dim.to_sql("airport_dimension", engine, if_exists="append", index=False)
    airline_dim.to_sql("airline_dimension", engine, if_exists="append", index=False)
    aircraft_dim.to_sql("aircraft_dimension", engine, if_exists="append", index=False)
    weather_dim.to_sql("weather_dimension", engine, if_exists="append", index=False)
    flight_fact.to_sql("flight_fact", engine, if_exists="append", index=False)

    print("✅ Données chargées avec succès dans ta base PostgreSQL locale.")
except Exception as e:
    print("❌ Erreur lors de l’insertion des données :", e)
