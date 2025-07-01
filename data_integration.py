import numpy as np
import psycopg2
import sqlalchemy
import pandas as pd
import os
from sqlalchemy import create_engine

# This file contains all the necessary code to integrate the data from the csv files into our database.
# To be able to run this one needs to have all the csv files in the correct path, or change the path associated to
# the csv files.
# Before running this, one needs to have created all the necessary tables in the database already (createTables.sql).

# This is the link to the database
DATABASE_CON = 'postgresql+psycopg2://postgres:admin@localhost:5432/weather_transport_data'

# Create database connection
engine = create_engine(DATABASE_CON)


# Imports the data for all the weather stations
def importWeatherStation():
    csv_file_path = 'datasets/weather/weatherStation.csv'
    weather = pd.read_csv(csv_file_path, delimiter=';', low_memory=False, encoding='ISO-8859-1')

    # Map CSV column names to database column names (CSV column names → Database column names)
    column_mapping = {
        'Station': 'station',
        'station/location': 'weatherstationname',
        'WIGOS-ID': 'wigosid',
        'Data since': 'datasince',
        'Station height m. a. sea level': 'stationheight',
        'CoordinatesE': 'coorde',
        'CoordinatesN': 'coordn',
        'Latitude': 'lat',
        'Longitude': 'long',
        'Climate region': 'climateregion',
        'Canton': 'canton'
    }

    weather.rename(columns=column_mapping, inplace=True)
    weather.drop(columns=['URL Previous years (verified data)', 'URL Current year'], inplace=True)

    # Insert data into the WeatherStation table
    weather.to_sql('weatherstation', engine, if_exists='append', index=False)


# Imports the measurements of every weather station for 2024
def importWeatherMeasurements():
    column_mapping = {
        'station/location': 'weatherstationname',
        'date': 'date',
        'gre000d0': 'globalradiation',
        'hto000d0': 'totalsnowdepth',
        'nto000d0': 'cloudcover',
        'prestad0': 'pressure',
        'rre150d0': 'precipitation',
        'sre000d0': 'sunshineduration',
        'tre200d0': 'airtemperature_mean',
        'tre200dn': 'airtemperature_min',
        'tre200dx': 'airtemperature_max',
        'ure200d0': 'relativehumidity'
    }

    measurementDir = 'datasets/weather/measurements'

    for file in os.listdir(measurementDir):
        if file.endswith('.csv'):
            file_path = os.path.join(measurementDir, file)
            print(f"Processing file: {file_path}")

            # Read the CSV file
            weather_station = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')
            weather_station.rename(columns=column_mapping, inplace=True)

            weather_station['date'] = pd.to_datetime(weather_station['date'], format='%Y%m%d')

            weather_station.replace('-', np.nan, inplace=True)

            # Import into the Weather table
            weather_station.to_sql('weather', engine, if_exists='append', index=False)


# Imports all the information about the train stations
def importTransportStations():
    file_path = 'datasets/transport/haltestellen_2024/haltestellen_2024.csv'
    chunk_size = 10000000

    column_mapping = {
        'BPUIC': 'bpuic',
        'BP_BEZEICHNUNG': 'tstationname',
        'BP_ABKUERZUNG': 'tstation_abk',
        'BP_ID': 'bp_id',
        'SLOID': 'sloid',
        'KANTON': 'canton'
    }

    for chunk in pd.read_csv(file_path, delimiter=',', low_memory=False, chunksize=chunk_size):
        chunk.drop(columns=['FP_ID', 'TU_CODE', 'TU_BEZEICHNUNG', 'TU_ABKUERZUNG', 'FARTNUMMER',
                            'VM_ART', 'FAHRTAGE', 'AB_ZEIT_KB',
                            'AN_ZEIT_KB', 'RICHTUNG_TEXT_AGGREGIERT', 'END_BP_BEZEICHNUNG',
                            'LINIE'], inplace=True)
        chunk.rename(columns=column_mapping, inplace=True)
        chunk = chunk.drop_duplicates(subset=['bpuic'], keep='first')
        chunk.to_sql('transportstation', engine, if_exists='append', index=False)


# This fills up the table used to map a transport station to a weather station (via canton)
def mapToTransport():
    # Load WeatherStation data
    weather_stations = pd.read_sql('SELECT weatherstationname, canton FROM weatherstation', engine)

    # Load TransportStation data
    transport_stations = pd.read_sql('SELECT bpuic, canton FROM transportstation', engine)

    # Merge WeatherStation and TransportStation data on Canton
    map_to_transport = pd.merge(transport_stations, weather_stations, on='canton', how='inner')

    map_to_transport.to_sql('map_to_transport', engine, if_exists='append', index=False)


# Imports information about each transport station and each transport undertaking
def importToStationInfo():
    file_path = 'datasets/transport/haltestellen_2024/haltestellen_2024.csv'
    chunk_size = 10000000

    column_mapping1 = {
        'TU_CODE': 'tu_code',
        'TU_BEZEICHNUNG': 'tu_bezeichnung',
        'TU_ABKUERZUNG': 'tu_abkuerzung'
    }

    column_mapping2 = {
        'FP_ID': 'fpid',
        'TU_CODE': 'tu_code',
        'FARTNUMMER': 'fartnummer',
        'BPUIC': 'bpuic',
        'VM_ART': 'vm_art',
        'FAHRTAGE': 'fahrtage',
        'AB_ZEIT_KB': 'ab_zeit_kb',
        'AN_ZEIT_KB': 'an_zeit_kb',
        'RICHTUNG_TEXT_AGGREGIERT': 'richtung_text_aggregiert',
        'END_BP_BEZEICHNUNG': 'end_bp_bezeichnung',
        'LINIE': 'linie',
    }

    for chunk in pd.read_csv(file_path, delimiter=',', low_memory=False, chunksize=chunk_size):
        transportStationInfo = chunk[['FP_ID', 'TU_CODE', 'FARTNUMMER', 'BPUIC', 'VM_ART',
                                      'FAHRTAGE', 'AB_ZEIT_KB', 'AN_ZEIT_KB', 'RICHTUNG_TEXT_AGGREGIERT',
                                      'END_BP_BEZEICHNUNG', 'LINIE']]

        tU = chunk[['TU_CODE', 'TU_BEZEICHNUNG', 'TU_ABKUERZUNG']].drop_duplicates(
            subset=['TU_CODE'], keep='first')

        tU.rename(columns=column_mapping1, inplace=True)
        transportStationInfo.rename(columns=column_mapping2, inplace=True)

        tU.to_sql('transportundertaking', engine, if_exists='append', index=False)
        transportStationInfo.to_sql('transportstationinfo', engine, if_exists='append', index=False)


# Imports the actual transport data into the tables
def importTransportEvent():
    directories = [
        'datasets/transport/ist-daten-2024-01',
        'datasets/transport/ist-daten-2024-04',
        'datasets/transport/ist-daten-2024-07',
        'datasets/transport/ist-daten-2024-11'
    ]

    column_mapping = {
        'BETRIEBSTAG': 'date',
        'BPUIC': 'bpuic',
        'PRODUKT_ID': 'produktid',
        'ANKUNFTSZEIT': 'arrivaltime',
        'ABFAHRTSZEIT': 'departuretime',
        'FAELLT_AUS_TF': 'faelltaus'
    }

    for directory in directories:
        for file in os.listdir(directory):
            if file.endswith('.csv'):
                file_path = os.path.join(directory, file)
                print(f"Processing file: {file_path}")

                transportEvent = pd.read_csv(file_path, delimiter=';', low_memory=False)
                transportEvent.drop(columns=['FAHRT_BEZEICHNER', 'BETREIBER_ID', 'BETREIBER_ABK', 'BETREIBER_NAME',
                                             'LINIEN_ID', 'LINIEN_TEXT', 'UMLAUF_ID', 'VERKEHRSMITTEL_TEXT',
                                             'ZUSATZFAHRT_TF', 'HALTESTELLEN_NAME', 'AN_PROGNOSE', 'AN_PROGNOSE_STATUS',
                                             'AB_PROGNOSE', 'AB_PROGNOSE_STATUS', 'DURCHFAHRT_TF'], inplace=True)
                transportEvent.rename(columns=column_mapping, inplace=True)
                transportEvent['date'] = pd.to_datetime(transportEvent['date'], format='%d.%m.%Y')
                transportEvent['arrivaltime'] = pd.to_datetime(transportEvent['arrivaltime'], format='%d.%m.%Y %H:%M')
                transportEvent['departuretime'] = pd.to_datetime(transportEvent['departuretime'],
                                                                 format='%d.%m.%Y %H:%M')
                # Load valid BPUIC values from the TransportStation table
                valid_bpuic = pd.read_sql('SELECT bpuic FROM transportstation', engine)['bpuic'].tolist()

                # Filter the chunk to only include rows with valid BPUIC values
                transportEvent = transportEvent[transportEvent['bpuic'].isin(valid_bpuic)]
                transportEvent.to_sql('transportevent', engine, if_exists='append', index=False)


# Imports data about every transport operator and every journey
def importTransportOperatorAndJourney():
    directories = [
        'datasets/transport/ist-daten-2024-01',
        'datasets/transport/ist-daten-2024-04',
        'datasets/transport/ist-daten-2024-07',
        'datasets/transport/ist-daten-2024-11'
    ]

    column_mapping1 = {
        'BETREIBER_ID': 'betreiberid',
        'BETREIBER_ABK': 'betreiberabk',
        'BETREIBER_NAME': 'betreibername'
    }

    column_mapping2 = {
        'FAHRT_BEZEICHNER': 'fahrt_bezeichner',
        'LINIEN_ID': 'linienid',
        'LINIEN_TEXT': 'linientext',
        'UMLAUF_ID': 'umlaufid',
        'VERKEHRSMITTEL_TEXT': 'verkehrsmitteltext'
    }

    chunk_size = 10000000

    for directory in directories:
        for file in os.listdir(directory):
            if file.endswith('.csv'):
                file_path = os.path.join(directory, file)
                print(f"Processing file: {file_path}")
                existing_betreiberid = pd.read_sql('SELECT distinct betreiberid FROM transportoperator', engine)[
                    'betreiberid']
                existing_fahrt_bezeichner = \
                    pd.read_sql('SELECT distinct fahrt_bezeichner FROM transportjourney', engine)[
                        'fahrt_bezeichner']
                for chunk in pd.read_csv(file_path, delimiter=';', low_memory=False, chunksize=chunk_size):
                    # Create DataFrame for TransportOperator by selecting the required columns
                    transportOperator = chunk[['BETREIBER_ID', 'BETREIBER_ABK', 'BETREIBER_NAME']].drop_duplicates(
                        subset=['BETREIBER_ID'], keep='first')

                    # Create DataFrame for TransportJourney by selecting the required columns
                    transportJourney = chunk[['FAHRT_BEZEICHNER', 'LINIEN_ID', 'LINIEN_TEXT', 'UMLAUF_ID',
                                              'VERKEHRSMITTEL_TEXT']].drop_duplicates(subset='FAHRT_BEZEICHNER',
                                                                                      keep='first')

                    # Rename columns
                    transportOperator.rename(columns=column_mapping1, inplace=True)
                    transportJourney.rename(columns=column_mapping2, inplace=True)

                    # Check for new betreiberid values
                    transportOperator = transportOperator[~transportOperator['betreiberid'].isin(existing_betreiberid)]
                    # Add new betreiberid to the existing set
                    existing_betreiberid.update(transportOperator['betreiberid'].tolist())

                    # Check for new fahrt_bezeichner values
                    transportJourney = transportJourney[
                        ~transportJourney['fahrt_bezeichner'].isin(existing_fahrt_bezeichner)]
                    # Add new fahrt_bezeichner to the existing set
                    existing_fahrt_bezeichner.update(transportJourney['fahrt_bezeichner'].tolist())

                    # Insert into the database
                    if not transportOperator.empty:
                        transportOperator.to_sql('transportoperator', engine, if_exists='append', index=False)
                    if not transportJourney.empty:
                        transportJourney.to_sql('transportjourney', engine, if_exists='append', index=False)


# Imports detailed information about each transport event
def importTransportEventInfo():
    directories = [
        'datasets/transport/ist-daten-2024-01',
        'datasets/transport/ist-daten-2024-04',
        'datasets/transport/ist-daten-2024-07',
        'datasets/transport/ist-daten-2024-11'
    ]

    column_mapping = {
        'FAHRT_BEZEICHNER': 'fahrt_bezeichner',
        'BETREIBER_ID': 'betreiberid',
        'ZUSATZFAHRT_TF': 'zusatzfahrt_tf',
        'AN_PROGNOSE': 'arrivaltimepred',
        'AN_PROGNOSE_STATUS': 'arrivalpredstatus',
        'AB_PROGNOSE': 'departuretimepred',
        'AB_PROGNOSE_STATUS': 'departurepredstatus',
        'DURCHFAHRT_TF': 'durchfahrt_tf',
        'BPUIC': 'bpuic'
    }

    chunk_size = 10000000
    tid = 1
    valid_bpuic = pd.read_sql('SELECT bpuic FROM transportstation', engine)['bpuic'].tolist()

    for directory in directories:
        for file in os.listdir(directory):
            if file.endswith('.csv'):
                file_path = os.path.join(directory, file)
                print(f"Processing file: {file_path}")
                for chunk in pd.read_csv(file_path, delimiter=';', low_memory=False, chunksize=chunk_size):
                    # Create DataFrame for TransportOperator by selecting the required columns
                    transportEventInfo = chunk[['FAHRT_BEZEICHNER', 'BETREIBER_ID', 'ZUSATZFAHRT_TF',
                                                'AN_PROGNOSE', 'AN_PROGNOSE_STATUS', 'AB_PROGNOSE',
                                                'AB_PROGNOSE_STATUS', 'DURCHFAHRT_TF', 'BPUIC']]
                    # Rename columns
                    transportEventInfo.rename(columns=column_mapping, inplace=True)
                    # Convert datetime columns
                    transportEventInfo['arrivaltimepred'] = pd.to_datetime(transportEventInfo['arrivaltimepred'],
                                                                           format='%d.%m.%Y %H:%M:%S')
                    transportEventInfo['departuretimepred'] = pd.to_datetime(transportEventInfo['departuretimepred'],
                                                                             format='%d.%m.%Y %H:%M:%S')
                    # Filter the chunk to only include rows with valid BPUIC values
                    transportEventInfo = transportEventInfo[transportEventInfo['bpuic'].isin(valid_bpuic)]

                    transportEventInfo.drop(columns=['bpuic'], inplace=True)

                    length = len(transportEventInfo)
                    print(length)
                    df = pd.DataFrame({'tid': range(tid, tid + length)})
                    tid += length

                    transportEventInfo['tid'] = df['tid'].values

                    # Insert into TransportEventInfo
                    transportEventInfo.to_sql('transporteventinfo', engine, if_exists='append', index=False)


# Runs the full integration of all the data in one function
# THIS NEEDS TO RUN FOR SEVERAL HOURS (approx. 6h) TO FINISH
def runFullIntegration():
    importWeatherStation()
    importWeatherMeasurements()
    importTransportStations()
    mapToTransport()
    importToStationInfo()
    importTransportEvent()
    importTransportOperatorAndJourney()
    importTransportEventInfo()  # this takes by far the longest time (approx. 4h)


# Run this to integrate all the data into all the tables
if __name__ == '__main__':
    runFullIntegration()
