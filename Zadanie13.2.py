import csv
from sqlite3 import Error

import sqlalchemy.sql.compiler
from sqlalchemy import Table, Column, Integer, Float, String, Date, MetaData, ForeignKey
from sqlalchemy import create_engine, insert
from sqlalchemy.exc import OperationalError

engine = create_engine('sqlite:///weather_database.db')
meta = MetaData()

clean_stations = Table(
    'stations', meta,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
    )

clean_measure = Table(
   'measures', meta,
    Column('station', String, ForeignKey('stations.station')),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer)
)

conn = engine.connect()
meta.create_all(engine)

print("Inserting data... Please wait.")

with open('clean_stations.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        try:
            insert_row = insert(clean_stations).values(
                station=row['station'],
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                elevation=float(row['elevation']),
                name=str(row['name']),
                country=str(row['country']),
                state=str(row['state'])
            )
            conn.execute(insert_row)
        except (OperationalError, ValueError) as error:
            print(f"Błąd bazy danych w wierszu {row}: \n{error}")


with open('clean_measure.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        try:
            insert_row = insert(clean_measure).values(
                station=row['station'],
                date=str(row['date']),
                precip=float(row['precip']),
                tobs=float(row['tobs'])
            )
            conn.execute(insert_row)
        except OperationalError as error:
            print(f"Błąd bazy danych w wierszu {row}: \n{error}")

print("\nData insert completed.")

five_rows = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
for row in five_rows:
    print(row)
