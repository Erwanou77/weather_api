
import requests, os, datetime, time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient



load_dotenv()
url = os.getenv('API_URL')
db_url = f"mysql+pymysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}@" \
     f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('DBNAME')}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
client = MongoClient(f'mongodb://{os.getenv("MONGO_USERNAME")}:{os.getenv("MONGO_PASSWORD")}@{os.getenv("MONGO_HOST")}:{os.getenv("MONGO_PORT")}/')
db = client[os.getenv('DBNAME')]
table = 'city'

def save_weather_data(weather_data):
    collection_name = datetime.datetime.now().strftime('%Y-%m-%d')
    collection = db[collection_name]
    
    # Sauvegarder les données météo dans la collection avec l'heure
    collection.insert_one(weather_data)
    
    collection.create_index('time')
    collection.create_index([('lat', 1), ('lon', 1)])



def main():
    while True:
        session = Session()
        run=True
        size=20
        page=0
        while run:
            query_sql = text(f"""SELECT longitude,latitude FROM {table} LIMIT {size} OFFSET {page*size}""")
            req = session.execute(query_sql)
            rows = req.fetchall()

            for row in rows:
                response = requests.get(f'{url}&latitude={row[1]}&longitude={row[0]}')
                if response.status_code == 200:
                    data = response.json()['current']
                    # Sauvegarder les données dans MongoDB
                    save_weather_data({
                        'temp': data['temperature_2m'],
                        'wind_speed': data['wind_speed_10m'],
                        'wind_direction': data['wind_direction_10m'],
                        'humidity': data['relative_humidity_2m'],
                        'lat': row[1],
                        'lon': row[0],
                        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                time.sleep(1)

            
            if len(rows) < size:
                run=False
            page+=1
        session.close()
        time.sleep(15*60)  # Rafraîchissement des données toutes les 60 secondes