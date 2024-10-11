
import requests, os, datetime, time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient

load_dotenv()
url = os.getenv('API_URL')
key = os.getenv('API_KEY')
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
    collection.insert_one({
        'lat': weather_data['lat'],
        'lon': weather_data['lon'],
        'temp': weather_data['temp'],
        'description': weather_data['description'],
        'humidity': weather_data['humidity'],
        'time': weather_data['time']
    })
    
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
                response = requests.get(f'{url}?appid={key}&exclude=minutely,hourly,daily,alerts&units=metric&lang=fr&lat={row[1]}&lon={row[0]}')
                if response.status_code == 200:
                    data = response.json()

                    # Sauvegarder les données dans MongoDB
                    save_weather_data({
                        'temp': data['main']['temp'],
                        'description': data['weather'][0]['description'],
                        'humidity': data['main']['humidity'],
                        'lat': data['coord']['lat'],
                        'lon': data['coord']['lon'],
                        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                time.sleep(0.1)
            
            if len(rows) < size:
                run=False
            page+=1
        session.close()
        time.sleep(10)  # Rafraîchissement des données toutes les 60 secondes