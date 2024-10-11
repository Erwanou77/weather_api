from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os, create_table, datetime, json, threading, batch, requests
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()
app = Flask(__name__)

# Configure the database connection
db_url = f"mysql+pymysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}@" \
         f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('DBNAME')}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
client = MongoClient(f'mongodb://{os.getenv("MONGO_USERNAME")}:{os.getenv("MONGO_PASSWORD")}@{os.getenv("MONGO_HOST")}:{os.getenv("MONGO_PORT")}/')
db = client[os.getenv('DBNAME')]
table = 'city'



@app.route('/',methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    query = f"%{data.get('query', '').strip()}%"
    results = []
    with Session() as session:
        
        # Cherche dans notre base si les données existe
        query_sql = text(f"""
            SELECT label, department_name, zip_code, region_name 
            FROM {table} 
            WHERE label LIKE :search_term 
            OR department_name LIKE :search_term 
            OR zip_code LIKE :search_term 
            OR region_name LIKE :search_term 
            LIMIT 20
        """)

        result = session.execute(query_sql, {'search_term': query})
        rows = result.fetchall()

        if rows:
            for row in rows:
                results.append(json.dumps(list(row)))
            return results

        # Recherche dans l'api de l'état pour vérifier si l'adresse existe
        api_response = requests.get(f"https://api-adresse.data.gouv.fr/search/?q={data['query']}&limit=20")
        if api_response.status_code != 200:
            return jsonify({"message": "External API error"}), api_response.status_code
        
        api_data = api_response.json().get('features', [])
        if not api_data:
            return jsonify({"message": "No results found"}), 404

        # Boucle pour stocker les données de l'api de l'état dans notre base
        results = []
        for feature in api_data:
            city_data = {
                'insee_code': feature['properties']['citycode'],
                'label': feature['properties']['city'],
                'department_name': feature['properties']['context'].split(", ")[-2],
                'zip_code': feature['properties']['postcode'],
                'region_name': feature['properties']['context'].split(", ")[-1],
                'latitude': feature['geometry']['coordinates'][1],
                'longitude': feature['geometry']['coordinates'][0]
            }

            fields = ', '.join(city_data.keys())
            placeholders = ', '.join([f":{key}" for key in city_data.keys()])

            session.execute(text(f"""
                INSERT INTO {table} ({fields}) 
                VALUES ({placeholders})
            """), city_data)
            results.append(city_data)
        session.commit()

    return jsonify(results)

@app.route('/<string:identifiant>',methods=['GET'])
def meteo(identifiant):
    session = Session()
    identifiant = [i.strip() for i in identifiant.split(',')]
    query_sql = text(f"""
            SELECT latitude,longitude FROM {table} 
            WHERE label IN :search_term 
            OR department_name IN :search_term 
            OR CAST( zip_code AS CHAR(5) ) IN :search_term
            OR region_name IN :search_term LIMIT 1
        """)
    req = session.execute(query_sql, {'search_term': identifiant})

    row = req.fetchone()
    if not row:
        return render_template('meteo404.html')

    return render_template('meteo.html', result=identifiant, lat=row[0], lon=row[1])


@app.route('/api/<longitude>/<latitude>',methods=['GET'])
def api_meteo(longitude,latitude):
    url = os.getenv('API_URL')
    response = requests.get(f'{url}&latitude={latitude}&longitude={longitude}')
    if response.status_code == 200:
        data = response.json()['current']
        collection_name = datetime.datetime.now().strftime('%Y-%m-%d')
        collection = db[collection_name]
        collection.insert_one({
                        'temp': data['temperature_2m'],
                        'wind_speed': data['wind_speed_10m'],
                        'wind_direction': data['wind_direction_10m'],
                        'humidity': data['relative_humidity_2m'],
                        'lat': latitude,
                        'lon': longitude,
                        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
    
        collection.create_index('time')
        collection.create_index([('lat', 1), ('lon', 1)])
        return jsonify({
                        'temp': data['temperature_2m'],
                        'wind_speed': data['wind_speed_10m'],
                        'wind_direction': data['wind_direction_10m'],
                        'humidity': data['relative_humidity_2m'],
                    })
    else:
        try:
            today = datetime.datetime.now()
        
            for days_ago in range(0, 365):  # Check up to a year in the past
                date_to_check = today - datetime.timedelta(days=days_ago)
                collection_name = date_to_check.strftime('%Y-%m-%d')
                collection = db[collection_name]
            
            # Search for weather data in this collection
                weather_data = collection.find_one(
                    {'lat': latitude, 'lon': longitude},
                    sort=[('time', -1)]  # Get the latest entry by 'time'
                )
            
                if weather_data:
                    # Remove MongoDB-specific fields before returning
                    del weather_data['_id']
                    del weather_data['time']
                    del weather_data['lat']
                    del weather_data['lon']
                    return jsonify(weather_data)
        except Exception as e:
            pass
        return jsonify({})
    


if __name__ == '__main__':
    create_table.main()
    t = threading.Thread(target=batch.main)
    t.start()
    app.run(host="0.0.0.0", port=5000, debug=True)
