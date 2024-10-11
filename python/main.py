from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os, create_table, json, threading, batch, requests
# Load environment variables from .env file
load_dotenv()
app = Flask(__name__)

# Configure the database connection
db_url = f"mysql+pymysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}@" \
         f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('DBNAME')}"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
table = 'city'

@app.route('/',methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    query = f"%{data.get('query', '').strip()}%"
    
    with Session() as session:
        
        # Cherche dans notre base si les données existe
        result = session.execute(text(f"""
            SELECT label, department_name, zip_code, region_name 
            FROM {table} 
            WHERE label LIKE :search_term 
            OR department_name LIKE :search_term 
            OR zip_code LIKE :search_term 
            OR region_name LIKE :search_term 
            LIMIT 20
        """), {'search_term': query}).fetchall()

        if result:
            return jsonify([dict(row) for row in result])

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

@app.route('/<identifiant>',methods=['GET'])
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

    return render_template('meteo.html', result=identifiant, lat=row[0], lon=row[1])
@app.route('/api/<longitude>/<latitude>',methods=['GET'])
def api_meteo(longitude,latitude):
    response = requests.get(f'{url}?appid={key}&exclude=minutely,hourly,daily,alerts&units=metric&lang=fr&lat={row[1]}&lon={row[0]}')
    if response.status_code == 200:
        data = response.json()
        return jsonify({
            'temp': data['main']['temp'],
            'description': data['weather'][0]['description'],
            'humidity': data['main']['humidity'],
            'lat': data['coord']['lat'],
            'lon': data['coord']['lon'],
            'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    


if __name__ == '__main__':
    create_table.main()
    t = threading.Thread(target=batch.main)
    t.start()
    app.run(debug=True)
