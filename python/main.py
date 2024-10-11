from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os, create_table, json, threading, batch
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
    # Get the search term from the form
    data = request.get_json()  # Get JSON data from the request
    query = data.get('query', '')
    
    # Create a new session
    session = Session()
    results = []

    try:
        # Prepare the SQL query
        query_sql = text(f"""
            SELECT label,department_name,zip_code,region_name FROM {table} 
            WHERE label LIKE :search_term 
            OR department_name LIKE :search_term 
            OR zip_code LIKE :search_term
            OR region_name LIKE :search_term LIMIT 20
        """)
        
        
        # Execute the query with the search term
        req = session.execute(query_sql, {'search_term': f'%{query}%'})

        rows = req.fetchall()

        for row in rows:
            results.append(json.dumps(list(row)))
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()  # Always close the session

    return jsonify(results)

@app.route('/<identifiant>',methods=['GET'])
def meteo(identifiant):
    identifiant = "('"+"', '".join([i.strip() for i in identifiant.split(',')])+"')"
    query_sql = text(f"""
            SELECT latitude,longitude FROM {table} 
            WHERE label IN :search_term 
            OR department_name IN :search_term 
            OR CAST( zip_code AS CHAR(5) ) IN :search_term
            OR region_name IN :search_term
        """)
    req = session.execute(query_sql, {'search_term': f'%{query}%'})

    rows = req.fetchone()

    return render_template('index.html')

if __name__ == '__main__':
    #create_table.main() # La decommenter pour lancer la creation de la table
    t = threading.Thread(target=batch.main)
    t.start()
    app.run(debug=True)
