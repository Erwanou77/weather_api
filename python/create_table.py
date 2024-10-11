import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
def main():
    # Step 1: Read CSV file
    csv_file_path = '../data/cities.csv'
    df = pd.read_csv(csv_file_path)

    # Step 2: Create a connection to MySQL Database
    db_url = f"mysql+pymysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}@" \
         f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('DBNAME')}"
    engine = create_engine(db_url)

    # Step 3: Create a table
    table_name = "city"
    inspector = inspect(engine)
    
    if inspector.has_table(table_name):
        print(f"Table '{table_name}' existe déjà.")

        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.fetchone()[0]
            if row_count > 0:
                print(f"Table '{table_name}' à déjà des données.")
                return
            else:
                print(f"Table '{table_name}' est vide.")
    else:
        print(f"Table '{table_name}' n'existe pas.")

    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print("Data inserted successfully")