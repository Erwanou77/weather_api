import pandas as pd
from sqlalchemy import create_engine
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
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print("Data inserted successfully")