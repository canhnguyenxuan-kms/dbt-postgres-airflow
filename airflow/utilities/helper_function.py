import psycopg2
import logging
import os
import requests
import time
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_data_from_api(city):
    load_dotenv()  # Load environment variables from .env file
    API_KEY = os.getenv('WEATHERSTACK_API_KEY')  # Get the API key
    URL = f'https://api.weatherstack.com/current?access_key={API_KEY}&query={city}'  # API endpoint with query parameters
    logger.info("Fetching data from WeatherStack API")
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raise an error for bad responses
        logger.info("API response received successfully !!!")
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"APT request failed: {e}", exc_info=True)
        raise

def connect_to_postgres():
    logger.info("Connecting to PostgreSQL database")
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5000,
            dbname='dw',
            user="cnguyen",
            password='123456'
        )
        logger.info("Connected to PostgreSQL database successfully !!!")
        return conn
    except psycopg2.Error as e: 
        logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
        raise

def create_schema_table(conn):
    logger.info("Creating schema and table if not exists in PostgreSQL")
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS weather;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather.weather_report (
                id SERIAL PRIMARY KEY,
                city text,
                temperature FLOAT,
                weather_description text,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_date TIMESTAMP default CURRENT_TIMESTAMP,
                utc_offset TEXT
            );
        """)
        cursor.execute("truncate table weather.weather_report;")
        conn.commit()
        logger.info("Schema and table created successfully !!!")
    except psycopg2.Error as e:
        logger.error(f"Failed to create schema or table: {e}", exc_info=True)
        conn.rollback()
        raise

def insert_data_into_table(conn, data):
    logger.info("Inserting data into PostgreSQL table")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO weather.weather_report (city, temperature, weather_description, wind_speed, time, inserted_date, utc_offset)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s);
        """, (
            data['location']['name'],
            data['current']['temperature'],
            data['current']['weather_descriptions'][0],
            data['current']['wind_speed'],
            data['location']['localtime'],
            data['location']['utc_offset']
        ))
        conn.commit()
        logger.info(f"Fetched data for {data['location']['name']} successfully !!!")
    except psycopg2.Error as e:
        logger.error(f"Failed to fetched data for {data['location']['name']}: {e}", exc_info=True)
        conn.rollback()
        raise

def main():
    try:
        conn = connect_to_postgres()  # Connect to PostgreSQL
        create_schema_table(conn)  # Create schema and table if not exists
        cities = ["London", "Singapore", "Shanghai", "New York", "Tokyo", "Berlin", "Hanoi", "Ho Chi Minh City", "Paris", "Moscow"]

        for city in cities:
            data = fetch_data_from_api(city)  # Fetch data from the API
            insert_data_into_table(conn, data)  # Insert data into the table
            time.sleep(10)  # Sleep 2 seconds between requests to avoid hitting API rate limits
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()  # Ensure the connection is closed
            
if __name__ == "__main__":
    main()  # Run the main function