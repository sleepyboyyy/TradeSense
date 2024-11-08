import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
import os
from dotenv import load_dotenv
# Load environment variables from the .env file
load_dotenv()

# Database configuration using environment variables
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

base_url = "https://www.mse.mk/mk/stats/symbolhistory/"
issuers_data = []

def num_there(s):
    return any(i.isdigit() for i in s)

def get_issuers():
    issuers_url = f"{base_url}kmb"
    with requests.Session() as session:
        response = session.get(issuers_url)
        soup = BeautifulSoup(response.content, "html.parser")
        issuers_elements = soup.select("#Code option")
        for option in issuers_elements:
            issuer = option.text.strip()
            if issuer and not num_there(issuer):
                issuers_data.append(issuer)

def get_last_scraped_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM stock_prices")
        result = cur.fetchone()
        return result[0] if result[0] else None


def insert_data_to_db(conn, stock_data):
    stock_data_filled = forward_fill_data(stock_data)
    stock_data_filled = sorted(stock_data_filled, key=lambda x: (
    x['Издавач'], datetime.strptime(x['Датум'], "%d.%m.%Y") if x['Датум'] else datetime.min))

    insert_query = """
        INSERT INTO stock_prices (
            stock_code, date, last_price, max_price, min_price, avg_price,
            percent_change, quantity, turnover_best, total_turnover
        ) VALUES %s
    """

    formatted_data = [
        (
            record['Издавач'],
            datetime.strptime(record['Датум'], "%d.%m.%Y") if record['Датум'] else None,
            float(record['Цена на последна трансакција'].replace('.', '').replace(',', '.')) if record[
                'Цена на последна трансакција'] else None,
            float(record['Макс.'].replace('.', '').replace(',', '.')) if record['Макс.'] else None,
            float(record['Мин.'].replace('.', '').replace(',', '.')) if record['Мин.'] else None,
            float(record['Просечна цена'].replace('.', '').replace(',', '.')) if record['Просечна цена'] else None,
            float(record['% пром.'].replace('.', '').replace(',', '.')) if record['% пром.'] else 0,
            float(record['Количина'].replace('.', '').replace(',', '.')) if record['Количина'] else None,
            float(record['Промет во БЕСТ во денари'].replace('.', '').replace(',', '.')) if record[
                'Промет во БЕСТ во денари'] else None,
            float(record['Вкупен промет во денари'].replace('.', '').replace(',', '.')) if record[
                'Вкупен промет во денари'] else None
        )
        for record in stock_data_filled
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_query, formatted_data)
    conn.commit()

def fetch_issuer_data(issuer, start_date, end_date):
    issuer_data = []
    url = f"{base_url}{issuer}"
    with requests.Session() as session:
        payload = {
            "FromDate": start_date.strftime("%d.%m.%Y"),
            "ToDate": end_date.strftime("%d.%m.%Y"),
            "Issuer": issuer
        }
        try:
            response = session.post(url, data=payload)
            soup = BeautifulSoup(response.text, "html.parser")
            table_body = soup.select_one("#resultsTable tbody")
            if not table_body:
                return []
            for row in table_body.find_all("tr"):
                row_data = row.find_all("td")
                if len(row_data) < 9:
                    continue
                issuer_data.append({
                    "Издавач": issuer,
                    "Датум": row_data[0].text.strip() or None,
                    "Цена на последна трансакција": row_data[1].text.strip() or None,
                    "Макс.": row_data[2].text.strip() or None,
                    "Мин.": row_data[3].text.strip() or None,
                    "Просечна цена": row_data[4].text.strip() or None,
                    "% пром.": row_data[5].text.strip() or None,
                    "Количина": row_data[6].text.strip() or None,
                    "Промет во БЕСТ во денари": row_data[7].text.strip() or None,
                    "Вкупен промет во денари": row_data[8].text.strip() or None,
                })
        except Exception as e:
            print(f"Error fetching data for {issuer}: {e}")
    return issuer_data


def forward_fill_data(stock_data_sorted):
    filled_data = []

    stock_data_sorted = sorted(stock_data_sorted, key=lambda x: (
    x['Издавач'], datetime.strptime(x['Датум'], "%d.%m.%Y") if x['Датум'] else datetime.min))

    grouped_by_issuer = {}
    for record in stock_data_sorted:
        issuer = record['Издавач']
        if issuer not in grouped_by_issuer:
            grouped_by_issuer[issuer] = []
        grouped_by_issuer[issuer].append(record)

    for issuer, records in grouped_by_issuer.items():
        records = sorted(records, key=lambda x: datetime.strptime(x['Датум'], "%d.%m.%Y"))

        for i in range(len(records) - 1):
            current_record = records[i]
            next_record = records[i + 1]

            current_date = datetime.strptime(current_record['Датум'], "%d.%m.%Y")
            next_date = datetime.strptime(next_record['Датум'], "%d.%m.%Y")
            while (next_date - current_date).days > 1:
                current_date += timedelta(days=1)
                filled_data.append({
                    "Издавач": current_record['Издавач'],
                    "Датум": current_date.strftime("%d.%m.%Y"),
                    "Цена на последна трансакција": current_record['Цена на последна трансакција'],
                    "Макс.": current_record['Макс.'],
                    "Мин.": current_record['Мин.'],
                    "Просечна цена": current_record['Просечна цена'],
                    "% пром.": '0',
                    "Количина": '0',
                    "Промет во БЕСТ во денари": '0',
                    "Вкупен промет во денари": '0'
                })

        filled_data.extend(records)

    return filled_data


def main():
    # Establish the database connection
    conn = psycopg2.connect(**DB_CONFIG)
    get_issuers()

    # Retrieve the last date in the database or default to 10 years ago
    last_scraped_date = get_last_scraped_date(conn)
    today = datetime.now().date()  # Convert today's date to a `date` object

    # Check if there is no new data to scrape
    if last_scraped_date == today:
        print("Data is already up-to-date. No new data to scrape.")
        conn.close()
        return

    # Define start_date and end_date for scraping
    start_date = (last_scraped_date + timedelta(days=1)) if last_scraped_date else (today - timedelta(days=365 * 10))
    end_date = today

    # Create date ranges for the desired period
    date_ranges = [
        (start_date + timedelta(days=365 * i),
         min(start_date + timedelta(days=365 * (i + 1)) - timedelta(days=1), end_date))
        for i in range((end_date.year - start_date.year) + 1)
    ]

    # Start the timer
    start_time = time.time()
    total_records_added = 0  # Initialize a counter for the number of records added

    # Use ThreadPoolExecutor to fetch data in parallel
    with ThreadPoolExecutor(max_workers=50) as executor:
        for issuer in issuers_data:
            results = executor.map(lambda dr: fetch_issuer_data(issuer, *dr), date_ranges)
            for result in results:
                if result:
                    insert_data_to_db(conn, result)
                    total_records_added += len(result)  # Increment by the number of records in the result

    # End timer and display time taken
    elapsed_time = time.time() - start_time
    print(f"Data scraping and insertion took {elapsed_time:.2f} seconds.")
    print(f"Total records added: {total_records_added}")

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()