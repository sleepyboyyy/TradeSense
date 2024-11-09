import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
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
        cur.execute("SELECT MAX(TO_DATE(date, 'DD.MM.YYYY')) FROM stock_items")
        result = cur.fetchone()
        return result[0] if result[0] else None


def insert_data_to_db(conn, stock_data):
    stock_data_sorted = sorted(stock_data, key=lambda x: (
    x['Издавач'], datetime.strptime(x['Датум'], "%d.%m.%Y") if x['Датум'] else datetime.min))

    insert_query = """
        INSERT INTO stock_items (
            stock_code, date, last_price, max_price, min_price, avg_price,
            percent_change, quantity, turnover_best, total_turnover
        ) VALUES %s
    """

    formatted_data = [
        (
            record['Издавач'],
            record['Датум'],
            record['Цена на последна трансакција'],
            record['Макс.'],
            record['Мин.'],
            record['Просечна цена'],
            record['% пром.'],
            record['Количина'],
            record['Промет во БЕСТ во денари'],
            record['Вкупен промет во денари']
        )
        for record in stock_data_sorted
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_query, formatted_data)
    conn.commit()


def fetch_issuer_data(issuer, start_date, end_date):
    if start_date!=end_date:
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
                    record = {
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
                    }

                    issuer_data.append(record)

            except Exception as e:
                print(f"Error fetching data for {issuer}: {e}")

        return issuer_data


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    get_issuers()

    last_scraped_date = get_last_scraped_date(conn)
    start_date = (last_scraped_date + timedelta(days=1)) if last_scraped_date else datetime.now().date() - timedelta(
        days=365 * 10)
    end_date = datetime.now().date()

    date_ranges = [(start_date + timedelta(days=365 * i),
                    min(start_date + timedelta(days=365 * (i + 1)) - timedelta(days=1), end_date)) for i in
                   range((end_date.year - start_date.year) + 1)]

    start_time = time.time()

    if start_date == end_date:
        print(f"No new data to scrape")
    else:
        with ThreadPoolExecutor(max_workers=50) as executor:
            for issuer in issuers_data:
                results = executor.map(lambda dr: fetch_issuer_data(issuer, *dr), date_ranges)
                for result in results:
                    if result:
                        insert_data_to_db(conn, result)

    elapsed_time = time.time() - start_time
    print(f"Data scraping and insertion took {elapsed_time:.2f} seconds.")

    conn.close()


if __name__ == "__main__":
    main()