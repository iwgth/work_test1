import psycopg2
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
import warnings
load_dotenv()

warnings.filterwarnings('ignore')

googleSheets = 'https://spreadsheets.google.com/feeds'
googleDrive = 'https://www.googleapis.com/auth/drive'
pathJson = '' # путь до джейсон ключа для сервісного акаунту 

# функція підключення до бази даних
def connect_to_database():
    try:
        connection = psycopg2.connect(
            host=os.getenv('HOST'),
            port=os.getenv('PORT'),
            database=os.getenv('DATABASE'),
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD')
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None

# запит до бази даних із курсором 
def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except psycopg2.Error as e:
        print("Error executing query:", e)
        return None

def update_google_sheet(orders):
    scope = [f'{googleSheets}',
             f'{googleDrive}']
    creds = ServiceAccountCredentials.from_json_keyfile_name(f'{pathJson}', scope)
    client = gspread.authorize(creds)
    
    # перевірка на існування таблиці
    try:
        spreadsheet = client.open('test4')
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create('test4')
        spreadsheet_url = spreadsheet.url
        print("Посилання на свторену таблицю:", spreadsheet_url)
        spreadsheet.share("", perm_type='anyone', role='writer', notify=False) # доступ для борду всім, хто з посиланням
    
    sheet = spreadsheet.sheet1
    sheet.clear()
    
    # заповнення таблиці даними
    header = ["id", "date", "merchant_id"]
    sheet.append_row(header)
    for order in orders:
        row = []
        for item in order:
            row.append(item)
        sheet.append_row(row)

# функція для того, щоб надсилати повідомлення в тг
def send_telegram_message(message):
    token = os.getenv('TG_TOKEN')
    chat_id = os.getenv('CHAT_ID'),
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": chat_id, "text": message}
    requests.post(url, params)

# саме підключення до бд
connection = connect_to_database()
if connection is None:
    exit()

# запит 
query = """
SELECT o.id, to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS'), merchant_id  
    FROM public.transactions tr
    INNER JOIN orders o ON o.id = tr.order_id
    WHERE tr.transaction_type = 'auth' 
    AND tr.created_date < NOW() - INTERVAL '7 days'
ORDER BY o.id ASC 
"""
orders = execute_query(connection, query)
for order in orders:
    print(order)

if orders:
    # створюємо словник, щоб порахувати к-ть замовлень для кожного мерчант_ід
    order_count = {}
    for order in orders:
        order_id = order[2]  
        if order_id in order_count:
            order_count[order_id] += 1
        else:
            order_count[order_id] = 1
    
    # формуємо повідомлення для тг
    message = "Кількість ордерів за кожним merchant_id:\n"
    for order_id, count in order_count.items():
        message += f"Merchant ID: {order_id}, Кількість ордерів: {count}\n"
    send_telegram_message(message)

    update_google_sheet(orders)

# закриваємо підключення до бд 
connection.close()
