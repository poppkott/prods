import psycopg2
import pandas as pd

# Подключение к базе данных
conn = psycopg2.connect(
    dbname="MP",
    user="postgres",
    password="пароль",
    host="localhost",
    port="5433"
)

cursor = conn.cursor()

# Загрузка данных из CSV в таблицы
def load_csv_to_stage(table_name, file_path):
    with open(file_path, 'r', encoding="utf-8") as file:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)
    conn.commit()
    print(f"Данные загружены в {table_name}")

# Загрузка клиентов
load_csv_to_stage("stage_clients", "генерация/clients.csv")

# Загрузка складов
load_csv_to_stage("stage_warehouses", "генерация/warehouses.csv")

# Загрузка продуктов
load_csv_to_stage("stage_products", "генерация/products.csv")

# Загрузка заказов
load_csv_to_stage("stage_orders", "генерация/orders.csv")

cursor.close()
conn.close()
print("Данные успешно загружены в Stage-таблицы!")
