import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

output_path = "генерация/"

# Инициализация Faker
faker = Faker("ru_RU")

# Количество записей
NUM_CLIENTS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 10000
NUM_WAREHOUSES = 100

# Генерация клиентов
def generate_clients():
    clients = []
    for client_id in range(1, NUM_CLIENTS + 1):
        # Генерация российского номера телефона
        # phone_number = "8" + "".join([str(random.randint(0, 9)) for _ in range(10)])
        clients.append({
            "client_id": client_id,
            "name": faker.name(),
            "email": faker.email(),
            # "phone": phone_number,
            "phone": faker.phone_number(),
            "region": random.choice(["Moscow", "Tula", "Saint-Peterburg", "Kazan"]),
        })
    return clients

# Генерация продуктов
def generate_products():
    categories = ["Electronics", "Clothing", "Books", "For car"]
    products = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        products.append({
            "product_id": product_id,
            "name": faker.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(10, 1000), 2),
        })
    return products

# Генерация заказов
def generate_orders(clients, products):
    orders = []
    for order_id in range(1, NUM_ORDERS + 1):
        client = random.choice(clients)
        product = random.choice(products)
        orders.append({
            "order_id": order_id,
            "client_id": client["client_id"],
            "product_id": product["product_id"],
            "order_date": faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"),
            "quantity": random.randint(1, 10),
        })
    return orders

# Генерация складов
def generate_warehouses():
    warehouses = []
    for warehouse_id in range(1, NUM_WAREHOUSES + 1):
        warehouses.append({
            "warehouse_id": warehouse_id,
            "region": random.choice(["Moscow", "Tula", "Saint-Peterburg", "Kazan"]),
            "capacity": random.randint(1000, 10000),
        })
    return warehouses

# Сохранение в CSV
# def save_to_csv(data, filename, fieldnames):
#     with open(filename, mode="w", newline="", encoding="utf-8") as file:
#         writer = csv.DictWriter(file, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(data)

# Генерация данных и сохранение
if __name__ == "__main__":
    clients = generate_clients()
    products = generate_products()
    orders = generate_orders(clients, products)
    warehouses = generate_warehouses()

#     save_to_csv(clients, "clients.csv", ["client_id", "name", "email", "phone", "region"])
#     save_to_csv(products, "products.csv", ["product_id", "name", "category", "price"])
#     save_to_csv(orders, "orders.csv", ["order_id", "client_id", "product_id", "order_date", "quantity"])
#     save_to_csv(warehouses, "warehouses.csv", ["warehouse_id", "region", "capacity"])

#     print("Данные успешно сгенерированы и сохранены в CSV!")

# Сохраняем клиентов
clients_df = pd.DataFrame(clients)
clients_df.to_csv(output_path + "clients.csv", index=False)

# Сохраняем склады
warehouses_df = pd.DataFrame(warehouses)
warehouses_df.to_csv(output_path + "warehouses.csv", index=False)

# Сохраняем продукты
products_df = pd.DataFrame(products)
products_df.to_csv(output_path + "products.csv", index=False)

# Сохраняем заказы
orders_df = pd.DataFrame(orders)
orders_df.to_csv(output_path + "orders.csv", index=False)

print("CSV-файлы успешно созданы!")