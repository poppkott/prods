import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Подключение к базе данных
def get_db_connection():
    return psycopg2.connect(
        dbname="nurse",
        user="postgres",
        password="пароль",
        host="localhost",
        port="5433"
    )

# Загрузка данных в dm_client_requests
def load_dm_client_requests():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Агрегация данных из fact_requests
        query = """
            INSERT INTO public.dm_client_requests (
                client_id, report_date, total_requests, avg_service_cost,
                max_service_cost, min_service_cost, total_revenue
            )
            SELECT
                fr.client_id,
                CURRENT_DATE AS report_date,
                COUNT(*) AS total_requests,
                AVG(fr.service_cost) AS avg_service_cost,
                MAX(fr.service_cost) AS max_service_cost,
                MIN(fr.service_cost) AS min_service_cost,
                SUM(fr.service_cost) AS total_revenue
            FROM public.fact_requests fr
            JOIN public.dim_clients dc ON fr.client_id = dc.client_id
            WHERE fr.service_date <= CURRENT_DATE
            GROUP BY fr.client_id
            ON CONFLICT (client_id, report_date) DO NOTHING;
        """
        cursor.execute(query)
        conn.commit()
        print("Данные успешно загружены в dm_client_requests.")
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в dm_client_requests: {e}")
    finally:
        cursor.close()
        conn.close()

# Загрузка данных в dm_nurse_performance
def load_dm_nurse_performance():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Агрегация данных из fact_requests
        query = """
            INSERT INTO public.dm_nurse_performance (
                nurse_id, report_date, total_requests, avg_service_cost,
                max_service_cost, min_service_cost, total_revenue
            )
            SELECT
                fr.nurse_id,
                CURRENT_DATE AS report_date,
                COUNT(*) AS total_requests,
                AVG(fr.service_cost) AS avg_service_cost,
                MAX(fr.service_cost) AS max_service_cost,
                MIN(fr.service_cost) AS min_service_cost,
                SUM(fr.service_cost) AS total_revenue
            FROM public.fact_requests fr
            JOIN public.dim_nurses dn ON fr.nurse_id = dn.nurse_id
            WHERE fr.service_date <= CURRENT_DATE
            GROUP BY fr.nurse_id
            ON CONFLICT (nurse_id, report_date) DO NOTHING;
        """
        cursor.execute(query)
        conn.commit()
        print("Данные успешно загружены в dm_nurse_performance.")
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в dm_nurse_performance: {e}")
    finally:
        cursor.close()
        conn.close()

# Основной процесс ETL
def run_etl_for_marts():
    print("Запуск ETL для слоя витрин...")
    load_dm_client_requests()
    load_dm_nurse_performance()
    print("ETL для слоя витрин завершен.")

# Запуск скрипта
if __name__ == "__main__":
    run_etl_for_marts()
