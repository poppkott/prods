import psycopg2
from datetime import datetime

# Database connection settings
DB_CONFIG = {
    "dbname": "nurse",
    "user": "postgres",
    "password": "пароль",
    "host": "localhost",
    "port": "5433"
}


def extract_data(query):
    """Execute a query to extract data from the stage layer."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

def execute_query(query, data=None):
    """Execute a query (with optional parameters)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()
        conn.close()

def load_dim_clients():
    """Load data into dim_clients with SCD2 logic."""
    update_existing = """
        UPDATE dim_clients
        SET end_date = CURRENT_DATE
        WHERE client_id IN (
            SELECT stg.client_id
            FROM stg_clients stg
            INNER JOIN dim_clients dwh
            ON stg.client_id = dwh.client_id
            WHERE (
                stg.name IS DISTINCT FROM dwh.name OR
                stg.phone IS DISTINCT FROM dwh.phone OR
                stg.region_id IS DISTINCT FROM dwh.region_id
            )
            AND dwh.end_date = '2999-12-31'
        );
    """
    insert_new = """
        INSERT INTO dim_clients (client_id, name, phone, region_id, start_date)
        SELECT stg.client_id, stg.name, stg.phone, stg.region_id, CURRENT_DATE
        FROM stg_clients stg
        LEFT JOIN dim_clients dwh
        ON stg.client_id = dwh.client_id
        WHERE dwh.client_id IS NULL
        OR (
            stg.name IS DISTINCT FROM dwh.name OR
            stg.phone IS DISTINCT FROM dwh.phone OR
            stg.region_id IS DISTINCT FROM dwh.region_id
        );
    """
    execute_query(update_existing)
    execute_query(insert_new)

def load_dim_nurses():
    """Load data into dim_nurses with SCD2 logic."""
    update_existing = """
        UPDATE dim_nurses
        SET end_date = CURRENT_DATE
        WHERE nurse_id IN (
            SELECT stg.nurse_id
            FROM stg_nurses stg
            INNER JOIN dim_nurses dwh
            ON stg.nurse_id = dwh.nurse_id
            WHERE (
                stg.name IS DISTINCT FROM dwh.name OR
                stg.specialization IS DISTINCT FROM dwh.specialization OR
                stg.region_id IS DISTINCT FROM dwh.region_id
            )
            AND dwh.end_date = '2999-12-31'
        );
    """
    insert_new = """
        INSERT INTO dim_nurses (nurse_id, name, specialization, region_id, start_date)
        SELECT stg.nurse_id, stg.name, stg.specialization, stg.region_id, CURRENT_DATE
        FROM stg_nurses stg
        LEFT JOIN dim_nurses dwh
        ON stg.nurse_id = dwh.nurse_id
        WHERE dwh.nurse_id IS NULL
        OR (
            stg.name IS DISTINCT FROM dwh.name OR
            stg.specialization IS DISTINCT FROM dwh.specialization OR
            stg.region_id IS DISTINCT FROM dwh.region_id
        );
    """
    execute_query(update_existing)
    execute_query(insert_new)

def load_dim_regions():
    """Load new regions into dim_regions."""
    insert_new = """
        INSERT INTO dim_regions (region_id, region_name)
        SELECT DISTINCT stg.region_id, NULL
        FROM (
            SELECT region_id FROM stg_clients
            UNION
            SELECT region_id FROM stg_nurses
        ) stg
        LEFT JOIN dim_regions dwh
        ON stg.region_id = dwh.region_id
        WHERE dwh.region_id IS NULL;
    """
    execute_query(insert_new)

def load_fact_requests():
    """Load data into fact_requests."""
    insert_new = """
        INSERT INTO fact_requests (request_id, client_id, nurse_id, service_date, status, service_cost)
        SELECT 
            stg.request_id,
            stg.client_id,
            stg.nurse_id,
            stg.service_date,
            stg.status,
            stg.service_cost
        FROM stg_requests stg
        LEFT JOIN fact_requests fact
        ON stg.request_id = fact.request_id
        WHERE fact.request_id IS NULL;
    """
    execute_query(insert_new)

def run_etl():
    """Execute the entire ETL process."""
    print("Starting ETL process...")
    try:
        print("Loading dim_clients...")
        load_dim_clients()
        print("Loading dim_nurses...")
        load_dim_nurses()
        print("Loading dim_regions...")
        load_dim_regions()
        print("Loading fact_requests...")
        load_fact_requests()
        print("ETL process completed successfully!")
    except Exception as e:
        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    run_etl()
