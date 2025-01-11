from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
from typing import List, Optional
from datetime import date

# Создаем приложение FastAPI
app = FastAPI()

# Подключение к базе данных PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="nurse",
            user="postgres",
            password="пароль"
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# ========================
# ЭНДПОИНТЫ ДЛЯ КЛИЕНТОВ
# ========================
# Модель для клиента (входящие данные)
class ClientIn(BaseModel):
    name: str
    phone: str
    region_id: int

# Модель для клиента (ответ от API)
class ClientOut(BaseModel):
    client_id: int
    name: str
    phone: str
    region_id: int

# Endpoint для получения списка клиентов
@app.get("/clients", response_model=List[ClientOut])
def get_clients():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("SELECT client_id, name, phone, region_id FROM stg_clients;")
        clients = cursor.fetchall()
        return [dict(client) for client in clients]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching clients: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Endpoint для добавления нового клиента
@app.post("/clients", response_model=ClientOut)
def add_client(client: ClientIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            "INSERT INTO stg_clients (name, phone, region_id) VALUES (%s, %s, %s) RETURNING client_id, name, phone, region_id;",
            (client.name, client.phone, client.region_id)
        )
        new_client = cursor.fetchone()
        conn.commit()
        return dict(new_client)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error adding client: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Endpoint для обновления клиента
@app.put("/clients/{client_id}", response_model=ClientOut)
def update_client(client_id: int, client: ClientIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            "UPDATE stg_clients SET name = %s, phone = %s, region_id = %s WHERE client_id = %s RETURNING client_id, name, phone, region_id;",
            (client.name, client.phone, client.region_id, client_id)
        )
        updated_client = cursor.fetchone()
        if not updated_client:
            raise HTTPException(status_code=404, detail="Client not found")
        conn.commit()
        return dict(updated_client)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating client: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Endpoint для удаления клиента
@app.delete("/clients/{client_id}", response_model=dict)
def delete_client(client_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM stg_clients WHERE client_id = %s RETURNING client_id;", (client_id,))
        deleted_client_id = cursor.fetchone()
        if not deleted_client_id:
            raise HTTPException(status_code=404, detail="Client not found")
        conn.commit()
        return {"message": "Client deleted successfully", "client_id": deleted_client_id[0]}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error deleting client: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# ================================
# МОДЕЛИ
# ================================

# Модели для заявок
class RequestIn(BaseModel):
    client_id: int
    nurse_id: int
    service_date: date
    status: str
    service_cost: float

class RequestOut(RequestIn):
    request_id: int

# Модели для медсестёр
class NurseIn(BaseModel):
    name: str
    specialization: str
    region_id: int

class NurseOut(NurseIn):
    nurse_id: int

# ================================
# ЭНДПОИНТЫ ДЛЯ ЗАЯВОК
# ================================

@app.get("/requests", response_model=List[RequestOut])
def get_requests():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("SELECT request_id, client_id, nurse_id, service_date, status, service_cost FROM stg_requests;")
        requests = cursor.fetchall()
        return [dict(request) for request in requests]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching requests: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.post("/requests", response_model=RequestOut)
def add_request(request: RequestIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            """
            INSERT INTO stg_requests (client_id, nurse_id, service_date, status, service_cost)
            VALUES (%s, %s, %s, %s, %s) RETURNING request_id, client_id, nurse_id, service_date, status, service_cost;
            """,
            (
                request.client_id,
                request.nurse_id if request.nurse_id is not None else None,
                request.service_date,
                request.status,
                request.service_cost,
            )
        )
        new_request = cursor.fetchone()
        conn.commit()
        return dict(new_request)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error adding request: {str(e)}")
    finally:
        cursor.close()
        conn.close()
# @app.post("/requests", response_model=RequestOut)
# def add_request(request: RequestIn):
#     conn = get_db_connection()
#     cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#     try:
#         cursor.execute(
#             """
#             INSERT INTO stg_requests (client_id, nurse_id, service_date, status, service_cost)
#             VALUES (%s, %s, %s, %s, %s) RETURNING request_id, client_id, nurse_id, service_date, status, service_cost;
#             """,
#             (request.client_id, request.nurse_id, request.service_date, request.status, request.service_cost)
#         )
#         new_request = cursor.fetchone()
#         conn.commit()
#         return dict(new_request)
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=400, detail=f"Error adding request: {str(e)}")
#     finally:
#         cursor.close()
#         conn.close()

@app.put("/requests/{request_id}", response_model=RequestOut)
def update_request(request_id: int, request: RequestIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            """
            UPDATE stg_requests
            SET client_id = %s, nurse_id = %s, service_date = %s, status = %s, service_cost = %s
            WHERE request_id = %s
            RETURNING request_id, client_id, nurse_id, service_date, status, service_cost;
            """,
            (request.client_id, request.nurse_id, request.service_date, request.status, request.service_cost, request_id)
        )
        updated_request = cursor.fetchone()
        if not updated_request:
            raise HTTPException(status_code=404, detail="Request not found")
        conn.commit()
        return dict(updated_request)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating request: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.delete("/requests/{request_id}", response_model=dict)
def delete_request(request_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM stg_requests WHERE request_id = %s RETURNING request_id;", (request_id,))
        deleted_request_id = cursor.fetchone()
        if not deleted_request_id:
            raise HTTPException(status_code=404, detail="Request not found")
        conn.commit()
        return {"message": "Request deleted successfully", "request_id": deleted_request_id[0]}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error deleting request: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# ================================
# ЭНДПОИНТЫ ДЛЯ МЕДСЁСТЁР
# ================================

@app.get("/nurses", response_model=List[NurseOut])
def get_nurses():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("SELECT nurse_id, name, specialization, region_id FROM stg_nurses;")
        nurses = cursor.fetchall()
        return [dict(nurse) for nurse in nurses]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching nurses: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.post("/nurses", response_model=NurseOut)
def add_nurse(nurse: NurseIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            """
            INSERT INTO stg_nurses (name, specialization, region_id)
            VALUES (%s, %s, %s) RETURNING nurse_id, name, specialization, region_id;
            """,
            (nurse.name, nurse.specialization, nurse.region_id)
        )
        new_nurse = cursor.fetchone()
        conn.commit()
        return dict(new_nurse)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error adding nurse: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.put("/nurses/{nurse_id}", response_model=NurseOut)
def update_nurse(nurse_id: int, nurse: NurseIn):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            """
            UPDATE stg_nurses
            SET name = %s, specialization = %s, region_id = %s
            WHERE nurse_id = %s
            RETURNING nurse_id, name, specialization, region_id;
            """,
            (nurse.name, nurse.specialization, nurse.region_id, nurse_id)
        )
        updated_nurse = cursor.fetchone()
        if not updated_nurse:
            raise HTTPException(status_code=404, detail="Nurse not found")
        conn.commit()
        return dict(updated_nurse)
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating nurse: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.delete("/nurses/{nurse_id}", response_model=dict)
def delete_nurse(nurse_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM stg_nurses WHERE nurse_id = %s RETURNING nurse_id;", (nurse_id,))
        deleted_nurse_id = cursor.fetchone()
        if not deleted_nurse_id:
            raise HTTPException(status_code=404, detail="Nurse not found")
        conn.commit()
        return {"message": "Nurse deleted successfully", "nurse_id": deleted_nurse_id[0]}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error deleting nurse: {str(e)}")
    finally:
        cursor.close()
        conn.close()