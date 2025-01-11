import psycopg2
from psycopg2 import Error

def book_in_db(book_title: str) -> int:
    """Проверяет наличие книги в базе данных и добавляет её, если она отсутствует."""
    try:
        with psycopg2.connect(
            user="postgres",
            password="пароль",
            host="localhost",
            port="5432",
            database="books"
        ) as connection:
            with connection.cursor() as cursor:
                # Проверка, есть ли книга в базе данных
                cursor.execute("SELECT id FROM books WHERE title = %s", (book_title,))
                result = cursor.fetchone()
                
                if result:
                    print("Книга уже существует в базе данных")
                    return result[0]  # Возвращаем ID книги, если она существует
                
                # Вставка новой книги
                insert_query = """INSERT INTO books (title) VALUES (%s) RETURNING id"""
                cursor.execute(insert_query, (book_title,))
                book_id = cursor.fetchone()[0]
                connection.commit()
                print("Книга добавлена в базу данных")
                return book_id

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL:", error)
        return -1  # Возвращаем -1 в случае ошибки

def add_adaptations(book_id: int, adaptations: list) -> None:
    """Добавляет экранизации в базу данных, если они ещё не добавлены."""
    try:
        with psycopg2.connect(
            user="postgres",
            password="пароль",
            host="localhost",
            port="5432",
            database="books"
        ) as connection:
            with connection.cursor() as cursor:
                for adaptation in adaptations:
                    # Проверяем, есть ли экранизация уже в базе данных
                    cursor.execute("SELECT * FROM adaptations WHERE book_id = %s AND adapts = %s", (book_id, adaptation['title']))
                    if cursor.fetchone() is None:  # Если не нашли, добавляем
                        insert_query = """INSERT INTO adaptations (book_id, adapts, kinopoisk) VALUES (%s, %s, %s)"""
                        cursor.execute(insert_query, (book_id, adaptation['title'], adaptation['url']))
                connection.commit()
                print("Экранизации добавлены в базу данных")
    except (Exception, Error) as error:
        print("Ошибка при добавлении экранизаций в PostgreSQL:", error)

def get_adaptations(book_id: int) -> list:
    """Получает экранизации из базы данных по ID книги."""
    try:
        with psycopg2.connect(
            user="postgres",
            password="пароль",
            host="localhost",
            port="5432",
            database="books"
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT adapts, kinopoisk FROM adaptations WHERE book_id = %s", (book_id,))
                adaptations = cursor.fetchall()
                return [{'title': row[0], 'url': row[1]} for row in adaptations]
    except (Exception, Error) as error:
        print("Ошибка при получении экранизаций из PostgreSQL:", error)
        return []
