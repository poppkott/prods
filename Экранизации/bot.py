from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from db import book_in_db, add_adaptations, get_adaptations  # Импортируем функции из db.py
import urllib.parse
import requests
from bs4 import BeautifulSoup

# Генерация ссылки для поиска экранизаций на Кинопоиске
def generate_kinopoisk_url(book_title):
    query = urllib.parse.quote(book_title)  # Кодируем название книги для URL
    url = f"https://www.kinopoisk.ru/s/type/film/list/1/find/{query}/"
    return url

def get_adaptations_from_kinopoisk(book_title):
    url = generate_kinopoisk_url(book_title)
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        adaptations = []
        
        # Ищем все элементы с классом 'element' (это обертка для каждой экранизации)
        for element in soup.find_all('div', class_='element'):
            # Ищем внутри элемента 'p' с классом 'name' для названия экранизации
            title_tag = element.find('p', class_='name')
            if title_tag:
                title = title_tag.get_text(strip=True)  # Получаем текст без лишних пробелов
                link = "https://www.kinopoisk.ru" + title_tag.find('a')['href']  # Формируем полный URL
                adaptations.append({'title': title, 'url': link})

        return adaptations[:5]  # Ограничиваем количество экранизаций, если нужно
    else:
        return None

async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Привет! Отправь название книги, а я пришлю её экранизации')

async def handle_message(update: Update, context: CallbackContext) -> None:
    book_title = update.message.text
    # Вызов функции для проверки наличия книги в базе данных
    book_id = book_in_db(book_title)

    if book_id:
        await update.message.reply_text(f'Книга "{book_title}" уже есть в базе.')
        
        # Если книга уже есть в базе, выводим её экранизации
        adaptations = get_adaptations(book_id)
        if adaptations:
            adaptation_messages = [f'{adapt["title"]}: {adapt["url"]}' for adapt in adaptations]
            await update.message.reply_text(f'Вот экранизации книги "{book_title}":\n' + '\n'.join(adaptation_messages))
        else:
            await update.message.reply_text(f'Экранизаций для книги "{book_title}" не найдено.')
    else:
        # Если книга отсутствует, добавляем её в базу данных
        book_id = add_book_to_db(book_title)  # Добавление книги
        await update.message.reply_text(f'Книга "{book_title}" добавлена.')
        
        # Ищем экранизации на Кинопоиске
        adaptations = get_adaptations_from_kinopoisk(book_title)
        
        if adaptations:
            # Добавляем экранизации в базу данных
            add_adaptations(book_id, adaptations)
            
            # Отправляем пользователю экранизации
            adaptation_messages = [f'{adapt["title"]}: {adapt["url"]}' for adapt in adaptations]
            await update.message.reply_text(f'Вот экранизации книги "{book_title}":\n' + '\n'.join(adaptation_messages))
        else:
            await update.message.reply_text(f'Экранизации книги "{book_title}" не найдены.')

def main() -> None:
    # Создание приложения и добавление обработчиков
    application = Application.builder().token("7317456751:AAEtgYfAqLDwzTUvS7LQv7pGf2b9vsRVT8M").build()

    # Добавление обработчиков команд
    application.add_handler(CommandHandler("start", start))
    
    # Добавление обработчика сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Запуск бота
    application.run_polling()

if __name__ == '__main__':
    main()
