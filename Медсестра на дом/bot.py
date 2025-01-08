import logging
import httpx
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import date, datetime

# Telegram Bot Token
API_TOKEN = "токен тг бота"

# API URL
API_BASE_URL = "http://localhost:8000"  

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# States for FSM
class RegisterRequest(StatesGroup):
    waiting_for_name = State()
    waiting_for_phone = State()
    waiting_for_region = State()
    waiting_for_service = State()



# Start command
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    await message.answer(
        "Добро пожаловать! Я помогу вам зарегистрировать заявку. "
        "Введите /new_request, чтобы создать новую заявку."
    )

# Command to create a new request
@dp.message(Command("new_request"))
async def new_request(message: types.Message):
    kb = [
       [
           types.KeyboardButton(text="Отправить заявку"),
        #    types.KeyboardButton(text="А это?")
       ],
   ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb,resize_keyboard=True)
    # await message.reply("Привет!\nЯ Эхобот от Skillbox!\nОтправь мне любое сообщение, а я тебе обязательно отвечу.", reply_markup=keyboard)
    # keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    # keyboard.add(KeyboardButton("Отправить заявку"))
    await message.answer(
        "Нажмите 'Отправить заявку', чтобы продолжить.",
        reply_markup=keyboard,
    )

# Collect client information
@dp.message(lambda msg: msg.text == "Отправить заявку")
async def collect_request_data(message: types.Message, state: FSMContext):
    await state.set_state(RegisterRequest.waiting_for_name)
    await message.answer("Введите ваше имя:")


@dp.message(StateFilter(RegisterRequest.waiting_for_name))
async def get_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(RegisterRequest.waiting_for_phone)
    await message.answer("Введите ваш номер телефона (в формате +123456789):")


@dp.message(StateFilter(RegisterRequest.waiting_for_phone))
async def get_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not phone.startswith("+") or not phone[1:].isdigit():
        await message.answer("Пожалуйста, введите номер телефона в правильном формате (например, +123456789).")
        return
    await state.update_data(phone=phone)
    await state.set_state(RegisterRequest.waiting_for_region)
    await message.answer("Введите ваш регион (ID региона):")


@dp.message(StateFilter(RegisterRequest.waiting_for_region))
async def get_region(message: types.Message, state: FSMContext):
    try:
        region_id = int(message.text.strip())
        if region_id <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Регион должен быть положительным числом. Попробуйте снова.")
        return
    await state.update_data(region_id=region_id)
    await state.set_state(RegisterRequest.waiting_for_service)
    await message.answer("Введите описание услуги:")


@dp.message(StateFilter(RegisterRequest.waiting_for_service))
async def get_service(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    user_data["service_description"] = message.text.strip()

    async with httpx.AsyncClient() as client:
        try:
            # Add client via API
            client_response = await client.post(
                f"{API_BASE_URL}/clients",
                json={
                    "name": user_data["name"],
                    "phone": user_data["phone"],
                    "region_id": user_data["region_id"],
                },
            )
            client_response.raise_for_status()
            client_data = client_response.json()

            # Add request via API
            request_response = await client.post(
                f"{API_BASE_URL}/requests",
                json={
                    "client_id": client_data["client_id"],
                    "nurse_id": 0,  # Пока без назначения
                    "service_date": datetime.now().strftime("%Y-%m-%d"),
                    "status": "Pending",
                    "service_cost": 0.0,
                    # "description": user_data["service_description"],
                },
            )
            request_response.raise_for_status()

        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP Error: {e.response.text}")
            await message.answer("Произошла ошибка при обработке данных. Проверьте данные и повторите.")
            await state.clear()
            return
        except Exception as e:
            logging.error(f"Unexpected Error: {e}")
            await message.answer("Произошла неожиданная ошибка. Попробуйте позже.")
            await state.clear()
            return

    await message.answer("Ваша заявка успешно зарегистрирована!")
    await state.clear()


# Main function
async def main():
    logging.info("Бот запущен и готов к работе!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
