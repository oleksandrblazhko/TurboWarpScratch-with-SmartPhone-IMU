import asyncio
import json
import math
import websockets
import aiohttp
import threading
import time
import keyboard
import numpy as np
import os
import signal
import winsound
import argparse

# Увімкнення підтримки ANSI-кольорів у терміналі Windows
os.system("")

COLOR_GREEN = "\033[92m"
COLOR_RED = "\033[91m"
COLOR_RESET = "\033[0m"

# --- Глобальні змінні та налаштування ---

# Змінні для калібрування
delta_accX = 0.0
delta_accY = 0.0

# Стан калібрування
class CalibrationState:
    IDLE = 0
    CALIBRATING = 1
    DONE = 2

calibration_state = CalibrationState.IDLE
calibration_data = []

# Множина для зберігання всіх підключених клієнтів WebSocket
CONNECTED_CLIENTS = set()

# Словник для зберігання останніх даних з сенсорів, включаючи розраховані кути
SENSOR_DATA = {
    "accX": 0,
    "accY": 0,
    "angle_x": 0,
    "angle_y": 0
}

# Базова частина IP-адреси
BASE_IP = "192.168.0."
DEFAULT_PORT = 8080

# URL-адреса HTTP-сервера, звідки беруться дані (буде встановлена після пошуку)
HTTP_SERVER_URL = None

# Коефіцієнт масштабування для перетворення значень акселерометра в кути
# Визначено експериментально: accX=5.0 при 30°, accX=8.45 при 45°
SCALING_FACTOR = 11.0

# Коефіцієнт згладжування для фільтра (EMA)
# 0.2 = сильне згладжування, 0.8 = слабке згладжування
ALPHA = 0.7

# Фільтровані значення
filtered_accX = 0.0
filtered_accY = 0.0

# Файл конфігурації
CONFIG_FILE = "config.json"

def clamp(value, min_val, max_val):
    """Допоміжна функція, що обмежує значення в заданому діапазоні [min_val, max_val]."""
    return max(min_val, min(value, max_val))

def format_server_url(ip_address):
    """
    Формує повний URL для звернення до сенсорів за заданою IP-адресою.
    """
    cleaned = ip_address.replace("http://", "").replace("https://", "").rstrip("/")
    cleaned = cleaned.split("/")[0]
    if ":" in cleaned:
        ip, port = cleaned.split(":", 1)
    else:
        ip, port = cleaned, DEFAULT_PORT
    return f"http://{ip}:{port}/get?accX&accY"

def load_config():
    """
    Завантажує конфігурацію з файлу.
    Повертає список збережених IP-адрес.
    """
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                ips = config.get("server_ips", [])
                if isinstance(ips, str):
                    ips = [ips]
                elif not isinstance(ips, list):
                    ips = []

                single_ip = config.get("server_ip")
                if single_ip and isinstance(single_ip, str) and single_ip not in ips:
                    ips.append(single_ip)

                unique_ips = []
                for ip in ips:
                    if isinstance(ip, str):
                        ip_clean = ip.strip()
                        if ip_clean and ip_clean not in unique_ips:
                            unique_ips.append(ip_clean)

                if unique_ips:
                    print(f"Завантажено збережені адреси ({len(unique_ips)}): {', '.join(unique_ips)}")
                    return unique_ips
        except (json.JSONDecodeError, IOError) as e:
            print(f"Помилка читання конфігурації: {e}")
    return []

def save_config(current_ip, ip_list=None):
    """
    Зберігає активну IP-адресу та оновлений список IP-адрес у файл конфігурації.
    Активна адреса переміщується на початок списку.
    """
    try:
        cleaned = current_ip.replace("http://", "").replace("https://", "").rstrip("/")
        cleaned = cleaned.split("/")[0]

        if ip_list is None:
            ip_list = load_config()

        updated_ips = [cleaned]
        for item in ip_list:
            if isinstance(item, str):
                c = item.replace("http://", "").replace("https://", "").rstrip("/").split("/")[0]
                if c and c not in updated_ips:
                    updated_ips.append(c)

        config = {
            "server_ip": cleaned,
            "server_ips": updated_ips
        }
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        print(f"Адресу {cleaned} збережено у конфігурації.")
    except IOError as e:
        print(f"Помилка запису конфігурації: {e}")

async def check_server_available(session, ip_address):
    """
    Перевіряє доступність HTTP-сервера за вказаною IP-адресою.
    Повертає True, якщо сервер доступний і повертає коректні дані.
    """
    url = format_server_url(ip_address)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as response:
            if response.status == 200:
                data = await response.json()
                # Перевіряємо, чи є дані в буфері (не null)
                accX = data.get("buffer", {}).get("accX", {}).get("buffer", [None])[0]
                accY = data.get("buffer", {}).get("accY", {}).get("buffer", [None])[0]
                if accX is not None and accY is not None:
                    return True, url
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError):
        pass
    return False, None

async def find_server():
    """
    Шукає доступний HTTP-сервер, перевіряючи діапазони адрес 100-105 та 160-165.
    Повертає URL знайденого сервера або None, якщо сервер не знайдено.
    """
    async with aiohttp.ClientSession() as session:
        # Перший діапазон: 100-110
        print("Пошук сервера в діапазоні 192.168.0.100-105...")
        for i in range(100, 105):
            ip = f"{BASE_IP}{i}"
            available, url = await check_server_available(session, ip)
            if available:
                print(f"Знайдено сервер: {url}")
                return url
        
        # Другий діапазон: 161-170
        print("Пошук сервера в діапазоні 192.168.0.160-165...")
        for i in range(160, 165):
            ip = f"{BASE_IP}{i}"
            available, url = await check_server_available(session, ip)
            if available:
                print(f"Знайдено сервер: {url}")
                return url
    
    return None

def get_user_ip():
    """
    Запитує у користувача IP-адресу сервера смартфона.
    Підтримує введення повної IP-адреси (наприклад, 10.110.54.23 або 192.168.1.15)
    або останнього числа для підмережі 192.168.0.x (наприклад, 105).
    """
    while True:
        try:
            user_input = input("Введіть IP-адресу смартфона (наприклад, 10.110.54.23 або 192.168.0.105): ").strip()
            if not user_input:
                continue

            cleaned = user_input.replace("http://", "").replace("https://", "").rstrip("/")
            cleaned = cleaned.split("/")[0]

            # Якщо введено одне число - вважаємо це останнім октетом для BASE_IP
            if cleaned.isdigit():
                last_octet = int(cleaned)
                if 1 <= last_octet <= 254:
                    ip = f"{BASE_IP}{last_octet}"
                    print(f"Використовується адреса: {ip}")
                    return ip
                else:
                    print("Число має бути в діапазоні від 1 до 254.")
                    continue

            # Перевірка на формат повної IP-адреси (можливо з портом)
            ip_only = cleaned.split(":")[0]
            octets = ip_only.split(".")
            if len(octets) == 4 and all(o.isdigit() and 0 <= int(o) <= 255 for o in octets):
                return cleaned
            else:
                print("Некоректний формат IP-адреси. Введіть коректну адресу (наприклад, 192.168.1.15).")
        except ValueError:
            print("Будь ласка, введіть коректне значення.")

async def register_client(websocket):
    """
    Реєструє нового клієнта, що підключився, 
    і утримує з'єднання відкритим до його закриття.
    """
    CONNECTED_CLIENTS.add(websocket)
    print(f"Новий клієнт підключився. Всього клієнтів: {len(CONNECTED_CLIENTS)}")
    try:
        # Очікуємо, поки клієнт не від'єднається
        await websocket.wait_closed()
    finally:
        # Видаляємо клієнта з множини після від'єднання
        CONNECTED_CLIENTS.remove(websocket)
        print(f"Клієнт від'єднався. Всього клієнтів: {len(CONNECTED_CLIENTS)}")

async def data_loop():
    """
    Головний цикл програми: періодично запитує дані з HTTP-сервера,
    обчислює кути нахилу та транслює їх усім підключеним клієнтам.
    """
    global calibration_state, delta_accX, delta_accY, calibration_data, filtered_accX, filtered_accY

    prev_angle_x = 0.0
    prev_angle_y = 0.0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(HTTP_SERVER_URL) as response:
                    if response.status == 200:
                        data = await response.json()
                        raw_accX = data.get("buffer", {}).get("accX", {}).get("buffer", [0])[-1]
                        raw_accY = data.get("buffer", {}).get("accY", {}).get("buffer", [0])[-1]

                        accX = raw_accX
                        accY = raw_accY

                        if calibration_state == CalibrationState.CALIBRATING:
                            calibration_data.append((accX, accY))
                            continue

                        if calibration_state == CalibrationState.DONE:
                            accX -= delta_accX
                            accY -= delta_accY

                        # Застосовуємо фільтр низьких частот (EMA)
                        filtered_accX = ALPHA * accX + (1 - ALPHA) * filtered_accX
                        filtered_accY = ALPHA * accY + (1 - ALPHA) * filtered_accY

                        ratio_x = clamp(filtered_accX / SCALING_FACTOR, -1.0, 1.0)
                        ratio_y = clamp(filtered_accY / SCALING_FACTOR, -1.0, 1.0)

                        angle_x = math.degrees(math.asin(ratio_x))
                        angle_y = math.degrees(math.asin(ratio_y))

                        SENSOR_DATA.update({
                            "accX": filtered_accX, "accY": filtered_accY,
                            "angle_x": angle_x, "angle_y": angle_y
                        })

                        if calibration_state == CalibrationState.DONE:
                            delta_x = angle_x - prev_angle_x
                            delta_y = angle_y - prev_angle_y

                            if delta_x > 0.05:
                                color_x = COLOR_GREEN
                            elif delta_x < -0.05:
                                color_x = COLOR_RED
                            else:
                                color_x = COLOR_RESET

                            if delta_y > 0.05:
                                color_y = COLOR_GREEN
                            elif delta_y < -0.05:
                                color_y = COLOR_RED
                            else:
                                color_y = COLOR_RESET

                            print(f"\r[Нахил] angle_x: {color_x}{angle_x:+6.1f}°{COLOR_RESET} | angle_y: {color_y}{angle_y:+6.1f}°{COLOR_RESET}   ", end="", flush=True)

                            prev_angle_x = angle_x
                            prev_angle_y = angle_y
                    else:
                        print(f"Помилка отримання даних: HTTP {response.status}")
            except aiohttp.ClientError as e:
                print(f"Помилка підключення до HTTP-сервера: {e}")
            except json.JSONDecodeError:
                print("Помилка: не вдалося розкодувати JSON.")

            if CONNECTED_CLIENTS:
                message = json.dumps({
                    "accX": SENSOR_DATA["accX"],
                    "accY": SENSOR_DATA["accY"],
                    "angle_x": SENSOR_DATA["angle_x"],
                    "angle_y": SENSOR_DATA["angle_y"]
                })
                # Використовуємо gather з return_exceptions=True, щоб уникнути падіння циклу,
                # якщо один з клієнтів від'єднався.
                await asyncio.gather(*[client.send(message) for client in CONNECTED_CLIENTS], return_exceptions=True)

            await asyncio.sleep(0.09)  # 0.05 - оновлення 20 Гц, 0.02 - 50 Гц

def calibration_thread():
    """
    Потік для виконання калібрування.
    """
    global calibration_state, delta_accX, delta_accY, calibration_data
    
    print("Режим калібрування. Тримайте смартфон у стані спокою впродовж 5 секунд")
    calibration_data = []
    calibration_state = CalibrationState.CALIBRATING
    
    for i in range(3, -1, -1):
        if i == 0:
            print(f"{i} .......")
            winsound.Beep(1000, 700)
        else:
            print(f"{i} ...")
            winsound.Beep(1000, 200)
        time.sleep(1)
        
    if calibration_data:
        accX_data, accY_data = zip(*calibration_data)
        delta_accX = np.mean(accX_data)
        delta_accY = np.mean(accY_data)
        print(f"Калібрування завершено: delta_accX={delta_accX:.2f}, delta_accY={delta_accY:.2f}")
    else:
        print("Не вдалося отримати дані для калібрування.")

    calibration_state = CalibrationState.DONE

def input_handler():
    """
    Обробник введення з клавіатури для керування програмою.
    """
    global calibration_state
    
    while True:
        key = keyboard.read_key()
        if key == 'c':
            if calibration_state != CalibrationState.CALIBRATING:
                cal_thread = threading.Thread(target=calibration_thread)
                cal_thread.start()
        elif key == 'q':
            print("\nЗавершення роботи...")
            os.kill(os.getpid(), signal.SIGINT)
            break
        time.sleep(0.1)


async def main_async(port=8767):
    """Основна функція, яка запускає WebSocket-сервер та цикл обробки даних."""
    global HTTP_SERVER_URL
    
    print("Перевірка доступності HTTP-сервера...")
    saved_ips = load_config()
    
    server_url = None
    working_ip = None
    
    # 1. Переглядаємо список збережених адрес для пошуку доступної поточної
    if saved_ips:
        print("Пошук доступного сервера серед збережених адрес...")
        async with aiohttp.ClientSession() as session:
            for ip in saved_ips:
                print(f"Перевірка збереженої адреси: {ip}...")
                available, url = await check_server_available(session, ip)
                if available:
                    print(f"Знайдено сервер за збереженою адресою: {url}")
                    server_url = url
                    working_ip = ip
                    break
                else:
                    print(f"Адреса {ip} недоступна.")
    
    # 2. Якщо доступних адрес у списку немає, запитуємо введення нової адреси
    if server_url is None:
        if saved_ips:
            print("\nЖодна зі збережених адрес не відповідає.")
        else:
            print("\nСписок збережених адрес порожній або файл конфігурації відсутній.")
            
        async with aiohttp.ClientSession() as session:
            while server_url is None:
                new_ip = get_user_ip()
                print(f"Перевірка введеної адреси: {new_ip}...")
                available, url = await check_server_available(session, new_ip)
                if available:
                    print(f"Знайдено сервер: {url}")
                    server_url = url
                    working_ip = new_ip
                    break
                else:
                    print(f"Сервер за адресою {new_ip} не відповідає.")
                    choice = input("Оберіть дію: [1 - ввести іншу адресу, 2 - повторити перевірку, 3 - продовжити з цією адресою]: ").strip()
                    if choice == "2":
                        while True:
                            print(f"Повторна перевірка адреси: {new_ip}...")
                            available, url = await check_server_available(session, new_ip)
                            if available:
                                print(f"Знайдено сервер: {url}")
                                server_url = url
                                working_ip = new_ip
                                break
                            else:
                                print(f"Адреса {new_ip} все ще недоступна.")
                                sub = input("Повторити перевірку ще раз? (y/n): ").strip().lower()
                                if sub not in ("y", "yes", "так", "т", ""):
                                    break
                    elif choice == "3":
                        server_url = format_server_url(new_ip)
                        working_ip = new_ip
                        break
                    else:
                        print("Спробуйте ввести нову адресу.")
    
    # Зберігаємо робочу адресу у файл конфігурації (переміщуючи її на початок списку)
    if working_ip:
        save_config(working_ip, saved_ips)
    
    HTTP_SERVER_URL = server_url
    print(f"Підключення до сервера: {HTTP_SERVER_URL}")
    
    server = await websockets.serve(register_client, "localhost", port)
    print(f"WebSocket-сервер запущено на ws://localhost:{port}")
    
    # Always print keyboard control hints
    print("Клавіші керування: c - калібрування стану спокою, q - завершення роботи")

    data_task = asyncio.create_task(data_loop())

    try:
        await data_task
    except asyncio.CancelledError:
        pass
    finally:
        server.close()
        await server.wait_closed()

def main():
    global HTTP_SERVER_URL

    parser = argparse.ArgumentParser(description="WebSocket-сервер для трансляції даних з акселерометра.")
    parser.add_argument("--port", type=int, default=8767, help="Порт для WebSocket-сервера (за замовчуванням: 8767).")
    args = parser.parse_args()

    input_thread = threading.Thread(target=input_handler)
    input_thread.daemon = True
    input_thread.start()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main_async(port=args.port))
    except KeyboardInterrupt:
        print("\nПрограму зупинено.")
    finally:
        tasks = asyncio.all_tasks(loop=loop)
        for task in tasks:
            task.cancel()

        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
        loop.close()


# Точка входу в програму
if __name__ == "__main__":
    main()
