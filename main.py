from multiprocessing import Process, Queue, Event, Lock
import time


# Процесс обработки данных
def processor(data_queue, storage_queue, log_queue, stop_event):
    while not stop_event.is_set():
        if not data_queue.empty():
            message = data_queue.get()

            if message == "STOP":
                break

            # Простая обработка данных
            result = message["content"].upper()

            storage_queue.put(result)

            log_queue.put(
                f"Обработчик получил: {message['content']}"
            )

        time.sleep(0.2)


# Процесс хранения данных
def storage(storage_queue, log_queue, stop_event, lock):
    while not stop_event.is_set():
        if not storage_queue.empty():
            data = storage_queue.get()

            if data == "STOP":
                break

            with lock:
                with open("storage.txt", "a", encoding="utf-8") as file:
                    file.write(data + "\n")

            log_queue.put(
                f"Хранилище сохранило: {data}"
            )

        time.sleep(0.2)


# Процесс логирования
def logger(log_queue, stop_event, lock):
    while not stop_event.is_set() or not log_queue.empty():
        if not log_queue.empty():
            log = log_queue.get()

            with lock:
                print(log)

                with open("log.txt", "a", encoding="utf-8") as file:
                    file.write(log + "\n")

        time.sleep(0.2)


if __name__ == "__main__":

    # Очереди
    data_queue = Queue()
    storage_queue = Queue()
    log_queue = Queue()

    # Событие завершения
    stop_event = Event()

    # Блокировка
    lock = Lock()

    # Создаём процессы
    process_1 = Process(
        target=processor,
        args=(data_queue, storage_queue, log_queue, stop_event)
    )

    process_2 = Process(
        target=storage,
        args=(storage_queue, log_queue, stop_event, lock)
    )

    process_3 = Process(
        target=logger,
        args=(log_queue, stop_event, lock)
    )

    # Запускаем
    process_1.start()
    process_2.start()
    process_3.start()

    # Сообщения с метаданными
    messages = [
        {
            "sender": "main",
            "receiver": "processor",
            "type": "DATA",
            "content": "Первое сообщение"
        },
        {
            "sender": "main",
            "receiver": "processor",
            "type": "DATA",
            "content": "Второе сообщение"
        },
        {
            "sender": "main",
            "receiver": "processor",
            "type": "DATA",
            "content": "Третье сообщение"
        }
    ]

    # Передаём сообщения
    for message in messages:
        data_queue.put(message)

    # Даём процессам время выполнить работу
    time.sleep(3)

    # Завершаем работу
    data_queue.put("STOP")
    storage_queue.put("STOP")

    process_1.join()
    process_2.join()

    stop_event.set()

    process_3.join()

    print("Работа завершена")