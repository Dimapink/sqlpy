import psycopg2
from builtins import enumerate


def create_db(connection):
    """
    Функция для создания базы данных
    :param connection:
    :return:
    """
    with connection.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS clients;
        CREATE TABLE IF NOT EXISTS clients.client (
        id SERIAL PRIMARY KEY, 
        first_name VARCHAR(255) NOT NULL,
        second_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS clients.phones (
        id SERIAL PRIMARY KEY, 
        user_id int references clients.client(id),
        phone VARCHAR(15) UNIQUE
        );""")
        print("Cхема и база данных созданы")


def add_client(connection, first_name, last_name, email=None, phones: list[str] = None):
    """
    Функция для добавления клиента в БД
    :param connection: экземпляр соединенния с БД
    :param first_name: Имя клиента
    :param last_name: Фамилия
    :param email: Адрес почты (должен быть уникальным)
    :param phones: список номеров телефонов
    :return:
    """
    with connection.cursor() as cur:
        cur.execute("INSERT INTO clients.client(first_name, second_name, email) "
                    "VALUES (%s, %s, %s) RETURNING id;", (first_name, last_name, email))
        user_id = cur.fetchone()
        print(f"Создан пользователь id: {user_id[0]}")
        if not phones:
            return user_id
        else:
            for phone in phones:
                cur.execute(f"INSERT INTO clients.phones(user_id, phone) "
                            f"VALUES (%s, %s);", (user_id[0], phone))
                connection.commit()
            return user_id


def add_phone(connection, client_id: int, phone: str):
    """
    Функция добавления телефона для клиента
    :param connection: экземпляр соединенния с БД
    :param client_id: id пользователя
    :param phone: новый телефон пользоватлея
    :return: None
    """
    with connection.cursor() as cur:
        cur.execute(f"INSERT INTO clients.phones(user_id, phone) "
                    f"VALUES (%s, %s);", (client_id, phone))
        connection.commit()
        print(f"Для пользователя id {client_id} добавлен телефон {phone}")


def change_client(connection, client_id, first_name: str = None,
                  last_name: str = None, email: str = None,
                  old_phone: str = None, new_phone: str = None):
    """
    Функция для изменеия данных о пользователе
    :param connection: экземпляр соединенния с БД
    :param client_id: id пользователя
    :param first_name: новое имя
    :param last_name: новая фамилия
    :param email: новая почта
    :param old_phone: старый телефон
    :param new_phone: новый телефон
    :return: NONE
    """
    base = "UPDATE clients.client "
    end = "WHERE id = %s"
    with connection.cursor() as cur:
        if first_name:
            cur.execute(base + 'SET first_name=%s' + end, (first_name, client_id))
        elif last_name:
            cur.execute(base + 'SET second_name=%s' + end, (last_name, client_id))
        elif email:
            cur.execute(base + 'SET email=%s' + end, (email, client_id))
        elif old_phone and new_phone:
            cur.execute("UPDATE clients.phones "
                        "SET phone =%s WHERE user_id = %s and phone = %s", (new_phone, client_id, old_phone))
        connection.commit()
        print(f"Данные пользоватея {client_id} изменены")


def delete_phone(connection, client_id, phone=None):
    base = "DELETE FROM clients.phones "
    with connection.cursor() as cur:
        if phone:
            cur.execute(base + "WHERE phones.user_id = %s and phones.phone = %s", (client_id, phone))
            print(f"Телефон {phone} для пользователя {client_id} удален")
        else:
            cur.execute(base + "WHERE phones.user_id = %s", (client_id,))
        connection.commit()


def delete_client(connection, client_id):
    delete_phone(connection, client_id)
    base = "DELETE FROM clients.client "
    with connection.cursor() as cur:
        cur.execute(base + "WHERE client.id = %s", (client_id,))
        connection.commit()
    print(f"Клиент {client_id} удален")


def find_client(connection, first_name=None, last_name=None, email=None, phone=None):
    base = "SELECT * FROM clients.client "
    join = "LEFT JOIN clients.phones on client.id = phones.user_id "
    with connection.cursor() as cur:
        if first_name:
            cur.execute(base + join + "WHERE client.first_name = %s", (first_name,))
        elif last_name:
            cur.execute(base + join + "WHERE client.second_name = %s", (last_name,))
        elif email:
            cur.execute(base + join + "WHERE client.email = %s", (email,))
        else:
            cur.execute(base + join + "WHERE phones.phone = %s", (phone,))
        found_data = cur.fetchall()
    return found_data


def parse_found_data(found_data: list[tuple]):
    payload = {}
    for n, user in enumerate(found_data, 1):
        if user[3] not in payload:
            if not user[-1]:
                payload.setdefault(user[3],
                                   f"{n}) Имя: {user[1]}, Фамилия: {user[2]}, Почта: {user[3]}, Телефон(ы): нет")
            else:
                payload.setdefault(user[3],
                                   f"{n}) Имя: {user[1]}, Фамилия: {user[2]}, Почта: {user[3]}, Телефон(ы): {user[-1]}")
        else:
            payload[user[3]] += f", {user[-1]}"
    return payload


def post_found_user(found_data):
    if len(found_data) == 0:
        return 'Не найдено ни одного пользователя :('
    else:
        title_text = 'Найдены следующие пользователи:\n'
        payload = ';\n'.join(parse_found_data(found_data).values())
        return title_text + payload


if __name__ == "__main__":
    with psycopg2.connect(host="192.168.14.141", database="clients_db", user="wisla", password="wisla") as conn:
        create_db(conn)  # создание схемы и таблиц
        # создание пользователя без номеров телефона
        add_client(conn, 'Первый', 'Пользователь', 'first@user.ru')
        # создание пользователя с 1 телефоном
        add_client(conn, 'Второй', 'Пользователь',
                         'second@user.ru', ['1-234-567-89-10'])
        # создание пользователя с n телефонами
        add_client(conn, 'Третий', 'Пользователь',
                         'third@user.ru', ['1-234-567-89-11', '1-131-415-16-17'])
        # добавление телефона пользователю
        print("----")
        print(post_found_user(find_client(conn, email='first@user.ru')))
        print("----")
        add_phone(conn, 1, "0-987-654-32-10")
        print("----")
        print(post_found_user(find_client(conn, email='first@user.ru')))
        print("----")
        print("Изменение пользователя:")
        print(post_found_user(find_client(conn, email='second@user.ru')))
        change_client(conn, 2, last_name="Change last-name")
        print(post_found_user(find_client(conn, email='second@user.ru')))
        print("----")
        print("Поиск:")
        print('По номеру телефона:')
        print(post_found_user(find_client(conn, phone='1-131-415-16-17')))
        print("----")
        print('По фамилии (найдено несколько записей)')
        print(post_found_user(find_client(conn, last_name='Пользователь')))
        print("----")
        print('По Имени (без результата)')
        print(post_found_user(find_client(conn, first_name='Пользователь')))
        print(post_found_user(find_client(conn, email='third@user.ru')))
        print('Удаление телефона')
        print(post_found_user(find_client(conn, email='third@user.ru')))
        delete_phone(conn, 3, '1-234-567-89-11')
        print(post_found_user(find_client(conn, email='third@user.ru')))
        print("----")
        print("----")
        print('Удаление клиента')
        print(post_found_user(find_client(conn, email='third@user.ru')))
        delete_client(conn, 3)


