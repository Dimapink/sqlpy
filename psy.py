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
    :param phone: список yомеров телефонов
    :return:
    """
    with connection.cursor() as cur:
        cur.execute("INSERT INTO clients.client(first_name, second_name, email) "
                    "VALUES (%s, %s, %s) RETURNING id;", (first_name, last_name, email))
        user_id = cur.fetchone()
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


def delete_phone(connection, client_id, phone):
    base = "DELETE FROM clients.phones"
    with connection.cursor() as cur:
        cur.execute(base + "WHERE client.user_id = %s and client.phone = %s", (client_id, phone))
        connection.commit()


def delete_client(connection, client_id):
    base = "DELETE FROM clients.client "
    join = "LEFT JOIN clients.phones on client.id = phones.user_id "
    with connection.cursor() as cur:
        cur.execute(base + join + "WHERE client.id = %s", (client_id,))
        connection.commit()


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
    with psycopg2.connect(database="clients_db", user="postgres", password="wisla") as conn:
    # print(add_client(conn, '35', 'cdf', 'axcadzc@zxcasda.ru',
    #                  ['8-913-789-36-06', '8-913-789-37-07']))
        print(post_found_user(find_client(conn, email='axcadzc@zxcasda.ru')))
