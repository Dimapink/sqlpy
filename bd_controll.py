import psycopg2


class BdForWork:
    def __init__(self, database: str = "clients_db", user: str = "postgres", password: str = "wisla"):
        self.database = database
        self.user = user
        self.password = password
        try:
            conn = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print(f"Возникла ошибка при работе с БД: {e}")
        else:
            self.connection = conn

    def exec(self, query: str = None):
        with self.connection.cursor() as cur:
            try:
                cur.execute("select * from music.artist;")
            except Exception as e:
                print(e)
            else:
                print(cur.fetchall())

    def exit(self):
        self.connection.close()


if __name__ == "__main__":
    a = BdForWork()
    a.exec()
