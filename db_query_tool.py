import csv
import sys

def connect_mysql(params):
    import mysql.connector
    try:
        conn = mysql.connector.connect(
            host=params['host'],
            user=params['user'],
            password=params['password'],
            database=params['database'],
            port=params.get('port', 3306)
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Ошибка подключения к MySQL/MariaDB: {err}")
        sys.exit(1)

def connect_postgresql(params):
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=params['host'],
            user=params['user'],
            password=params['password'],
            dbname=params['database'],
            port=params.get('port', 5432)
        )
        return conn
    except psycopg2.Error as err:
        print(f"Ошибка подключения к PostgreSQL: {err}")
        sys.exit(1)

def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            return headers, rows
        else:
            conn.commit()
            return None, None
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None, None
    finally:
        cursor.close()

def save_to_csv(headers, rows, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Результаты сохранены в {filename}")

def main():
    print("Выберите СУБД:")
    print("1. MySQL/MariaDB")
    print("2. PostgreSQL")
    db_choice = input("Введите номер: ")

    params = {}
    params['host'] = input("Хост (например, localhost): ")
    params['user'] = input("Пользователь: ")
    params['password'] = input("Пароль: ")
    params['database'] = input("База данных: ")

    if db_choice == '1':
        conn = connect_mysql(params)
    elif db_choice == '2':
        conn = connect_postgresql(params)
    else:
        print("Некорректный выбор")
        return

    print("Введите SQL-запрос (завершите пустой строкой для выполнения):")
    query_lines = []
    while True:
        line = input()
        if line.strip() == '':
            break
        query_lines.append(line)
    query = ' '.join(query_lines)

    headers, rows = execute_query(conn, query)

    if rows is not None and headers is not None:
        print("Вывод результатов:")
        print('\t'.join(headers))
        for row in rows:
            print('\t'.join(str(x) for x in row))

        save = input("Сохранить результаты в CSV? (y/n): ").lower()
        if save == 'y':
            filename = input("Введите имя файла (например, result.csv): ")
            save_to_csv(headers, rows, filename)

    conn.close()

if __name__ == "__main__":
    main()
