import pymysql.cursors
from prettytable import PrettyTable
import os

connection = pymysql.connect(host='localhost', user='root', password='', db='cours',
                             cursorclass=pymysql.cursors.DictCursor)

query_names = ['Посчитайте прибыль, принесенную печатью изданий определенного автора.'
               '\nРаспределение прибыли между авторами равномерное.',
               'Отсортируйте заказчиков в порядке суммарной суммы заказов.']
queries = ['SELECT author_id, full_name, SUM(income) summed_income FROM orders JOIN publications USING ('
           'publication_id) JOIN publications_authors USING (publication_id)JOIN authors USING (author_id) JOIN '
           '(SELECT publication_id, name, cost/COUNT(*) as income FROM orders JOIN publications USING ('
           'publication_id) JOIN publications_authors USING (publication_id) JOIN authors USING (author_id) '
           'GROUP BY publication_id, order_id) A USING (publication_id) GROUP BY author_id',
           'SELECT customer_id, SUM(cost) FROM customers JOIN orders USING (customer_id) GROUP BY customer_id '
           'ORDER BY SUM(cost)']


def get_tables_names(cursor):
    query = 'SHOW TABLES'
    cursor.execute(query)
    return [row['Tables_in_cours'] for row in cursor]


def get_column_names(table_name, cursor):
    query = 'SHOW COLUMNS FROM {}'.format(table_name)
    cursor.execute(query)
    return [row['Field'] for row in cursor]


tables_names = get_tables_names(connection.cursor())


def get_table_name():
    print('Select table:')
    table = PrettyTable(tables_names)
    for name in tables_names:
        print(name)
    return input()


def select_from(cursor):
    table_name = get_table_name()
    column_names = get_column_names(table_name, cursor)
    table = PrettyTable(column_names)
    query = 'SELECT * FROM {}'.format(table_name)
    cursor.execute(query)
    for row in cursor:
        table.add_row(list(row.values()))
    print(table)


def execute_query(cursor):
    print('Select query:')
    for i in range(len(queries)):
        print('{} - {}'.format(i + 1, query_names[i]))
    i = int(input())
    cursor.execute(queries[i - 1])
    column_names = [desc[0] for desc in cursor.description]
    table = PrettyTable(column_names)
    for row in cursor:
        table.add_row(list(row.values()))
    print(table)


def insert_into(cursor):
    table_name = get_table_name()
    column_names = get_column_names(table_name, cursor)
    row_values = []
    for name in column_names:
        print('Enter {}:'.format(name))
        value = input()
        if value == '':
            row_values.append('NULL')
        else:
            row_values.append(value)
    query = 'INSERT INTO {} VALUES({})'.format(table_name,
                                               row_values.__str__().replace('[', '').replace(']', '').replace(
                                                   '\'NULL\'', 'NULL'))
    cursor.execute(query)
    connection.commit()
    print('1 new raw was successfully added')


def help():
    print('Commands:\nshow - shows table rows;\ninsert - inserts new raw to the table;\nclear - clears screen'
          '\nshow - shows tables names;\nquery - executes query\nexit - exits from the program')


def clear():
    os.system('cls')


dictionary = {'show': select_from, 'insert': insert_into, 'query': execute_query, 'help': help, 'clear': clear}

if __name__ == '__main__':
    with connection.cursor() as cursor:
        while True:
            print('Enter command:')
            command = input().lower().strip()
            try:
                if command == 'exit':
                    break
                elif command == 'help' or command == 'clear':
                    dictionary.get(command)()
                else:
                    dictionary.get(command)(cursor)
            except (TypeError, IndexError):
                print('Wrong command, try \'help\'')
            except NameError:
                print('Wrong query name')
            except (pymysql.err.ProgrammingError, pymysql.err.IntegrityError):
                print('Wrong input values')
