import pymysql.cursors
from prettytable import PrettyTable

connection = pymysql.connect(host='localhost', user='root', password='', db='cours',
                             cursorclass=pymysql.cursors.DictCursor)

first = 'SELECT author_id, full_name, SUM(income) summed_income FROM orders JOIN publications USING (' \
        'publication_id) JOIN publications_authors USING (publication_id)JOIN authors USING (author_id) JOIN ' \
        '(SELECT publication_id, name, cost/COUNT(*) as income FROM orders JOIN publications USING (' \
        'publication_id) JOIN publications_authors USING (publication_id) JOIN authors uSING (author_id) ' \
        'GROUP BY publication_id, order_id) A USING (publication_id) GROUP BY author_id'

second = 'SELECT customer_id, SUM(cost) FROM customers JOIN orders USING (customer_id) GROUP BY customer_id ' \
         'ORDER BY SUM(cost)'


def get_column_names(table_name, cursor):
    query = 'SHOW COLUMNS FROM {}'.format(table_name)
    cursor.execute(query)
    return [row['Field'] for row in cursor]


def select_from(table_name, cursor):
    column_names = get_column_names(table_name, cursor)
    table = PrettyTable(column_names)
    query = 'SELECT * FROM {}'.format(table_name)
    cursor.execute(query)
    for row in cursor:
        table.add_row(list(row.values()))
    print(table)


def show_tables(cursor):
    query = 'SHOW TABLES'
    cursor.execute(query)
    for row in cursor:
        print(row['Tables_in_cours'])


def execute_query(query, cursor):
    query = eval(query)
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    table = PrettyTable(column_names)
    for row in cursor:
        table.add_row(list(row.values()))
    print(table)


def insert_into(table_name, cursor):
    column_names = get_column_names(table_name, cursor)
    row_values = []
    for name in column_names:
        print(name)
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
    print('select <table-name> - shows table rows;\ninsert <table-name> - inserts new raw to the table;'
          '\nshow - shows tables names;\nquery <query number> - executes query (\'first\' or \'second\')')


dictionary = {'select': select_from, 'insert': insert_into, 'show': show_tables, 'query': execute_query, 'help': help}

if __name__ == '__main__':
    with connection.cursor() as cursor:
        while True:
            command = input()
            command = command.split(' ')
            try:
                if command[0] == 'exit':
                    break
                elif command[0] == 'show' or command[0] == 'help':
                    dictionary.get(command[0])(cursor)
                else:
                    dictionary.get(command[0])(command[1], cursor)
            except (TypeError, IndexError) as e:
                print('Wrong command, try \'help\'')
            except NameError:
                print('Wrong query name')
            except pymysql.err.ProgrammingError:
                print('Wrong input values')
