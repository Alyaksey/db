import pymysql.cursors

connection = pymysql.connect(host='localhost', user='root', password='', db='cours',
                             cursorclass=pymysql.cursors.DictCursor)

first = 'SELECT author_id, full_name, SUM(income) summed_income FROM orders JOIN publications USING (' \
        'publication_id) JOIN publications_authors USING (publication_id)JOIN authors USING (author_id) JOIN ' \
        '(SELECT publication_id, name, cost/COUNT(*) as income FROM orders JOIN publications USING (' \
        'publication_id) JOIN publications_authors USING (publication_id) JOIN authors uSING (author_id) ' \
        'GROUP BY publication_id, order_id) A USING (publication_id) GROUP BY author_id'

second = 'SELECT customer_id, SUM(cost) FROM customers JOIN orders USING (customer_id) GROUP BY customer_id ' \
         'ORDER BY SUM(cost)'


def select_from(table_name):
    with connection.cursor() as cursor:
        query = 'SELECT * FROM {}'.format(table_name)
        cursor.execute(query)
        for row in cursor:
            print(row)


def show_tables():
    with connection.cursor() as cursor:
        query = 'SHOW TABLES'
        cursor.execute(query)
        for row in cursor:
            print(row['Tables_in_cours'])


def execute_query(query):
    query = eval(query)
    with connection.cursor() as cursor:
        cursor.execute(query)
        for row in cursor:
            print(row)


def insert_into(table_name):
    with connection.cursor() as cursor:
        query = 'SHOW COLUMNS FROM {}'.format(table_name)
        cursor.execute(query)
        column_names = [row['Field'] for row in cursor]
        row_values = []
        for names in column_names:
            print(names)
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
    while True:
        command = input()
        command = command.split(' ')
        try:
            if command[0] == 'exit':
                break
            elif command[0] == 'show' or command[0] == 'help':
                dictionary.get(command[0])()
            else:
                dictionary.get(command[0])(command[1])
        except (TypeError, IndexError) as e:
            print('Wrong command, try \'help\'')
        except NameError:
            print('Wrong query name')
        except pymysql.err.ProgrammingError:
            print('Wrong input values')
