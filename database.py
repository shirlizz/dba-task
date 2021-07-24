""""
DBA-Task: Using psycopg2 to create a database 
Authors: 
Shirley Chuquin - Oscar Vega - Gissel Cabascango - Ronny Cortez

"""
import psycopg2 as ps

def create_table(table_name):
    """ create tables in the PostgreSQL database"""
    command = """
        CREATE TABLE {}(
            admin_cedula VARCHAR PRIMARY KEY,
            admin_name VARCHAR(255) NOT NULL,
            admin_birth DATE NOT NULL
        )
        """.format(table_name)
    conn = None
    try:
        conn = ps.connect("host=localhost port=5432 dbname=clientsdb user=postgres password=PASSWORD")

        cur = conn.cursor()
        # create table one by one
        cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, ps.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_data(table_name, data_list):
    """ insert data"""
    sql = """
        INSERT INTO {name}(admin_cedula, admin_name, admin_birth) VALUES(%s, %s, %s)
        """.format(name=table_name)
    
    conn = None
    try:
        conn = ps.connect("host=localhost port=5432 dbname=clientsdb user=postgres password=PASSWORD")
        cur = conn.cursor()
        # execute query
        cur.executemany(sql,data_list)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, ps.DatabaseError) as error:
        print("Error to insert data in tables: ", error)
    finally:
        if conn is not None:
            conn.close()


def call_function(function_name, parameters):
    """ Calling a function"""
    
    conn = None
    try:
        conn = ps.connect("host=localhost port=5432 dbname=clientsdb user=postgres password=PASSWORD")
        cur = conn.cursor()
        # call function
        cur.callproc(function_name, parameters)
        row = cur.fetchone()

        for i in row:
            print(i)
            row = cur.fetchone()

        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except Exception as error:
        print("Error to call function: ", error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_table('administrators')
    admin_list = [('1004459952', 'Alberto Rojas', '1990-01-23'), ('1234555678','Tatiana Cabrera', '1978-04-15')]
    insert_data('admin', admin_list)
    name= 'Alberto Rojas'
    call_function('get_admins_name', (name,))