import csv

from flask import Flask, render_template, request, jsonify
import logging as lg
import mysql.connector as connection
lg.basicConfig(filename="logs.log", level=lg.INFO, format='%(levelname)s %(asctime)s %(message)s')

app = Flask(__name__)

@app.route('/createTable', methods=['POST'])
def create_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        table_name = request.json['tableName']
        columns = request.json['columns']
        lg.info(columns)
        col_to_string = ""
        for value in columns:
            col_to_string += value+" "+columns[value]+","
        col_to_string = col_to_string[:-1]
        lg.info(col_to_string)
        q = "Create table {0}({1});".format(table_name, col_to_string)
        lg.info(q)
        c.execute(q)
        my_db.commit()
        lg.info("Commit")
        return "Table created successfully"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")
@app.route('/insertIntoTable', methods=['POST'])
def insert_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        table_name = request.json['tableName']
        values = request.json['values']
        lg.info(values)
        val_to_string = ""
        val = ""
        for value in values:
            val += value+","
            if(type(values[value])==str):
                val_to_string += "'"+values[value]+"'"+","
            else:
                val_to_string += str(values[value])+","
        val = val[:-1]
        val_to_string = val_to_string[:-1]
        lg.info(val_to_string)
        q = "INSERT INTO {0} ({1}) values ({2});".format(table_name, val, val_to_string)
        lg.info(q)
        c.execute(q)
        my_db.commit()
        lg.info("Commit")
        return "Inserted successfully"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")

@app.route('/updateTable', methods=['POST'])
def update_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        table_name = request.json['tableName']
        where = request.json['where']
        to_be = request.json['set']
        for i in where:
            w = i
            if(type(where[i])==str):
                value = "'"+where[i]+"'"
            else:
                value = where[i]
        for j in to_be:
            col = j
            if (type(to_be[j]) == str):
                val_col = "'"+to_be[j]+"'"
            else:
                val_col = to_be[j]
        q = "UPDATE {0} SET {1}={2} where {3}={4};".format(table_name, col, val_col, w, value)
        lg.info(q)
        c.execute(q)
        my_db.commit()
        lg.info("Commit")
        return "Updated successfully"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")

@app.route('/bulkInsertIntoTable', methods=['POST'])
def bulk_insert_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        with open('bulk.csv', "r") as f:
            data = csv.reader(f, delimiter="\n")
            for line in enumerate(data):
                for list_ in (line[1]):
                    new_list = list_.split(",")
                    val = ""
                    for i in new_list:
                        if (type(i)==str):
                            val += "'"+i+"'"+","
                        else:
                            val += i+","
                    val = val[:-1]
                    lg.info(val)
                    c.execute('INSERT INTO Student values ({values})'.format(values=(val)))
        my_db.commit()
        lg.info("Commit")
        return "Inserted successfully"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")

@app.route('/deleteFromTable', methods=['POST'])
def delete_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        table_name = request.json['tableName']
        where = request.json['where']
        for i in where:
            w = i
            if (type(where[i]) == str):
                value = "'" + where[i] + "'"
            else:
                value = where[i]
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        c.execute('DELETE FROM {0} where {1}={2}'.format(table_name, w, value))
        my_db.commit()
        lg.info("Commit")
        return "Deleted successfully"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")
@app.route('/downloadTable', methods=['POST'])
def download_table():

    try:
        my_db = connection.connect(host="localhost", database="automation", user="root", passwd="password", use_pure=True)
        # check if the connection is established
        table_name = request.json['tableName']
        limit = request.json['limit']
        lg.info(my_db.is_connected())
        c = my_db.cursor()
        if(limit=="all"):
            c.execute('SELECT * FROM {0};'.format(table_name))
        else:
            c.execute('SELECT * FROM {0} LIMIT {1}'.format(table_name, limit))
        result = c.fetchall()
        headers = c.description
        columns = []
        for head in headers:
            columns.append(head[0])
        with open('output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(list(result))
        my_db.commit()
        lg.info("Commit")
        return "Downloaded successfully in output.csv"
    except Exception as e:
        lg.error(str(e))
        return "Some error has occurred"
    finally:
        my_db.close()
        lg.info("DB is closed.")


if __name__ == '__main__':
    app.run()
