import csv
import json
import sqlite3
import os

from process_sql import get_sql
from preprocess.schema import Schema, get_schemas_from_json
from transformers.models.auto import AutoTokenizer

def get_schema_info(db):
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param db: database path
    :return: schema dict
    """

    schema = {}
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # fetch table info
    for table in tables:
        cursor.execute("PRAGMA table_info({})".format(table))
        schema[table] = []
        for col in cursor.fetchall():
            schema[table].append([str(col[1].lower()), col[2], col[5]])
        #schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema

def create_sqlite(data_folder, schema_name):
    con = sqlite3.connect(os.path.join(data_folder, "asanadb.sqlite"))
    cur = con.cursor()
    with open(os.path.join(data_folder, schema_name), 'r') as sql_file:
        result_iterator = cur.executescript(sql_file.read())
    con.commit()

def create_tables_json(schema_name, output_name):
    """
    Parse that can take a schema and create tables.json file
    The result is a dictionary of lists
    """
    schema = get_schema_info(schema_name)
    # part 1 - "column_names"
    column_names = []
    column_names.append([-1, '*'])
    table_names = []
    column_types = []
    col_counter = 0
    primary_keys = []
    for k, name in enumerate(schema.keys()):
        table_names.append(name)
        for col in schema[name]:
            column_names.append([k, col[0]])
            column_types.append(col[1])
            if col[2] == 1:
                primary_keys.append(col_counter)
            col_counter = col_counter + 1

    # Go over all columns and create tuples of their names
    pass
    final_dict = {'db_id': 'asana'}
    final_dict['column_types'] = column_types
    final_dict['column_names'] = column_names
    final_dict['column_names_original'] = column_names
    final_dict['primary_keys'] = primary_keys
    final_dict['table_names'] = table_names
    final_dict['table_names_original'] = table_names
    final_dict['foreign_keys'] = [[5, 0], [6, 0], [26, 0]]
    # foreign_keys - should be added manually
    with open(output_name,"w") as dump_f:
        json.dump([final_dict], dump_f)

def create_json_from_gt_query(query, text):
    """
    Given SQL query, create the ground truth JSON
    """
    json_dict = {"db_id": "asana"}
    json_dict["query"] = query
    json_dict["question"] = text
    return json_dict

def process_ground_truth_excel(csv_in, json_out):
    list_of_dict = []
    with open(csv_in) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        for row in csv_reader:
            print(row)
            if row[0] == '' or row[1] == '':
                continue
            if row[0][0] == '\ufeff':
                row[0] = row[0].replace('asana.', '')
                row[0] = row[0].replace('Asana.', '')
                row[1] = row[1].replace('asana.', '')
                row[1] = row[1].replace('Asana.', '')
                row[1] = row[1].replace("“High”", "\'High\'")
                row[0] = row[0].replace("“High”", "\'High\'")
                json_dict = create_json_from_gt_query(row[1], row[0][1:])
            else:
                row[0] = row[0].replace('asana.', '')
                row[0] = row[0].replace('Asana.', '')
                row[1] = row[1].replace('asana.', '')
                row[1] = row[1].replace('Asana.', '')
                row[1] = row[1].replace("“High”", "\'High\'")
                row[0] = row[0].replace("“High”", "\'High\'")
                json_dict = create_json_from_gt_query(row[1], row[0])
            list_of_dict.append(json_dict)

    with open(json_out, "w") as dump_f:
        json.dump(list_of_dict, dump_f)

def prepare_dev_json(dev_json_filename, table_json_filename, dev_out_name):
    """
    dev_json_filename is both the input and the output
    """
    db_id = "asana"
    schemas, db_names, tables = get_schemas_from_json(table_json_filename)
    schema = schemas[db_id]
    table = tables[db_id]
    schema = Schema(schema, table)

    file = open(dev_json_filename)
    data = json.load(file)
    # sql_label = get_sql(schema, sql)
    tokenizer = AutoTokenizer.from_pretrained("t5-base", use_fast=True)
    for d in data:
        d['sql'] = get_sql(schema, d['query'])
        question_toks = []
        question_encodings = tokenizer.encode(d['question'])
        for quest in question_encodings:
            if quest == 1:
                continue
            question_toks.append(tokenizer.decode(quest))
        d['question_toks'] = question_toks


    # Serializing json
    json_object = json.dumps(data)

    # Writing to sample.json
    with open(dev_out_name, "w") as outfile:
        outfile.write(json_object)

if __name__ == "__main__":
    """
    Given CSV file with questions and ground truth and a sql.schema file, prepare all the files needed
    for running spider
    """
    data_folder = '/home/paperspace/data/asana/'
    asana_small_scheme = 'small_schema.sql'
    create_sqlite(data_folder=data_folder, schema_name=asana_small_scheme)
    create_tables_json(os.path.join(data_folder, 'asana.sqlite'),
                       '/home/paperspace/deployment/rasat/dataset_files/ori_dataset/asana/tables_new.json')
    process_ground_truth_excel('/home/paperspace/data/asana/rasat_csv.csv', '/home/paperspace/data/asana/dev_out.json')

    prepare_dev_json('/home/paperspace/data/asana/dev_out.json',
                     '/home/paperspace/deployment/rasat/dataset_files/ori_dataset/asana/tables_new.json',
                     '/home/paperspace/data/asana/dev.json')
