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
    fkeys = {}
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

        fkeys[table] = []
        cursor.execute("PRAGMA foreign_key_list(" + table + ");")
        for fk in cursor.fetchall():
            fkeys[table].append([fk[2], fk[3], fk[4]])

    return schema, fkeys

def create_sqlite(data_folder, schema_name, db_name):
    # Remove the DB file if it exists
    if os.path.exists(os.path.join(data_folder, db_name)):
        os.remove(os.path.join(data_folder, db_name))
    con = sqlite3.connect(os.path.join(data_folder, db_name))
    cur = con.cursor()
    with open(os.path.join(data_folder, schema_name), 'r') as sql_file:
        result_iterator = cur.executescript(sql_file.read())
    con.commit()

def create_tables_json(schema_name, output_name):
    """
    Parse that can take a schema and create tables.json file
    The result is a dictionary of lists
    """
    schema, fkeys = get_schema_info(schema_name)
    # part 1 - "column_names"
    column_names = []
    column_names.append([-1, '*'])
    table_names = []
    column_types = []
    column_types.append('TEXT')
    col_counter = 1
    primary_keys = []
    foreign_keys = []
    for k, name in enumerate(schema.keys()):
        table_names.append(name)
        for col in schema[name]:
            column_names.append([k, col[0]])
            column_types.append(col[1])
            if col[2] == 1:
                primary_keys.append(col_counter)
            col_counter = col_counter + 1

    # Create foreign keys
    # Check if this column belongs to one of the foreign keys
    for fk_k, fk_name in enumerate(fkeys.keys()):
        if len(fkeys[fk_name]) == 0:
            continue
        for fk_details in fkeys[fk_name]:
            pass
            # Find the number of column fk_details[1] in project fk_name with ID fk_k
            second_fk = -1
            first_fk = -1
            # Find the ID of the first table
            first_table_id = -1
            for table_k, tb_name in enumerate(table_names):
                if fk_details[0].lower() == tb_name:
                    first_table_id = table_k
            if first_table_id >= 0:
                for col_k, column in enumerate(column_names):
                    if column[0] == fk_k and column[1].lower() == fk_details[1].lower():
                        second_fk = col_k
                    if column[0] == first_table_id and column[1] == fk_details[2].lower():
                        first_fk = col_k
                foreign_keys.append([first_fk, second_fk])

    # Go over all columns and create tuples of their names
    pass
    final_dict = {'db_id': 'asana'}
    final_dict['column_types'] = column_types
    final_dict['column_names'] = column_names
    final_dict['column_names_original'] = column_names
    final_dict['primary_keys'] = primary_keys
    final_dict['table_names'] = table_names
    final_dict['table_names_original'] = table_names
    final_dict['foreign_keys'] = foreign_keys # [[5, 0], [6, 0], [26, 0]]

    # Important - All columns start from 0 and include the -1

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
    SQL.schema should be adapted for SQLite. It may include both definitions of the table and the actual data
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--in_data_folder', type=str, help='Storage path for input and temporary files',
                        default='/home/paperspace/data/train_asana/')
    parser.add_argument('--out_data_folder', type=str, help='Storage path for output files',
                        default='/home/paperspace/data/asana/')
    parser.add_argument('--schema_file', type=str, help='Asana SQLLite schema file',
                        default='schema.sql')
    parser.add_argument('--db_file', type=str, help='Asana SQLLite database name',
                        default='asana.sqlite')

    args = parser.parse_args()
    create_sqlite(data_folder=args.in_data_folder, schema_name=args.schema_file, db_name=args.db_file)

    create_tables_json(os.path.join(args.in_data_folder, args.db_file),
                       os.path.join(args.in_data_folder, 'tables_new.json'))


    data_folder = '/home/paperspace/data/asana/'
    asana_small_scheme = 'small_schema.sql'
    create_sqlite(data_folder=data_folder, schema_name=asana_small_scheme)
    create_tables_json(os.path.join(data_folder, 'asana.sqlite'),
                       '/home/paperspace/deployment/rasat/dataset_files/ori_dataset/asana/tables_new.json')
    process_ground_truth_excel('/home/paperspace/data/asana/rasat_csv.csv', '/home/paperspace/data/asana/dev_out.json')

    prepare_dev_json('/home/paperspace/data/asana/dev_out.json',
                     '/home/paperspace/deployment/rasat/dataset_files/ori_dataset/asana/tables_new.json',
                     '/home/paperspace/data/asana/dev.json')
