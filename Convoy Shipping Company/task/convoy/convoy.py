import math
from xml.etree import ElementTree

import pandas as pd
import sqlite3
import json
from xml.etree.ElementTree import Element,tostring


def scoring_func(tank_, consumption_, load_):
    first_ = 0 if 450 * consumption_ / 100 <= 230 else 1
    num_of_stops_ = math.floor(tank_ / (450 * consumption_ / 100))
    second_ = 0 if num_of_stops_ == 0 else 1 if num_of_stops_ == 1 else 2
    third_ = 0 if load_ < 20 else 2
    return first_ + second_ + third_


def load_from_db(conn_):
    query_ = "SELECT * from 'convoy'"
    df_ = pd.read_sql(query_, conn_)
    return df_


def save_to_db(df_, conn_):
    df_.a
    df_.to_sql("convoy", conn_, if_exists="append", index=False)
    conn_.commit()
    ending = " was" if len(df_updated) == 1 else "s were"
    print(f"{len(df_)} record{ending} inserted into {file_name}.s3db")


def save_to_json(df_):
    json_dict_ = {"convoy": df_.to_dict('records')}
    with open(f"{file_name}.json", 'w', encoding='utf-8') as file_:
        json.dump(json_dict_, file_, indent=4)
    ending_ = " was" if len(df_) == 1 else "s were"
    print(f'{len(df_)} vehicle{ending_} imported to {file_name}.json')


def save_to_csv(df_):
    num_of_lines_ = df_.shape[0]
    ending_ = " was" if df_.shape[0] == 1 else "s were"
    df_.to_csv(f'{file_name}.csv', index=False)
    print(f'{num_of_lines_} line{ending_} imported to {file_name}.csv')


def load_from_xlsx():
    return pd.read_excel(f'{file_name}.xlsx', sheet_name='Vehicles', dtype=str)


def load_from_csv():
    return pd.read_csv(f'{file_name}.csv')


def save_corrected_data(df_, df_updated_):
    df_updated_.to_csv(f'{file_name}[CHECKED].csv', index=False)
    difference = (df_ != df_updated_).stack()
    changed = difference[difference]
    print(f"{len(changed)} cells were corrected in {file_name}[CHECKED].csv")


def save_to_xml(df_):
    with open(f"{file_name}.xml", 'w', encoding='utf-8') as file_:
        file_.write(dict_to_xml('convoy', df_.to_dict('records')))
    ending_ = " was" if len(df_) == 1 else "s were"
    print(f'{len(df_)} vehicle{ending_} imported to {file_name}.xml')


def dict_to_xml(tag, d):

    elem = Element(tag)
    for child_ in d:
        child = Element("vehicle")
        for key, val in child_.items():
            # create an Element
            # class object
            child_elem = Element(key)
            child_elem.text = str(val)
            child.append(child_elem)
        elem.append(child)
    return ElementTree.tostring(elem, encoding='unicode', method='xml')


print("Input file name")
file_name, file_ext = input().split(".")
file_name_corr = file_name.replace("[CHECKED]", "")
connection = sqlite3.connect(f"{file_name_corr}.s3db")
cursor = connection.cursor()
create_table_query = """CREATE TABLE IF NOT EXISTS convoy (
                            vehicle_id INTEGER PRIMARY KEY,
                            engine_capacity INTEGER NOT NULL,
                            fuel_consumption INTEGER NOT NULL,
                            maximum_load INTEGER NOT NULL);"""
cursor.execute(create_table_query)
connection.commit()

if file_ext == "s3db":
    df = load_from_db(connection)
    save_to_json(df)
    save_to_xml(df)
elif file_ext == "xlsx":
    df = load_from_xlsx()
    save_to_csv(df)
    df_updated = df.replace(to_replace='\D', value='', regex=True)
    save_corrected_data(df, df_updated)
    df_updated = df_updated.astype('int32')
    save_to_db(df_updated, connection)
    save_to_json(df_updated)
    save_to_xml(df_updated)
elif file_ext == "csv":
    df = load_from_csv()
    df_updated = df.replace(to_replace='\D', value='', regex=True)
    if not file_name.endswith("[CHECKED]"):
        save_corrected_data(df, df_updated)
    file_name = file_name.replace("[CHECKED]", "")
    df_updated = df_updated.astype('int32')
    save_to_db(df_updated, connection)
    save_to_json(df_updated)
    save_to_xml(df_updated)
cursor.close()
connection.close()
