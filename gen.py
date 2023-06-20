import datetime
import os
import random
import re
import shutil
import string
import sys
import uuid
from pprint import pprint


class GenSQL:
    @classmethod
    def gen_sqls(cls,db_structure:dict,generate_num:int):
        sqls = ""
        for table_name,table_structure in db_structure.items():
            for _ in range(generate_num):
                sqls += cls.gen_dummy_data_sql(table_name,table_structure)
        return sqls

    def gen_insert_sql(table_name:str,columns:dict):
        return f"INSERT INTO {table_name} ({', '.join(columns.keys())}) VALUES ({', '.join(columns.values())});\n"

    def gen_dummy_data_sql(table_name:str,columns:dict):
        insert_data = {}
        for column_name,column_detail in columns.items():
            if column_detail.get("references") is not None:
                with open(f"./tmp/{column_detail['references']['table']}_{column_detail['references']['column']}.txt") as f:
                    dummy_data = random.sample(f.read().split(",")[:-1],1)[0]
            else:
                dummy_data = GenDummyData.gen_dummy_data(column_detail["type"])
            if column_detail.get("save") is not None:
                with open(f"./tmp/{table_name}_{column_name}.txt","a+")as f:
                    f.write(f"{dummy_data},")
            insert_data[column_name] = dummy_data
        return GenSQL.gen_insert_sql(table_name,insert_data)


class GenDummyData:
    def random_name(n):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=n))
    def random_date():
        now = datetime.datetime.now()
        random_time = now
        while random_time > now:
            random_time =now.replace(hour=random.randint(0,23), minute=random.randint(0,59),second=random.randint(0,60))
        return random_time - datetime.timedelta(days=random.randint(0,365*20))
    @classmethod
    def gen_dummy_data(cls,column_type:str):
        if "varchar" in column_type :
            return f"""'{cls.random_name(20 if column_type.replace("varchar","")=="" else int(column_type.replace("varchar","")))}'"""
        elif column_type == "bool":
            return f"'{random.sample([True,False],1)[0]}'"
        elif column_type == "uuid":
            return f"'{uuid.uuid4()}'"
        elif column_type == "timestamp":
            return f"'{GenDummyData.random_date()}'"
        elif column_type == "int":
                return f"'{random.randint(0,100)}'"

class Shape:
    @classmethod
    def get_table_name(cls,sql:string):
        return sql.split("CREATE TABLE")[1].split("(")[0].replace(" ","")

    @classmethod
    def get_columns(cls,sql:string):
        return sql.split(f"CREATE TABLE {cls.get_table_name(sql)}")[1].replace("(","").replace(")","").split(",")

    @classmethod
    def shape_raw_sql(cls,raw_sql:str):
        return raw_sql.replace('"','').replace("\n","")

def generate_dummy_sql(file_path:str,generate_num:int):
    db_structure = load_sql(file_path)
    generated_sql = GenSQL.gen_sqls(db_structure,generate_num)
    with open("./generated.sql","w") as f:
        f.write(generated_sql)

def load_sql(file_path:str):
    with open(file_path,"r")as f:
        raw_sql_commands = f.read()
    sql_commands = (Shape.shape_raw_sql(raw_sql_commands)).split(";")[:-1]
    columns_structure = {}
    for sql_command in sql_commands:
        if sql_command.startswith("CREATE"):
            table_structure = {}
            table_name = Shape.get_table_name(sql_command)
            columns = Shape.get_columns(sql_command)
            for column in columns:
                column = column.lstrip(" ").split(" ")
                column_name = column[0]
                column_type = "timestamp" if column_name.endswith("_at") else column[1]
                column_options = column[2:]
                table_structure[column_name] = {"type":column_type,"options":column_options}
            columns_structure[table_name] = table_structure
        elif sql_command.startswith("ALTER"):
            pattern = r'[a-z_]+'
            [table_name,foreign_key,reference_table,reference_column] = re.findall(pattern, sql_command)
            columns_structure[table_name][foreign_key]["references"] = {"table":reference_table,"column":reference_column}
            columns_structure[reference_table][reference_column]["save"] = True
    return columns_structure

if __name__ == '__main__':
    file_path  =sys.argv[1]
    generate_num  =int(sys.argv[2])
    if file_path != "":
        os.makedirs("./tmp", exist_ok=True)
        generate_dummy_sql(file_path,generate_num)
        shutil.rmtree("./tmp",)