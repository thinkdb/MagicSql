#!/bin/env python
# _*_ coding:utf8 _*_
import MySQLdb
import logging
import re
import ConfigParser
import time
import binlog


# 获取配置文件选项
def get_config(group, config_name):
    config = ConfigParser.ConfigParser()
    config.read("../etc/db_info")
    config_values = config.get(group, config_name).strip("\'")
    return config_values


# 日志记录模块
def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("../logs/PYbinlog.log")
    fh.setLevel(logging.INFO)
    datefrm = "%y-%m-%d %H:%M:%S"
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefrm)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


# 获取数据表结构模块
def mysql_conn(host, user, passwd, port, database_name, table_name):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, port=int(port), connect_timeout=5)
        cur = conn.cursor()
        cur.execute("show create table %s.%s;" %(database_name, table_name) )
        data = cur.fetchall()
        if data:
            return data
        else:
            return 0
    except Exception, e:
        print e
        binlog.logger.info(e)
        return 0


# 这边主要针对 update 操作的恢复，修改where 里面null 为 = null, set 里面的null 为 is null
def filter_file(binlog_file_name):
    sub_after_line_list = []
    flag = 0
    for item in binlog_file_name:
        if "WHERE" in item:
            flag = 1
        if "SET" in item:
            flag = 0
        sub_line = re.sub("###", "", item)
        if sub_line and flag == 1:    # 这边是修改 where 后面的数据
            if "TIMESTAMP" in sub_line:
                # 修改timstamp类型的数据为datetime类型的数据， 不然入库后的数据是一串时间戳
                sub_after_line = sub_line.split("/*")[0].strip().split("=")[1]
                data_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_after_line)))
                a = sub_line.split("/*")[0].strip().split("=")[0] + "=\'" + data_timestamp + "\'"
                sub_after_line_list.append(a)
            else:
                sub_after_line = sub_line.split("/*")[0].strip()
                if "NULL" in sub_after_line:
                    sub_after_line_list.append(sub_after_line[:-5] + " = null")
                else:
                    sub_after_line_list.append(sub_after_line)
        else:
            flag = 0
            if sub_line:  # 这边是修改 set 后面的数据
                if "TIMESTAMP" in sub_line:
                    # 同上
                    sub_after_line = sub_line.split("/*")[0].strip().split("=")[1]
                    data_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_after_line)))
                    a = sub_line.split("/*")[0].strip().split("=")[0] + "=\"" + data_timestamp + "\""
                    sub_after_line_list.append(a)
                else:
                    sub_after_line = sub_line.split("/*")[0].strip()
                    if "NULL" in sub_after_line:
                        sub_after_line_list.append(sub_after_line[:-5] + " is null")
                    else:
                        sub_after_line_list.append(sub_after_line)
    return sub_after_line_list


# 获取列名和列数
def col_count(data):
    col_num = 0
    col_list = []
    for item in data.split("\n")[1:]:
        col_re = re.search("^`", item.strip())   # 列名是包括 `` 符号，获取`开头的行
        if col_re:
            col_list.append(item.split()[0].strip("`"))   # 去除列两个 ` 符号
            col_num = col_num + 1
    col_list.append(col_num)
    return col_list


# 修改 update 操作的列的位置
def dml_update(dml_sql, col_num):
    convert_col_name = []
    where_flag = 0
    set_flag = 0
    col_num_where = col_num
    col_num_set = col_num
    col_num_set2 = col_num_set
    num = col_num
    for i in dml_sql:
        if "UPDATE" in i:
            convert_col_name.append(i)
        if i[:5] == "WHERE":
            convert_col_name.append("set")
            where_flag = 1
            continue
        if where_flag == 1 and i[:1] == "@":
            if col_num > 1:
                convert_col_name.append(i + ",")
            else:
                convert_col_name.append(i)
            col_num = col_num - 1
        else:
            where_flag = 0
            col_num = col_num_where
        if i[:3] == "SET":
            convert_col_name.append("where")
            set_flag = 1
            continue
        if set_flag == 1 and i[:1] == "@":
            if col_num_set > 1:
                convert_col_name.append(i + " and ")
            else:
                convert_col_name.append(i + ";")
                col_num = num
            col_num_set = col_num_set -1
        else:
            set_flag = 0
            col_num_set = col_num_set2
    return convert_col_name


# 修改 insert 语句为 delete 语句
def dml_insert(dml_sql, col_num):
    convert_col_name = []
    num = col_num
    for line in dml_sql:
        if "INSERT INTO" in line:
            sub_delete = re.sub("^INSERT INTO", "delete from", line)   # 替换 insert 为 delete
            convert_col_name.append(sub_delete)
        elif "SET" in line:                           # 替换 set 为 where
            convert_col_name.append("where")
        else:
            if col_num > 1:
                convert_col_name.append(line + "\nand ")  # 判断是否到最后一行，没有则在行的最后添加一个换行符和 and
                col_num = col_num - 1
            else:
                convert_col_name.append(line + "; ")  # 到最后一行， 在行最后添加 分号
                col_num = num
    return convert_col_name


# 修改 delete 操作为 insert 操作
def dml_delete(dml_sql, col_num):
    convert_col_name = []
    num = col_num
    for line in dml_sql:
        if "DELETE FROM" in line:
            sub_delete = re.sub("^DELETE FROM", "insert into", line)   # 替换 delete 为 insert
            convert_col_name.append(sub_delete)
        elif "WHERE" in line:                          # 替换 where 为 set
            convert_col_name.append("set")
        else:
            if col_num > 1:
                convert_col_name.append(line + ",")    # 通过计数器， 判断是否到最后一行，没有到，在行最后加上逗号
                col_num = col_num - 1
            else:
                convert_col_name.append(line + ";")    # 如果到了最后一行，就在行最后添加一个分号
                col_num = num
    return convert_col_name


# 过滤要恢复表的对应操作
def py_grep(binlog_file, table_name):
    flag = 0
    binlog_list = []
    with open(binlog_file) as b_file:
        for line in b_file:
            if line[:3] == "###" and table_name in line:  # 判断行是否以 ### 开头， 并且表名也在同一行里面
                flag = 1                                   # 设置一个标识
                binlog_list.append(line)
                continue
            if flag == 1 and line[:3] == "###":    # 标识 = 1， 并且以 ### 开头的行，说明这些行是同一个恢复表的里面
                binlog_list.append(line)            # 把行添加到列表里面
            else:
                flag = 0     # 否则标识就为0, 不会把对应的行记录到列表中，
    return binlog_list


# 把最后的结果写入到文件
def file_dump(item):
    bin_file = open("../tmp/binlog_tmp_file.sql", "w")
    for line in item:
        bin_file.write(line + "\n")
    bin_file.close()


# 转换 @+number 标识为具体的列名
def convert_col_name_list(data, col_list):
    num = len(col_list)
    sql_file = []
    id = 0
    for item in data:
        if id <= num:
            if re.search("^@", item):        # 过滤以 @ 开头的行
                a = re.sub("^@[\d]+", col_list[id], item)      # 把@值改成对应的列名
                if a:
                    id = id + 1
                    sql_file.append(a)
                else:
                    id = 0
                    sql_file.append(item)
            else:
                id = 0
                sql_file.append(item)

    return sql_file