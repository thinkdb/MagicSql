#!/bin/env python
# _*_ coding:utf8 _*_
import func


# 从配置文件中获取必要的信息
host = func.get_config('source_database', 'host')
user = func.get_config('source_database', 'user')
passwd = func.get_config('source_database', 'passwd')
port = func.get_config('source_database', 'port')
database_name = func.get_config('source_database', 'database_name')
table_name = func.get_config('source_database', 'table_name')
dml_type = func.get_config('source_database', 'dml_type').upper()
file_path = func.get_config('source_database', 'binlog_file')
logger = func.logger()


def main():
    logger.info("Start convert dml operation to recover......")
    logger.info("Get table structure from database......")
    data = func.mysql_conn(host, user, passwd, port, database_name, table_name)
    # 从数据库获取表结构信息
    if data:
        col_count_result = func.col_count(data[0][1])
        # 获取具体的列名及列数
        if col_count_result:
            binlog_list = func.py_grep(file_path, table_name)
            # 从binlog文件中过滤出需要恢复表的相关操作
            logger.info("Get the %s's all dml operation......" % table_name)
            if binlog_list:
                dml_sql = func.filter_file(binlog_list)
                # 做一次简单的转换， 把注释去掉， 并修改null值 和 timestamp 类型的数据
                if dml_sql:
                    col_list = col_count_result[:-1]
                    col_num = col_count_result[-1]

    # 转换成sql文件， 并写入文件
    if dml_type == "UPDATE":
        note_sql = func.dml_update(dml_sql, col_num)
        if note_sql:
            file_dump = func.convert_col_name_list(note_sql, col_list)
            logger.info("Convert to update sql file......")
            if file_dump:
                func.file_dump(file_dump)

    elif dml_type == "INSERT":
        note_sql = func.dml_insert(dml_sql, col_num)
        if note_sql:
            file_dump = func.convert_col_name_list(note_sql, col_list)
            logger.info("Convert to delete sql file......")
            if file_dump:
                func.file_dump(file_dump)
    elif dml_type == "DELETE":
        note_sql = func.dml_delete(dml_sql, col_num)
        if note_sql:
            file_dump = func.convert_col_name_list(note_sql, col_list)
            logger.info("Convert to insert sql file......")
            if file_dump:
                func.file_dump(file_dump)
    else:
        print "Error. dml type is error....."

