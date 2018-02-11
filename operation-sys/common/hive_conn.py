#!/usr/bin/env python
# -*- coding: utf-8 -*-
# hive util with hive server2
"""
@author:liangqian
@create:2017-06-17
"""
import pyhs2
import pyhs2.error
import logging.config
import sys

#在centos环境中，告诉当前脚本，加载指定目录的python文件到环境中
sys.path.append('../common')
from base import *

logging.config.fileConfig(LOG_CONF_PATH)
log = logging.getLogger("operation")

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)


class HiveClient:
    def __init__(self, db_host=HIVE_DB_HOST, user=HIVE_USER, password=HIVE_PWD, database=HIVE_DB, port=HIVE_PORT,
                 authMechanism=HIVE_AUTH_MECHANISM):
        """
        create connection to hive server2
        """
        self.conn = pyhs2.connect(host=db_host,
                                  port=port,
                                  authMechanism=authMechanism,
                                  user=user,
                                  password=password,
                                  database=database,
                                  )

    def query(self, sql):
        """
        query
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetch()

    def execute(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor

    def close(self):
        """
        close connection
        """
        self.conn.close()

def main():
    try:
        hive_client = HiveClient()
        print hive_client.query("select * from action_log limit 10")

        #test code
    except pyhs2.error, tx:
        print '%s' % (tx.message)
        sys.exit(1)


if __name__ == '__main__':
    main()