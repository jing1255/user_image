# -*- coding:utf-8 -*-
# mysql client
"""
@author:liangqian
@create:2017-06-17
"""
import os
import configparser as cg
import logging
import sys
import time
import MySQLdb as mydb

#数据库配置加载类
class DBInfo:
    def __init__(self,config_name):
        config = cg.ConfigParser()
        path = os.path.split(os.path.realpath(__file__))[0] + '/../conf/db.conf'
        config.read(path)
        self.host = str(config.get(config_name, "dbhost"))
        self.username = str(config.get(config_name, "dbuser"))
        self.password = str(config.get(config_name, "dbpassword"))
        self.db = str(config.get(config_name, "dbname"))
        self.port = int(config.get(config_name, "dbport"))
        self.charset = str(config.get(config_name, "dbcharset"))

    def get_conf(self):
        return {
            "host":self.host,
            "username":self.username,
            "password":self.password,
            "db":self.db,
            "port":self.port,
            "charset":self.charset
        }

CONN_COUNT = 0

# connect database
def getConnection(hostIp='', username='', password='', dbName='', connPort=3306,
                  connCharset='utf8mb4'):
    global CONN_COUNT
    try:
        CONN_COUNT += 1
        conn = mydb.connect(host=hostIp, user=username, passwd=password, db=dbName, port=connPort,
                                   charset=connCharset)
        # conn.ping(True)
        cur = conn.cursor()
    except Exception as e:
        logging.error('connect database failed.')
        logging.error(e)
        # 如果连接出现问题 等待3秒后重连
        time.sleep(3)
        # 如果重连次数小于5 则重新连接，否则退出程序
        if CONN_COUNT < 5:
            cur,conn = getConnection(hostIp, username, password, dbName, connPort, connCharset)
        else:
            cur = ''
            conn = ''
            logging.error("connected db %d times, but failed." % CONN_COUNT)
            sys.exit(2)
    finally:
        CONN_COUNT = 0
        return cur,conn


# close database
def close(cur, conn):
    try:
        cur.close()
        conn.close()
    except Exception as e:
        logging.info('close database failed.')

if __name__ == '__main__':
    print('---Done---')