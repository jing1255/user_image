# -*- coding: utf-8 -*-
# Created by liangqian@shsxt.com on 2017/6/21.
#
# upload action log to hive
# usage:
#       python upload_action_log.py 2017-06-19

import pyhs2
import pyhs2.error
import datetime
import commands
import logging.config
import sys

sys.path.append('../common')
from base import *
from hive_conn import HiveClient

logging.config.fileConfig(LOG_CONF_PATH)
log = logging.getLogger("operation")


def main():
    try:
        hive_client = HiveClient()

        date = ""
        # 如果无参数则以今天时间推算昨天时间进行统计
        if (len(sys.argv) <= 1):
            date = datetime.date.today() - datetime.timedelta(days=1)
        # 否则解析参数出指定的统计时间
        else:
            date = str(datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d'))[0:10]

        # 指定分区
        month = str(date)[5:7]
        year = str(date)[0:4]


        # 日志路径
        log_path = "/user/dmteam/action_log/" + str(date) + "/action_log_simp.csv"

        # 日志上传hql
        hql = "LOAD DATA INPATH '{log_path}' OVERWRITE INTO TABLE  dm.action_log partition (year='{year}',month='{month}',dt='{date}')".format(
            log_path=log_path, year=year, month=month, date=date)

        # hive shell 命令
        hql_shell = HIVE_HOME + "/bin/hive" + " -e " + "\"" + hql + "\""
        log.info(hql_shell)
        log.info("upload_action_log start ...")
        (status, output) = commands.getstatusoutput(hql_shell)
        log.info(output)
        # 执行hql
        # rs = hive_client.execute(hql)
        log.info("finished !")

    except pyhs2.error, tx:
        print '%s' % (tx.message)
        sys.exit(1)


if __name__ == '__main__':
    main()


