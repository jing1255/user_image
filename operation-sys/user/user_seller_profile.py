# -*- coding: utf-8 -*-
# Created by liangqian@xxx.com on 2017/6/21.
#
# user seller profile
# usage:
#       python user_seller_profile.py
#       or
#       python user_seller_profile.py 2017-03-27

import pyhs2
import pyhs2.error
import datetime
import logging.config
import sys

#引入目录下的所有python文件到当前脚本环境里
sys.path.append('../common')

from base import *
from hive_conn import HiveClient
import db_util as cm

logging.config.fileConfig(LOG_CONF_PATH)
log = logging.getLogger("operation")


def main():
    try:
        # connDatabase(k1=v1,k2=v2)
        # connDatabase(**{"k1":v1,"k2":v2})
        # cur, conn = cm.conn(**cm.DBInfo("dev_operation").get_conf())
        db_info = cm.DBInfo("dev_operation")
        print db_info.get_conf()


        cur,conn = cm.getConnection(
            hostIp=db_info.host,
            username=db_info.username,
            password=db_info.password,
            dbName=db_info.db,
            connPort=db_info.port,
            connCharset=db_info.charset
        )

        hive_client = HiveClient()


        date = ""
        # 如果无参数则以今天时间推算昨天时间进行统计
        if (len(sys.argv) <= 1):
            date = datetime.date.today() - datetime.timedelta(days=1)
        else:
            # 否则解析参数出指定的统计时间
            date = str(datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d'))[0:10]


        hql = """
            SELECT
                SUM(u.order_total),
                SUM(u.goods_pv),
                SUM(u.register_total),
                '{date}' dt
            FROM
            (
                SELECT
                    count(1) order_total,
                    0 goods_pv,
                    0 register_total
                FROM
                    action_log
                WHERE
                    dt = "{date}" AND event_id = 1
                UNION ALL
                SELECT
                    0 order_total,
                    count(1) goods_pv,
                    0 register_total
                FROM
                    action_log
                WHERE
                    dt = "{date}" AND event_id = 2
                UNION ALL
                SELECT
                    0 order_total,
                    0 goods_pv,
                    count(1) register_total
                FROM
                    action_log
                WHERE
                    dt = "{date}" AND event_id = 3
            ) u
        """.format(
            date=date
        )

        log.info(hql)
        rs = hive_client.query(hql)
        log.info(rs)

        # 结果入库
        for i in rs[0]:
            try:
                insert_sql = """
                            INSERT INTO user_seller_profile
                            (
                              order_total,
                              goods_pv,
                              register_total,
                              cycle,
                              dt
                            )
                            values
                            (
                              {values}
                            );
                          """.format(
                    values=
                    str(i[0]) +
                    str(i[1]) +
                    str(i[2]) +
                    str(0) + "," +
                    "'" + str(i[3]) + "'"
                )

                log.info(insert_sql)

                cur.execute(insert_sql)
                conn.commit()
                log.info("finished !")
            except Exception as e:
                conn.rollback()
                log.error("unable to insert data @ scheduleSummary")
                log.error(e)

    except pyhs2.error,e:
        conn.rollback()
        log.error("unable to insert data @ scheduleSummary")
        log.error(e)

if __name__ == '__main__':
    main()