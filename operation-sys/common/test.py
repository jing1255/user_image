# -*- coding:utf-8 -*-
# 测试
# import os
# import subprocess
# import sys
import commands

# reload(sys)
# sys.setdefaultencoding('utf-8')
# print os.path.split(os.path.realpath(__file__))[0]
# p = subprocess.Popen("dir", shell=True)
(status, output) = commands.getstatusoutput('pwd')
print status
print output
# s = str(p.stdout.readline(),encoding='gbk')
# print p
# print status
# print output
