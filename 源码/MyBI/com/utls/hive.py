# _*_ coding:UTF-8 _*_
'''
Created on 2016年11月23日

@author: duking
'''
from com.utls.pro_env import HIVE_PATH
import commands

class HiveUtil(object):
    
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(hql):
        
        #将hql语句进行字符串转意
        hql = hql.replace("\"", "'")
        
        print HIVE_PATH + "hive -S -e \"" + hql + "\""
        #执行查询，并取得执行的状态和输出
        status ,output = commands.getstatusoutput(HIVE_PATH + "hive -S -e \"" + hql + "\"")
        
        if status != 0:
            return None
        else:
            print "success"
            
        output = str(output).split("\n")
        
        return output
        