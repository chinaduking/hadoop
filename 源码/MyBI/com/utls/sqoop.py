# _*_ coding:UTF-8 _*_
'''
Created on 2016年11月21日

@author: duking
'''

import commands
from pro_env import SQOOP_PATH

class SqoopUtil(object):
    '''
    sqoop operation
    '''
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(shell,sqoop_path = SQOOP_PATH) :
        
        #将传入的shell命令执行
        status, output = commands.getstatusoutput(SQOOP_PATH + shell)
        if status != 0:
            return None
        else:
            print "success"
            output = str(output).split("\n")
            
        return output
            