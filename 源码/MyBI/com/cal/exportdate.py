# _*_ coding:UTF-8 _*_
'''
Created on 2016年11月23日

@author: duking
'''
from com.utls.pro_env import PROJECT_CONF_DIR
from com.utls.sqoop import SqoopUtil
import xml.etree.ElementTree as ET
import collections
import cmd

def eresolve_conf():
    
    #获得配置文件名
    conf_file = PROJECT_CONF_DIR + "Export.xml"
    #解析配置文件
    xml_tree = ET.parse(conf_file)
    
    tasks = xml_tree.findall('./task')
    
    cmds = []
    
    for task in tasks:
        
        #获得表名集合
        tables = task.findall('./table')
        
        for i in range(len(tables)):
            #表名
            table_name = tables[i].text
            #表配置文件名
            table_conf_file = PROJECT_CONF_DIR + table_name + ".xml"
            
            xmlTree = ET.parse(table_conf_file)
            
            sqoopNodes = xmlTree.findall("./sqoop-shell")
            
            #获取sqoop 命令类型
            sqoop_cmd_type = sqoopNodes[0].attrib["type"]
            
            #首先组装成sqoop命令头
            command = "sqoop " + sqoop_cmd_type
            
            #获取
            praNodes = sqoopNodes[0].findall("./param")
            
            #用来保存param的信息的有序字典
            cmap = collections.OrderedDict()
            
            for i in range(len(praNodes)):
                #获取key的属性值
                key = praNodes[i].attrib["key"]
                #获取param标签中的值
                value = praNodes[i].text
                #保存到字典中
                cmap[key] = value
                
            #迭代字典将param的信息拼装成字符串
            for key in cmap:
                  
                value = cmap[key]
                 
                #如果不是键值对形式的命令 或者值为空，跳出此次循环
                if(value == None or value == "" or value == " "):
                    value = ""    
                #拼装为命令
                command += " --" + key + " " + value + " " 
                    
            #将命令加入至待执行的命令集合
            cmds.append(command)
            
    return cmds

#python 模块的入口：main函数
if __name__ == '__main__':
     
    #解析配置文件，获得sqoop命令集合
    cmds = eresolve_conf()
    
    #迭代集合，执行命令
    for i in range(len(cmds)):
        cmd = cmds[i]
        print cmd
        #执行导入过秤
        SqoopUtil.execute_shell(cmd)
            