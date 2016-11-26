# _*_ coding:UTF-8 _*_
'''
Created on 2016年11月21日

@author: duking
'''

import xml.etree.ElementTree as ET
from com.utls.pro_env import PROJECT_CONF_DIR
from com.utls.sqoop import SqoopUtil
import cmd
import collections
import datetime

def iresolve_conf(dt):
    
    #获得配置文件名
    conf_file = PROJECT_CONF_DIR + "Import.xml"
    #解析配置文件
    xml_tree = ET.parse(conf_file)
    
    #提取出本次导入的类型  全导入或者增量导入  通过配置import.xml中的plan标签的value值设定
    plan_types = xml_tree.findall('./plan')
    for plan_type in plan_types:
        aim_types = plan_type.findall('./value')
        for i in range(len(aim_types)):
            aim_type = aim_types[i].text
            
    #获得task元素
    tasks = xml_tree.findall('./task')
    
    #用来保存待执行的sqoop命令的集合
    cmds = []
    
    for task in tasks:
        #获得导入类型，增量导入或者全量导入
        import_type = task.attrib["type"]
        
        if(import_type != aim_type):
            continue
        
        #获得表名集合
        tables = task.findall('./table')
             
        #迭代表名集合，解析表配置文件
        for i in range(len(tables)):
            #表名
            table_name = tables[i].text
            #表配置文件名
            table_conf_file = PROJECT_CONF_DIR + table_name + ".xml"
            
            #解析表配置文件
            xmlTree = ET.parse(table_conf_file)
            
            #获取sqoop-shell 节点
            sqoopNodes = xmlTree.findall("./sqoop-shell")
            #获取sqoop 命令类型
            sqoop_cmd_type = sqoopNodes[0].attrib["type"]
            
            #首先组装成sqoop命令头
            command = "sqoop " + sqoop_cmd_type
                
            #获取
            praNodes = sqoopNodes[0].findall("./param")
            
            #用来保存param的信息的有序字典
            cmap = collections.OrderedDict()            
            #将所有param中的key-value存入字典中
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
                        
                #合成前一天的时间
                if(key == "last-value"):
                    value = '"' + str(dt) + " " + value + '"'
                        
                #拼装为命令
                command += " --" + key + " " + value + " " 
                    
            #将命令加入至待执行的命令集合
            cmds.append(command)
            
    return cmds



#获取昨天时间
def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday


#python 模块的入口：main函数
if __name__ == '__main__':
     
    dt = getYesterday()
    #解析配置文件，获得sqoop命令集合
    cmds = iresolve_conf(dt)
    
    #迭代集合，执行命令
    for i in range(len(cmds)):
        cmd = cmds[i]
        print cmd
        #执行导入过秤
        SqoopUtil.execute_shell(cmd)
                    