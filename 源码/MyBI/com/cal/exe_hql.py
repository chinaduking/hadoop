# _*_ coding:UTF-8 _*_
'''
Created on 2016年11月23日

@author: duking
'''
from com.utls.pro_env import PROJECT_CONF_DIR
import xml.etree.ElementTree as ET
import datetime
from com.utls.hive import HiveUtil

#解析配置文件
def hresolve_conf(inputtype,dt):
    
    #获得配置的文件名
    conf_File = PROJECT_CONF_DIR + "HiveJob.xml"
    
    #解析配置文件
    xmlTree = ET.parse(conf_File)
    
    #job元素
    jobs = xmlTree.findall('./Job')
    
    hqls = []
    
    #遍历job的子元素，获得所需要的参数
    for job in jobs:

        if job.attrib["type"] == inputtype:
            for hql in job.getchildren():
                #获得hql标签，去掉两头空格
                hql = hql.text.strip()
                
                #检查hql是否有效，无效则抛出异常
                if len(hql) == 0 or hql == "" or hql == None:
                    raise Exception('参数有误，终止运行')
                else:
                    #将时间信息替换为输入值
                    hql = hql.replace('\$dt',str(dt))
                    hqls.append(hql)
    
    return hqls

#获取昨天时间
def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday



if __name__ == '__main__':
    
    dt = getYesterday()
    #使用调度模块传入的两个参数，第一个为可执ype,第二个日期
    hqls = hresolve_conf("elt_db", dt)

    for hql in hqls:
        HiveUtil.execute_shell(hql)