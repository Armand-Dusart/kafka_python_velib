# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 19:36:01 2021

@author: arman
"""


from build_kafka import *
from run_metabase import *
import json



if __name__ == "__main__" :
    user = "arman"
    PATH_powershell = r"C:\Users/"+user+"\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Anaconda3 (64-bit)\Anaconda Powershell Prompt (anaconda3)"
    
    PATH_SERVER = r"C:/data_streaming/kafka"  # FAUT METTRE CES SLASH : "/" et un chemin pas trop long !!!!!
    servers = init_servers(PATH_SERVER,PATH_powershell)
    servers.run_zookeeper()
    servers.run_kafka()
    # servers.create_topics(["personne"])
    servers.connect_database_with_table("personne.db")
    servers.show_data_from_topic('personne')
    servers.send_data(10000,"personne",True)
    df = servers.return_data()
    PATH_metabase = r"C:/data_streaming"
    run_metabase(PATH_powershell,PATH_metabase)


