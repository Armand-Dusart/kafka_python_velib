# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 19:34:04 2021

@author: arman
"""
import argparse
import shlex, subprocess
from time import sleep

from tools import *

from faker import Faker
import json
import sqlite3
import pandas as pd

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

class init_servers:
    def __init__(self,PATH_SERVER,PATH_POWERSHELL):
        self.path_powershell = PATH_POWERSHELL
        self.path_file_server = PATH_SERVER
        with open(PATH_SERVER+"/config/zookeeper.properties","r+") as file :
            temps = file.read()
            file.close()
        with open(PATH_SERVER+"/config/zookeeper.properties","w") as file :
            current_var = [f for f in temps.split("\n") if "dataDir" in f][0]
            file.write(temps.replace(current_var,"dataDir="+PATH_SERVER+"\zookeeper"))
            file.close()
        with open(PATH_SERVER+"/config/server.properties","r+") as file :
            temps = file.read()
            file.close()

        with open(PATH_SERVER+"/config/server.properties","w") as file :
            current_var = [f for f in temps.split("\n") if "log.dirs" in f][0]
            file.write(temps.replace(current_var,"log.dirs="+PATH_SERVER+"\kafka-logs"))
            file.close() 
        
        
        
    def run_zookeeper(self):
        command_line = self.path_file_server+"/bin/windows/zookeeper-server-start.bat "+self.path_file_server+"/config/zookeeper.properties"
        args = shlex.split(command_line)     
        try :  
            self.zookeeper = subprocess.Popen(["start",self.path_powershell]+args,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True)
            sleep(10)
            print("success zookeeper")
        except ValueError :
            print("Wrong argument(s)")
            raise SystemExit()
        except OSError:
            print("Powershell inexistante")
            raise SystemExit()
        except :
            print("Failed to build zookeeper's server")
            raise SystemExit()

            

    def run_kafka(self):
        command_line = self.path_file_server+"/bin/windows/kafka-server-start.bat "+self.path_file_server+"/config/server.properties"
        args = shlex.split(command_line)

        try :  
            self.kafka = subprocess.Popen(["start",self.path_powershell]+args,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True)
            sleep(20)
            self.admin_clients = KafkaAdminClient(bootstrap_servers="localhost:9092")
            self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            self.consumer = KafkaConsumer(
            "personne",
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            print("Customer Admin and producer Create")
        except ValueError :
            print("Wrong argument(s)")
            raise SystemExit()
        except OSError:
            print("Powershell inexistante")
            raise SystemExit()
        except :
            print("Failed to build kafka's server")
            raise SystemExit()
            
           
    def show_data_from_topic(self,topic) :
        command_line = self.path_file_server+"/bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic "+topic+" --from-beginning"
        args = shlex.split(command_line)
        
        try :  
            self.req = subprocess.Popen(["start",self.path_powershell]+args,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True)
            print("Return Data Success from topic :", topic)
        except :
            print("return failed")
            
    def create_topics(self,topic_name,num_partitions=1,replication_factor=1):
        try :
            topics = [NewTopic(name=topic, num_partitions= num_partitions,replication_factor=replication_factor) for topic in topic_name]
            self.admin_clients.create_topics(new_topics=topics, validate_only=False)
            print("Topic(s) : ",[t for t in topic_name] , ' Created')
        except :
            print("Failed Topic Create")
            
    def delete_topics(self,topic_name):
        try :
            self.admin_clients.delete_topics(topic_name)
            print("Topic(s) : ",[t for t in topic_name], " Deleted")
        except :
            print("Failed Topic Delete")
            
    def fake_data(self):
        fk = Faker(['fr_FR'])
        data = {
            "Nom" : fk.last_name(),
            "Prenom" : fk.first_name(),
            "Pays" : fk.country(),
            "Sexe" : fk.random_element(elements=("homme","femme")),
            "Age" :fk.random_element(elements=range(0,100))
                }
        return json.dumps(data).encode('utf-8')

    
    def send_data(self,number,topic,s=False):
        try :
            bar = progress_bar(number)
            for i in range(0,number):
                data = self.fake_data()
                data_loads = json.loads(data.decode('utf-8'))
                self.c.execute('INSERT INTO personne (Nom, Prenom, Pays, Sexe, Age) VALUES (:Nom, :Prenom, :Pays, :Sexe, :Age);',data_loads)
                self.engine.commit()
                self.producer.send(topic,value=data)
                bar.update(i)
                #self.producer.flush(30)
                if s == True :
                    sleep(1)    
            print("Data sent")
        except :
            print("send data failed")
            
    def connect_database_with_table(self,database_name):
        self.database_name = database_name
        self.engine = sqlite3.connect(self.database_name)
        try :
            self.c = self.engine.cursor()
            table_personne='''
            CREATE TABLE personne(
                [Nom] text, 
                [Prenom] text,
                [Pays] text,
                [Sexe] text,
                [Age] integer
                         )'''
            sleep(3)
            self.c.execute(table_personne) 
        except :
            print("Already create ?")
        print("DB create")
        
 
    def return_data(self):
        df = pd.read_sql_query("SELECT * FROM personne", self.engine)
        return df
           

        
        
   
        
   
    
        