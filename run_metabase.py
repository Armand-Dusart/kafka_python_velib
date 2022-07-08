# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 15:26:32 2021

@author: arman
"""

import argparse
import shlex, subprocess
from time import sleep

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager



def run_metabase(path_powershell,path_metabase):
    command_line = "java -jar "+path_metabase+"/metabase.jar"
    args = shlex.split(command_line)     
    try :  
        metabase = subprocess.Popen(["start",path_powershell,]+args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True)
        sleep(50)
    except :
        print("Failed metabase")
        
    print("create driver chrome")
    driver= webdriver.Chrome(ChromeDriverManager().install())
    driver.get("http://localhost:3000/")