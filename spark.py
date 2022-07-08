# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 10:44:48 2021

@author: arman
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as pysqlf
from pyspark.sql import types as pysqlt
import json




spark = SparkSession.builder.appName("test").getOrCreate()



response = requests.get('https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json')
data = json.loads(response.text)
print(data)

