# read data from file (input file -  subscriber_subset.csv)
# filter data based on status ( 4& 3) with current as (1 & 0)  
# then sort result filter data based on date timestamp param
# keep the latest record per customer and ignore rest
# translate the status and current into meaningful format
# 
import pandas as pd
import random
import os
import re
import sys
import timeit
import string
import time
from datetime import datetime
from time import time

df = pd.read_csv('subscriber_subset.csv');
#print (df)

df2 = df[
    (
      (
        (df['status'] ==4)
        &
        (df['current'] ==0)
      )
     |
      ( 
        (df['status'] == 3) 
       &
       (df['current'] ==1)
     )
    )
   ].sort_values(by=['date_time'])
#print (df2)

df2['date_time'] = pd.to_datetime(df2['date_time'])
df2 = (df2[df2['endpoint_id'].ne(0)]
         .sort_values(['endpoint_id','date_time'])
         .drop_duplicates('endpoint_id', keep='last'))


df2.insert(5,"Churn_status","",allow_duplicates=True)
df2.loc[df2['status'] == 3, 'Churn_status'] = 'Churned'
df2.loc[df2['status'] == 4, 'Churn_status'] = 'Not Churned'

print(df2)

df2.to_csv('ChurnOutput.csv')