#!/usr/bin/env python
# -*- encoding: utf8 -*-

import sys
import time

from time import strftime, localtime
from datetime import timedelta, date
import datetime
import calendar

from sailog  import *





def sai_get_today():
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    dt = time.strftime("%Y-%m-%d", time.localtime()) 
    return str(dt)

def sai_get_time():
    tm = time.strftime("%H:%M:%S", time.localtime()) 
    return str(tm)

def sai_get_date_time():
    tm = time.strftime("%Y%m%d-%H%M%S", time.localtime()) 
    return str(tm)

def sai_get_micro_second():
    tm = time.time() * 1000000
    return tm 


'''
if n>=0,date is larger than today
if n<0,date is less than today
date format = "YYYY-MM-DD"
'''
def sai_get_date_by(n=0):
    if (n<0):
        n = abs(n)
        dt = date.today()-timedelta(days=n)
    else:
        dt = date.today()+timedelta(days=n)

    return str(dt)


# get list
def sai_get_args():
    sys.argv.pop(0)
    args = sys.argv
    return args



if __name__=="__main__":
    sailog_set("saiutil.log")
    print sai_get_today()  
    print sai_get_date_by(2)
    print sai_get_date_by(-3)
    print sai_get_date_time()


# saiutil.py
