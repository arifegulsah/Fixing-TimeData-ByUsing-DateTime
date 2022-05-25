# -*- coding: utf-8 -*-
"""
Created on Tue May 24 22:08:58 2022

@author: arife
"""

#IMPORTING LIBRARIES
import pandas as pd
import feather
import datetime as dt
from datetime import timedelta

#UPLOADING DATA WITH 2 OPTIONS DOWNHERE /(WILL USE THE VARIABLE NAMED "datapd" )
datapd = pd.read_feather('problem-2.ftr')
dataftr = feather.read_dataframe('problem-2.ftr')

#SINCE ORIGINAL DATA IS NOT A DATETIME FORMAT, IT MUST CONVERT WITH THIS FUNCTION
#THIS FUNC WILL TAKE THE ORIGINAL DATA AND CONVERT IT BY USING to_datetime FUNC FROM pandas
#ERRORS WILL BE IGNORED SINCE NOT ALL DATA (ROWS) IS IN THE SAME FORMAT
def convertToDateTıme(originalData):
    date_time = pd.to_datetime(originalData, format='%d-%m-%Y', errors='ignore')
    return date_time

datapd.Aangebodensinds = datapd.Aangebodensinds.apply(convertToDateTıme)

#JUST WANTED TO CHECK IF CONVERSION  IS SUCCESSFULL
mystr = datapd.iloc[0, 0]
mystr = datapd.iloc[2,0]
print(type(mystr))

#isinstance RETURNS US EITHER THE VARIABLE IS IN DATETIME FORMAT OR NOT (WITH BOOLEAN OUTPUTS)
#WILL USE isinstance IN ORDER TO SCAN THE ENTIRE DATASET ROW BY ROW
isinstance(mystr, dt.datetime)

#CREATE A NEW DATAFRAME FILE WITH A COLUMN NAMED "dates" FOR OUR NEW RESULTS
#df WILL BE THE NAME OF IT
columns = ['dates']
df = pd.DataFrame(columns=columns)

#WANT TO CHECK IF ALL ROWS ARE BEING HANDLED OR NOT.
countForRows = 0

#SHOULD EXTRACT NUMBERS FROM  “n (maanden/weken)” AND “6+ maaden”
#WITH THIS FUNCTION I WILL ONLY TAKE THE NUMBER FROM THE STRING 
def keepNumbers(data):
    import re
    result = re.sub("[^0-9]", "", data)
    result = int(result)
    return result


#BY THIS LOOP:
    
#FIRST: WILL START CHECKING EACH ROW IF THE ROW IS A DATETIME OR NOT
#IF IT IS IT WILL DIRECTLY COPY THE DATA TO OUR NEW DATAFRAME VARIABLE NAMED df
#AND KEEP THE LAST ROW IN DATAFRAME FORMAT IN temp VARIABLE

#ELSE WILL CHECK MORE CONDITIONS DOWN HERE
#IF isinstance NOT TRUE, LETS CHECK IF IT IS RELATED TO WEEKS AND,
#SUBSTRACTING AS MANY WEEKS AS THE NUMBER FOUND BY keepNumbers THE FUNC

#DO THE SAME PROCESS WHEN IT IS RELATED TO MONTHS

#AGAIN, SAME THING IS VALID FOR 'Today'
#WHEN IT IS 'Today' ONLY ONE DAY WILL BE SUBSRACTING
for index in datapd['Aangebodensinds']:
    ifItsDateTimeOrNot = isinstance(index, dt.datetime)
    
    if ifItsDateTimeOrNot == True:
        temp = index
        df.at[countForRows,'dates'] = temp
        
    else:
        if 'weken' in index:
            temptxt = index
            number = keepNumbers(temptxt)
            a = (temp - timedelta(weeks=number))
            df.at[countForRows,'dates'] = a
            
        elif 'maanden' in index:
            temptxt = index
            number = keepNumbers(temptxt)
            a = (temp - pd.DateOffset(months=number))
            df.at[countForRows,'dates'] = a
            
        elif index == 'Vandaag':
            a = (temp - timedelta(days=1))
            df.at[countForRows,'dates'] = a
            
    print(countForRows)
    countForRows = countForRows +1    
    
#LET'S CREATE A NEW CSV FILE WITH OUR NEW RESULTS
df.to_csv('Problem1_Result.csv')    