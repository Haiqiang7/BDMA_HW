#对原始数据预处理
def formatd(_row):
  return _row['Date received']+'|'+_row['Product'].lower()+'|'+_row['Company']+'\n'
import csv

with open('complaints.csv','r', encoding='UTF-8') as fi, open('output.csv', 'w',encoding='UTF-8') as fo:
  reader = csv.DictReader(fi)
  for row in reader:
    fo.write(formatd(row))