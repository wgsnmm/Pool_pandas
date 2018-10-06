import pandas as pd
import jieba.analyse
from threading import Thread   
import os

def jieba_jisuan():
    if b < 3 :
        end_num = (b+1)*cont
    else :
        end_num = len(df1.index)
    start_num = b*cont 
    for i in range(start_num,end_num):
        df1['TF-IDF'].loc[i] = jieba.analyse.extract_tags(df1['cont'].loc[i],topK=10)
        df1['TextRank'].loc[i] = jieba.analyse.textrank(df1['cont'].loc[i],topK=10)

path = r'C:\Users\MSI-PC\Desktop'
os.chdir(path)
df = pd.read_excel('groupid.xlsx',encoding='utf-8')
data = df.groupby(by='groupid')['cont'].sum()
df1 = data.to_frame(name = None)
df1['groupid'] = df1.index
df1.index = range(len(df1.index))
df1['TF-IDF'] = df1['TextRank']=0
b = 0
cont = int(len(df1.index)/3)
for j in range(4):
    th = Thread(target = jieba_jisuan)
    th.start()
    b +=1
df1.to_excel('result.xlsx',encoding='utf-8',index=False,columns=['groupid','TF-IDF','TextRank'])
