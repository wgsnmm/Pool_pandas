import pandas as pd
import jieba.analyse
from threading import Thread   
import os
from multiprocessing import Pool


def jieba_jisuan(df1,b,cont,max):
    if b < (max-1):
        end_num = (b+1)*cont
    else:
        end_num = len(df1.index)
    start_num = b*cont
    l =[]
    for i in range(start_num,end_num):
        l.append(jieba.analyse.extract_tags(df1['cont'].loc[i],topK=10))
        l.append(jieba.analyse.textrank(df1['cont'].loc[i],topK=10))
    return l

if __name__ == '__main__':
    path = r'C:\Users\MSI-PC\Desktop'
    os.chdir(path)
    df = pd.read_excel('groupid.xlsx',encoding='utf-8')
    data = df.groupby(by='groupid')['cont'].sum()
    df1 = data.to_frame(name = None)
    df1['groupid'] = df1.index
    df1.index = range(len(df1.index))
    df1['TF-IDF1'] = df1['TextRank1']=df1['TF-IDF'] = df1['TextRank']=0
    JC = int(input('开几个进程：'))
    ps = Pool(JC)
    result = []
    cont = int(len(df1.index)/(JC-1))
    for b in range(JC):
        df2 = ps.apply_async(jieba_jisuan,args=(df1,b,cont,JC))
        result.append(df2)
    ps.close()
    ps.join()
    TextRank = []
    TFIDF = []
    for res in result:
        for i in range(len(res.get())):
            if i % 2 == 1:
                TextRank.append(res.get()[i])
            else:
                TFIDF.append(res.get()[i])
    df1['TF-IDF'] = TFIDF
    df1['TextRank'] = TextRank
    # for i in df1.index:
    #     df1['TF-IDF1'].loc[i] = jieba.analyse.extract_tags(df1['cont'].loc[i],topK=10)
    #     df1['TextRank1'].loc[i] = jieba.analyse.textrank(df1['cont'].loc[i],topK=10)
    df1.to_excel('result.xlsx',encoding='utf-8',index=False,columns=['groupid','TF-IDF','TextRank'])

