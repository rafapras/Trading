import numpy as np
import pandas as pd
import pickle
import TradingRefact as td
with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
     data =pickle.load(file)
data1 =data['Adj Close']



def estrategia_mmovel(serie,longmean=100,shortmean=10):
    serie =pd.DataFrame(serie)

    



    serie['roll']   =  serie.rolling(shortmean,min_periods=5).mean()-serie.rolling(longmean,min_periods=5).mean()
    

    serie['DECISAO']=np.nan
    
    serie.loc[serie['roll']>0.0,'DECISAO']=0
    serie.loc[serie['roll']<=0.0,'DECISAO'] =1
    #serie=serie.dropna()
    return serie
def troca_posicao(x):
    x['DECISAO_1'] =  x['DECISAO'].shift(1)
    filtro=(x['DECISAO']!=x['DECISAO_1'])
    
    x['ESTRAT']=np.nan 
    x.loc[filtro,'ESTRAT']=x.loc[filtro,'DECISAO']
    return x
def calculate_returns(x):
    x['DAILY_CHANGE']=np.nan
   
    x['DAILY_CHANGE']= x.iloc[:,0]/x.iloc[:,0].shift(1)
    
    trocas=x['ESTRAT']==0
    
    
    comprado=x['DECISAO']==0
    
    
    #x.loc[comprado,'TOTAL_CHANGE']=np.cumprod(x.loc[comprado,'DAILY_CHANGE'].values)   
    x.loc[:,'DAILY_CHANGE']=x.loc[comprado,'DAILY_CHANGE']
    x.loc[trocas,'DAILY_CHANGE']=1
    
    return x 
#print(calculate_returns(troca_posicao(estrategia_mmovel(data1['ABEV3.SA']))))
def stop_loss(x,stop):
    x['SUM']=x['DECISAO'].cumsum()
    
    x["OP_RESULT"]=x[x['DECISAO']==0].groupby(['SUM'])['DAILY_CHANGE'].cumprod()
    f=x['OP_RESULT']<stop
   
    f=x[f]['SUM'].reset_index().groupby(['SUM']).min()
    
    f=pd.DataFrame(index=x.SUM).join(f)
    
    x=x[~(x.index>=f['Date']) &( f.index==x.SUM)]
    return x


def worker(x):
    with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)

    data1 =data['Adj Close']
    data1=data1[data1.index<='2018-08-01']
    data1.drop('MGLU3.SA',axis=1,inplace=True)
    lista=[]
    for n in x:
        long=n[0]
        short=n[1]
        test=[td.stop_loss(td.calculate_returns(td.troca_posicao(td.estrategia_mmovel(data1[x],long,short))),0.95) for  x in  data1.columns ]
        retornos =pd.concat([i['DAILY_CHANGE']  for i  in test],axis=1)
        pesos=pd.notnull(retornos).sum(axis=1)
        retorno=pd.Series(np.cumprod((retornos.sum(axis=1)/pesos).fillna(1).values),index=retornos.index)
        lista.append(retorno)

    return lista