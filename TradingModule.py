import pandas as pd 
import numpy as np 
import pickle

inic =''
with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)
data1 =data['Adj Close']
def estrat(x):
    global inic
    if inic!=x:
        inic = x
        return x
    
    else:
        return np.nan


def rollmeanstrat(serie,longmean=100,shortmean=10):
    
    serie =pd.DataFrame(serie)

   



    serie['roll']   =  serie.rolling(shortmean,min_periods=5).mean()-serie.rolling(longmean,min_periods=5).mean()
    

    serie['DECISAO']=np.nan
    
    serie.loc[serie['roll']>0.0,'DECISAO']='COMPRA'
    serie.loc[serie['roll']<=0.0,'DECISAO'] ='VENDA'
    
    serie['ESTRAT'] =serie['DECISAO'].apply(func=estrat)

    
    x =returns(serie)
    
    return x
def returns(serie,tipo_oper ='COMPRA'):

    
    serie=serie.dropna()

    serie['LUCRO']= serie.iloc[:,0].shift(-1)/(serie.iloc[:,0])


    serie= serie.loc[(serie['ESTRAT']==tipo_oper) & (pd.notna(serie['LUCRO']))]





    retorno_total=serie['LUCRO'].rolling(len(serie )).apply(np.prod, raw=True).dropna()

    return retorno_total


def worker(params):
    
    global data1
    pars = {}
    pars2 = {}
    for i in params:
        z,y=i[0],i[1]
        test =data1.apply(rollmeanstrat,args=(z,y)) 
        pars[(z,y)]=test.sum().mean()
        pars2[(z,y)]=test.sum().median()
    return pars