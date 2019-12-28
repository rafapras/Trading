import pandas as pd 
import numpy as np 

#funcao que marca estrategia, recebe como parametros 2 médias móveis
def rollmeanstrat(serie,longmean,shortmean):
    roll=   serie.rolling(shortmean,min_periods=5).mean()-serie.rolling(longmean,min_periods=5).mean()
    inic =''
    def estrat(x):
        global inic
        if inic!=x:
            inic = x
            return x
        else:
            return np.nan
    roll['DECISAO']=np.nan
    roll.loc[roll.iloc[0]>0.0,'DECISAO']='COMPRA'
    roll.loc[roll.iloc[0]<=0.0,'DECISAO'] ='VENDA'
    roll['ESTRAT'] =roll['DECISAO'].apply(func=estrat)


#funcao que dada a marcação retornar os pontos





#função qua calcula os retornos