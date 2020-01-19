import pandas as pd 
import numpy as np 
import pickle





class estrategias:
    decisao_previa =''  
    preco_inicial=0
    resultado_previo=0      
    
    with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)
    data1 =data['Adj Close']
    def estrat(self,x):
        
        if self.decisao_previa!=x:
            self.decisao_previa = x
            return x
        
        else:
            return np.nan


    def estrat_mmovel(self,serie,longmean=100,shortmean=10):

   
            
        serie =pd.DataFrame(serie)

        



        serie['roll']   =  serie.rolling(shortmean,min_periods=5).mean()-serie.rolling(longmean,min_periods=5).mean()
        

        serie['DECISAO']=np.nan
        
        serie.loc[serie['roll']>0.0,'DECISAO']='COMPRA'
        serie.loc[serie['roll']<=0.0,'DECISAO'] ='VENDA'
        serie=serie.dropna()
        serie['ESTRAT'] =serie['DECISAO'].apply(func=self.estrat)

    
        
        return serie
    def returns(self,serie,tipo_oper ='COMPRA'):

        
        serie=serie.dropna()

        serie['LUCRO']= serie.iloc[:,0].shift(-1)/(serie.iloc[:,0])


        serie= serie.loc[(serie['ESTRAT']==tipo_oper) & (pd.notna(serie['LUCRO']))]





        retorno_total=serie['LUCRO'].rolling(len(serie )).apply(np.prod, raw=True).dropna()

        return retorno_total

    def estrat_retornos(self,x):
    
        
        
        
        if x['DECISAO']=='COMPRA' and x['DECISAO']!=self.decisao_previa  :
            self.preco_inicial=x.iloc[0]
            self.decisao_previa='COMPRA'
        elif    x['DECISAO']=='COMPRA' and x['DECISAO']==self.decisao_previa  :
            if self.resultado_previo!=0:
                return((x.iloc[0]/self.preco_inicial)*self.resultado_previo)
            else: 
                return(x.iloc[0]/self.preco_inicial)
        elif x['DECISAO']=='VENDA' and self.decisao_previa =='COMPRA':
            self.decisao_previa='VENDA'
            if self.resultado_previo!=0:
                self.resultado_previo=(x.iloc[0]/self.preco_inicial*self.resultado_previo)
            else :
                self.resultado_previo = x.iloc[0]/self.preco_inicial
            
            return(self.resultado_previo)
    def estrat_retornos1(self,x):
    
        
        
        if x['DECISAO']=='COMPRA' and x['DECISAO']!=self.decisao_previa  :
            self.preco_inicial=x.iloc[0]
            self.decisao_previa=x['DECISAO']
        elif    x['DECISAO']=='COMPRA' and x['DECISAO']==self.decisao_previa  :
            return((x.iloc[0]/self.preco_inicial))
            
        elif x['DECISAO']=='VENDA' and self.decisao_previa =='COMPRA':
            self.decisao_previa='VENDA'
            self.resultado_previo=(x.iloc[0]/self.preco_inicial)
                       
            return(self.resultado_previo)
    









    def worker(self,params):
        
        global data1
        pars = {}
        pars2 = {}
        for i in  params:
            z,y=i[0],i[1]
            test =self.data1.apply(estrat_mmovel,args=(z,y)) 
            pars[(z,y)]=test.sum().mean()
            pars2[(z,y)]=test.sum().median()
        return pars