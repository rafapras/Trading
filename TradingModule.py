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
        
        
        if     self.resultado_previo!=0:
            retorno_total=  (x.iloc[0]/self.preco_inicial)*self.resultado_previo  
        if  self.preco_inicial!=0:
            retorno_op =  x.iloc[0]/self.preco_inicial  

        
        
        if x['DECISAO']=='COMPRA' and x['DECISAO']!=self.decisao_previa  :
            
            self.preco_inicial=x.iloc[0]
            self.decisao_previa='COMPRA'
        
        
        elif    x['DECISAO']=='COMPRA' and x['DECISAO']==self.decisao_previa  :
               
            if self.resultado_previo!=0:
                return([retorno_op, retorno_total])
            else: 
                return([retorno_op,retorno_op])
        elif x['DECISAO']=='VENDA' and self.decisao_previa =='COMPRA':
            self.decisao_previa='VENDA'
            if self.resultado_previo!=0:
                self.resultado_previo=retorno_total
                return([retorno_op, retorno_total])
            else :
                self.resultado_previo = retorno_op
                return([retorno_op,retorno_op])




    def estrat_retornos22(self,x):


        if     self.resultado_previo!=0:
            retorno_total=  (x[0]/self.preco_inicial)*self.resultado_previo  
        if  self.preco_inicial!=0:
            retorno_op =  x[0]/self.preco_inicial  



        if x[1]=='COMPRA' and x[1]!=self.decisao_previa  :
            
            self.preco_inicial=x[0]
            self.decisao_previa='COMPRA'
            
            #return(1,1)

        elif    x[1]=='COMPRA' and x[1]==self.decisao_previa  :
            
            if self.resultado_previo!=0:
                return([retorno_op, retorno_total])
            else: 
                return([retorno_op,retorno_op])
        elif x[1]=='VENDA' and self.decisao_previa =='COMPRA':
            self.decisao_previa='VENDA'
            if self.resultado_previo!=0:
                self.resultado_previo=retorno_total
                return([retorno_op, retorno_total])
            else :
                self.resultado_previo = retorno_op
                return([retorno_op,retorno_op])











            
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
    
    result=''
    def stop(self,valores,gain=1000000000,loss=10000000000):
        if valores[0]=='COMPRA':
            print(self.result,valores,loss) 
        if valores[1]>gain:
            
            print(1)
            self.result='VENDA'
            return ('VENDA')
        elif valores[1]<loss:
            print(2)
            self.result='VENDA'
            return( 'VENDA')
        elif self.result=='VENDA' and  valores[0]=='COMPRA':
            print(3)
            return('VENDA')
        elif self.result=='VENDA' and  valores[0]=='VENDA':
            print(4)
            self.result='COMPRA'
            return ('VENDA')
        else:
            
            return valores[0]








def worker(params):
    with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)

    data1 =data['Adj Close']
    data1.drop('NAN.SA',axis=1,inplace=True)
    data1.drop('MGLU3.SA',axis=1,inplace=True)
    data1.drop('BPAC11.SA',axis=1,inplace=True)
    data1.drop('KLBN11.SA',axis=1,inplace=True)
    data1.drop('TAEE11.SA',axis=1,inplace=True)
    lista=[]
    lista2 =[]
    for n in params:
        long=n[0]
        short=n[1]
        
        for i in data1.columns:

            td1 =estrategias()
            teste=td1.estrat_mmovel(data1[i],long,short)
            td2=estrategias()

            

            
            teste2=pd.Series([td2.estrat_retornos22((a,b)) for a, b in zip(teste[i], teste['DECISAO'])],index=teste.index ).dropna()
            teste2=pd.DataFrame(teste2.tolist(),index=teste2.index)


            if teste2.empty:
                teste2=pd.DataFrame(columns=[1,2])


            teste2.columns=['RETORNO','RETORNO_TOT']

            teste2= pd.concat([teste,teste2],axis=1)
            lista.append(teste2)
            cols=sorted(data1.columns.to_list()*6)
        y=pd.concat(lista,axis=1)
        y.columns=zip(cols,y)
        y.columns=pd.MultiIndex.from_tuples(y.columns.to_list())
        retorno=(y.xs('RETORNO_TOT',axis=1,level=1).fillna(method='ffill').fillna(1).sum(axis=1)/73)
        lista2.append(retorno)
        lista=[] 
        y=0
    return lista2,y,teste
def worker1(params):
    with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)

    data1 =data['Adj Close']
    data1.drop('NAN.SA',axis=1,inplace=True)

    data1.drop('MGLU3.SA',axis=1,inplace=True)
    data1.drop('BPAC11.SA',axis=1,inplace=True)
    data1.drop('KLBN11.SA',axis=1,inplace=True)
    data1.drop('TAEE11.SA',axis=1,inplace=True)
    #data1.drop('^BVSP',axis=1,inplace=True)
    
    lista=[]
    lista2 =[]
    for n in params:
        long=n[0]
        short=n[1]
        
        for i in data1.columns:

            td1 =estrategias()
            teste=td1.estrat_mmovel(data1[i],long,short)
            td2=estrategias()



            teste2=pd.Series([td2.estrat_retornos22((a,b)) for a, b in zip(teste[i], teste['DECISAO'])],index=teste.index ).dropna()
            teste2=pd.DataFrame(teste2.tolist(),index=teste2.index)


            if teste2.empty:
                teste2=pd.DataFrame(columns=[1,2])
            mask = (teste['DECISAO'] =='COMPRA')
            z_valid = teste[mask]
            teste['ret_dia'] = np.nan
            teste.loc[mask, 'ret_dia'] = teste[i]/ teste[i].shift(1)

            teste2.columns=['RETORNO','RETORNO_TOT']

            teste2= pd.concat([teste,teste2],axis=1)
            lista.append(teste2)
            cols=sorted(data1.columns.to_list()*7)
        y=pd.concat(lista,axis=1)
        y.columns=zip(cols,y)
        y.columns=pd.MultiIndex.from_tuples(y.columns.to_list())
        retorno=(y.xs('RETORNO_TOT',axis=1,level=1).fillna(method='ffill').fillna(1).sum(axis=1)/73)
        #lista2.append(retorno)
        y=y.mask(y=='COMPRA',1)
        y=y.mask(y=='VENDA',0)
        pesos=y.xs('DECISAO',axis=1,level=1).fillna(0).sum(axis=1).reset_index(drop=True)
        div=pd.Series([1]*1301)
        pesos=div.divide(pesos).fillna(0)
        k=y.xs('ret_dia',axis=1,level=1).sum(axis=1).reset_index(drop=True).multiply(pesos)
        x=1
        lista3=[]
        for i in k.fillna(1).iteritems():
            
            x=i[1]*x
            lista3.append(x)
        ret=pd.Series(lista3)
        lista2.append(ret)
        lista=[] 
        lista=[]
#lista.append(ser)
        listax =[]
        for i in y.xs('ret_dia',axis=1,level=1).columns:
            filtro=0
            filtro=((y.loc[:, (slice(i), 'DECISAO')])==1).iloc[:,0]
            #filtro=np.concatenate(filtro)
            serie= y.xs('ret_dia',axis=1,level=1)[filtro][i].dropna()
            listax.append(pd.Series(np.cumprod(serie.values),index=serie.index))
        l=[]
        rends=pd.concat(listax,axis=1)
        cols=[1]*len(data1.columns)
        cal=pd.Series(cols)
        for i in rends.T.columns:
            l.append((rends.T[ i].notnull()*cal).sum())   
        
            
            
        
        
        knn= (pd.DataFrame(pd.concat(
                    listax,axis=1).values*cal.values,index=rends.index).sum(axis=1)/l)
        #y=0
    return knn