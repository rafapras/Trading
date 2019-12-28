import pandas as pd
from pandas_datareader import data
import matplotlib.pyplot as plt
import yfinance as yf
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
# pyfolio  qgrid empyrical
import pickle
import numpy as np
import time
import seaborn as sns
from itertools import combinations
import multiprocessing as mp
from tqdm import tqdm
import sys
sys.path.append('C:\\Users\\Rafael\\Trading\\Trading')
import TradingModule

inic =''
with open('C:\\Users\\Rafael\\Trading\\Trading\\ibov.pkl','rb') as file:
        data =pickle.load(file)
data1 =data['Adj Close']



def worker(params):
    
    global data1
    pars = {}
    pars2 = {}
    for i in params:
        z,y=i[0],i[1]
        test =data1.apply(TradingModule.rollmeanstrat,args=(z,y)) 
        pars[(z,y)]=test.sum().mean()
        pars2[(z,y)]=test.sum().median()
    return pars