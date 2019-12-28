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