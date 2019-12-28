from multiprocessing import  Pool
from functools import partial
import numpy as np
import pandas as pd

def parallelize(data, func, num_of_processes=4):
    if __name__ == '__main__':
        data_split = np.array_split(data, num_of_processes)
        print(1)
        
        pool = Pool(num_of_processes)
        data = pd.concat(pool.map(func, data_split))
        pool.close()
        pool.join()
        return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)

def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)