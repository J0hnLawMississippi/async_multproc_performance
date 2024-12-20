#!/usr/bin/env python3

import json
import asyncio
import websockets
import time
import tracemalloc
import csv
import requests
from concurrent.futures import ProcessPoolExecutor
from math import ceil
from morerequests import req1

jsonhead = { "Content-Type": "application/json" }


def measure_execution_and_memory(func):
    def wrapper(*args, **kwargs):
        # Start tracing memory allocations
        tracemalloc.start()
        
        # Record start time
        start_time = time.perf_counter()
        
        # Call the original function
        result = func(*args, **kwargs)
        
        # Record end time
        end_time = time.perf_counter()
        
        # Take a snapshot of the memory allocation
        snapshot = tracemalloc.take_snapshot()
        
        # Get the top memory-consuming lines
        top_stats = snapshot.statistics('lineno')
        
        # Calculate the total memory usage
        total_memory_usage = sum(stat.size for stat in top_stats)
        
        # Stop tracing memory allocations
        tracemalloc.stop()
        
        # Return the result along with execution time and memory usage
        return result, end_time - start_time, total_memory_usage
    
    return wrapper



def write_results_to_csv(filename, results):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['Function', 'Execution Time (seconds)', 'Memory Usage (bytes)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow({
                'Function': result['function_name'],
                'Execution Time (seconds)': result['execution_time'],
                'Memory Usage (bytes)': result['memory_usage']
            })


def stringin(string, v0, v1, v2):
	return string.format(v0,v1,v2)

def get_options(base_string, asset, maturity, strike_itvl):

    idxurl = f"https://deribit.com/api/v2/public/get_index?currency={asset}"
    index = req1(idxurl,jsonhead, 'BTC').reqjson1()

    strikezero = ceil(index/strike_itvl) * strike_itvl
    X0 = [strikezero - 5*strike_itvl + i*strike_itvl for i in range(10)]

    expiry0  = [maturity]
    inst0 = ['C','P']

    expiry1 = expiry0 * len(X0) * len(inst0)
    inst1 = len(expiry0) * (inst0[0] * len(X0) + inst0[1] *len (X0))
    X1 = list(X0) * len(expiry0) * len(inst0)
        
    InstList = list(map(lambda x,y,z: stringin(base_string, x,y,z), expiry1, X1, inst1))
    return InstList

async def ws_get_order_book(instrument):
    async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
        msg = {
            "jsonrpc": "2.0",
	    "id": 1,
            "method": "public/get_order_book",
            "params": {
	        "instrument_name": instrument
	    }
        }
        await websocket.send(json.dumps(msg))
        response = await websocket.recv()
        return json.loads(response)


@measure_execution_and_memory
def ws_fetch_order_books(instruments):
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(loop_fun(instruments))
    return results


async def loop_fun(instruments):
    tasks = [ws_get_order_book(instrument) for instrument in instruments]
    results = await asyncio.gather(*tasks)
    return results


def get_order_book(instrument):
    url = "https://deribit.com/api/v2/public/get_order_book"
    params = {"instrument_name": instrument}
    response = requests.get(url, params=params)
    return response.json()

@measure_execution_and_memory
def fetch_order_books(instruments):
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(get_order_book, instrument): instrument for instrument in instruments}
        results = {}
        for future in futures:
            instrument = futures[future]
            try:
                results[instrument] = future.result()
            except Exception as e:
                print(f"Error fetching {instrument}: {e}")
    return results


string0 = 'BTC-{0}-{1}-{2}'
insts = get_options(string0,'BTC','31JAN25',5000)


# Main execution
if __name__ == "__main__":
    results = []
    func0_name = "ws_fetch_order_books(instruments)"
    _, execution_time0, memory_usage0 = ws_fetch_order_books(insts)
    results.append({
        'function_name': func0_name,
        'execution_time': execution_time0,
        'memory_usage': memory_usage0
    })
    
    func1_name = "fetch_order_books(instruments)"
    _, execution_time1, memory_usage1 = fetch_order_books(insts)
    results.append({
        'function_name': func1_name,
        'execution_time': execution_time1,
        'memory_usage': memory_usage1
    })
    
    write_results_to_csv('execution_results.csv', results)
