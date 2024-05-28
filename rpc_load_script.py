import datetime
import time

import pandas as pd

from bitcoincli import Bitcoin

# creds

host = "172.33.0.2"
port = "8332"
username = "dappnode"
password = "dappnode"

bitcoin = Bitcoin(username, password, host, port)
def get_latest_block():
    return bitcoin.getblockcount()
def get_tx(api, txid, blockhash):
    return api.getrawtransaction(txid, True, blockhash)
def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции '{func.__name__}': {execution_time:.6f} секунд")
        print(f"Аргументы: args={args}, kwargs={kwargs}")
        return result
    return wrapper
#%%
def ts_to_datetime(ts):
    return datetime.datetime.utcfromtimestamp(ts)


def block_info_to_datetime(block_info):
    return ts_to_datetime(block_info['time'])

@timing_decorator
def binary_search(getfn, target, left, right):
    while right - left > 1:
        middle = (left + right) // 2
        if getfn(middle) >= target:
            right = middle
        else:
            left = middle
    return left
def get_block_by_height(api, height):
    blockhash = api.getblockhash(height)
    return api.getblock(blockhash)
def block_date_by_height(height):
    block_info = get_block_by_height(bitcoin, height)
    return block_info_to_datetime(block_info)

search_start = 830000
search_end = 840000
print(block_date_by_height(search_start))
print(block_date_by_height(search_end))

dt_start = datetime.datetime(2024, 3, 13, 22, 59, 0)
dt_end = datetime.datetime(2024, 3, 18, 18, 34, 0)

start_block = binary_search(block_date_by_height, dt_start, search_start, search_end)

end_block = binary_search(block_date_by_height, dt_end, search_start, search_end)
end_block = end_block + 1

print(block_date_by_height(start_block))
print(block_date_by_height(end_block))
@timing_decorator
def get_block_txs(height):
    txs = []
    block_info = get_block_by_height(bitcoin, height)
    block_hash = block_info['hash']
    block_txs = block_info['tx']
    for txid in block_txs:
        txs.append(get_tx(bitcoin, txid, block_hash))
    return txs

df_name = 'script_rpc_blocks.csv'
txs = []

for bn in range(start_block, end_block):
    txs.extend(get_block_txs(bn))
    if (bn - start_block) % 10 == 0:
        fulldf = pd.DataFrame(txs)
        fulldf.to_csv(df_name, index=False)

fulldf = pd.DataFrame(txs)
fulldf.to_csv(df_name, index=False)