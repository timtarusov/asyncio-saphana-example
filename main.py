import asyncio
import yaml
from concurrent.futures import ThreadPoolExecutor
from functools import wraps, partial

import pandas as pd
from hdbcli import dbapi

TABLES = [
    "YBS_HC.SWP.TABLE::SWP_PROJECTS",
    "YBS_HC.SWP.TABLE::SWP_EMPROLE",
    "YBS_HC.SWP.TABLE::SWP_TARGETS",
    "YBS_HC.SWP.TABLE::SWP_TARGET_GROWTH",
]

_executor = ThreadPoolExecutor(max_workers=len(TABLES))

with open("./credentials.yaml", 'r') as f:
    creds = yaml.load(f, Loader=yaml.FullLoader)
    
def write_to_csv(df:pd.DataFrame, table:str):
    df.to_csv(f"./data/{table}.csv", index=False, sep=";")
    
def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=_executor, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run 

async_read_table = async_wrap(pd.read_sql)  
async_write_to_csv = async_wrap(write_to_csv)  

async def read_table(table:str, conn:dbapi.Connection):
    print(f"Reading from {table}")
    df = await async_read_table(sql=f"""
        select * from "{table}" """, con=conn)
    # print(df)
    await async_write_to_csv(df=df, table=table)
    print(f"Written to {table}.csv")
    
async def get_from_hana(conn:dbapi.Connection):
    readers = [read_table(table, conn) for table in TABLES]
    res = await asyncio.gather(*readers)
    return res
    


if __name__ == "__main__":
    conn = dbapi.connect(address="hanabwprod-01.severstal.severstalgroup.com",
                         port=30015, user=creds["user"], password=creds["password"])    
    asyncio.run(get_from_hana(conn))
    conn.close()