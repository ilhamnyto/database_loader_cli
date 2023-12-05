import os
import aiomysql
import urllib.parse
import pandas as pd
import numpy as np

class Connection:
    def __init__(self, credentials: dict) -> None:
        self.host = credentials['host']
        self.port = int(credentials['port'])
        self.user = credentials['user']
        self.password = credentials['password']
        self.db = credentials['db']

    async def create_pool(self) -> aiomysql.Pool:
        conf = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': urllib.parse.quote(self.password),
            'db': self.db,
            'minsize': 5,
            'maxsize': 10
        }
        pool = await aiomysql.create_pool(**conf)
        return pool
    
    async def get_query(self, pool: aiomysql.Pool, query: str) -> (list, list):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchall()

                columns_name = [i[0] for i in cur.description]
                return columns_name, list(result)
            
    async def load_data(self, pool: aiomysql.Pool, schema: str, df: pd.DataFrame, table: str):
        df = df.replace({np.nan: None})
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    drop_query = f"DROP TABLE IF EXISTS {table}"

                    await cur.execute(drop_query)

                    await cur.execute(schema)

                    cols = " , ".join([c for c in df.columns.tolist()])
                    for _, row in df.iterrows():
                        query = f"insert into {table} ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ")"
                        await cur.execute(query, tuple(row))

                except Exception as e:
                    await conn.rollback()
                    raise e
                else:
                    await conn.commit()

def MySQLConnection(credentials: dict) -> Connection:
    return Connection(credentials)
