import asyncio
import pybotters
import pandas as pd
from loguru import logger

import json
import requests
import pandas as pd
import datetime
import time

# async def main():
#     async with pybotters.Client() as client:
#         path = generate_path(
#             exchange='bitflyer',
#             pair='btcjpy',
#             min='60',
#             unixTime=1675368565
#             )
#         # REST API
#         resp = await client.get(path)
#         data = await resp.json()
#         print(resp)
#         # print(data)
#         # df = pd.read_json(data)
#         # df

#         # メインループ
#         # while True:
#         #     # データ参照
#         #     orderbook = store.orderbook.find()

async def main(candle: int, before: int, size: int) -> pd.DataFrame:
    async with pybotters.Client() as client:
        url="https://api.cryptowat.ch/markets/bitflyer/btcfxjpy/ohlc?periods="
        candle=candle*60
        period=("%s"%(candle))
        period=[period]
        beforeurl="&before="
        afterurl="&after="
        # The maximum size of data is 6000 depending on a timing (sometimes you get less than 6000)
        after= before-(candle*size)
        URL=("%s%s%s%s%s%s"%(url,candle,afterurl,after,beforeurl,before))
        # e.g https://api.cryptowat.ch/markets/bitflyer/btcjpy/ohlc?periods=86400&after=1483196400

        response = await client.get(URL)
        body = await response.json()
        res = body['result']
        # print(data['result'])

        # res=json.loads(requests.get(URL).text)["result"]
        # print(res)
        
        data = []
        for i in period:
            row = res[i]
            for column in row:
                if column[4] != 0:
                    column = column[0:6]
                    data.append(column)

        date = [price[0] for price in data]
        op = [int(price[1]) for price in data]
        hi = [int(price[2]) for price in data]
        lo = [int(price[3]) for price in data]
        cl = [int(price[4]) for price in data]
        date_datetime = map(datetime.datetime.fromtimestamp, date)
        dti = pd.DatetimeIndex(date_datetime)
        df = pd.DataFrame({"op" : op, "hi" : hi, "lo": lo, "cl" : cl}, index=dti)
        
        return df


if __name__ == '__main__':
    try:
        # df = await main(15, 1675368565, 6000)
        # print(df)
        asyncio.run(main(15, 1675368565, 6000))
    except KeyboardInterrupt:
        pass