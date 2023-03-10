import requests
import pandas as pd
import time
import datetime
import hmac
import hashlib

# 約定履歴を取得してCSVとして保存する
# https://lightning.bitflyer.com/docs#%E7%B4%84%E5%AE%9A%E5%B1%A5%E6%AD%B4

# APIキーとシークレットキーを入力
API_KEY = 'YOUR_API_KEY'
API_SECRET = 'YOUR_API_SECRET'

# APIエンドポイント
url = 'https://api.bitflyer.com/v1/getexecutions'

# 期間の指定（例：2022年1月1日〜2022年2月28日）
start_date = datetime.datetime(2022, 1, 1)
end_date = datetime.datetime(2022, 2, 28)

# 取得するデータの種類
params = {
    'product_code': 'FX_BTC_JPY'
}

# 取得したデータを格納するリスト
results = []

# 時間帯ごとにデータを取得
while start_date < end_date:
    start_time = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_time = (start_date + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

    # APIにリクエストを送信
    headers = {
        'ACCESS-KEY': API_KEY,
        'ACCESS-TIMESTAMP': str(int(time.time())),
        'ACCESS-SIGN': '',
        'Content-Type': 'application/json'
    }

    message = headers['ACCESS-TIMESTAMP'] + \
              'GET' + \
              '/v1/getexecutions' + \
              '' + \
              headers['ACCESS-KEY']

    signature = hmac.new(API_SECRET.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()
    headers['ACCESS-SIGN'] = signature

    params['count'] = 500
    params['before'] = 0
    params['after'] = 0
    params['after'] = start_time
    params['before'] = end_time

    res = requests.get(url, headers=headers, params=params)

    if res.status_code == 200:
        executions = res.json()
        results += executions
        print('Data from ' + start_time + ' to ' + end_time + ' is collected.')
    else:
        print('Failed to get data from ' + start_time + ' to ' + end_time + '.')

    start_date += datetime.timedelta(hours=24)
    time.sleep(1)  # 連続してAPIリクエストを送信しないように1秒間のウェイトを設定

# 取得したデータをpandasデータフレームに変換
df = pd.DataFrame(results)

# CSVファイルとして保存
df.to_csv('bitflyer_fx_executions.csv', index=False)
