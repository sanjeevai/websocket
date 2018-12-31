# timestamp, amount, price, direction

from websocket import create_connection
import gzip
import time
import json
import logging
import logging.handlers
import datetime as dt

def setup_logger():
    logger = logging.getLogger()
    
    logger.setLevel(logging.INFO)
    ch = logging.handlers.TimedRotatingFileHandler('huobi.dump', when='h', backupCount=100)
    
    # create formatter
    formatter = logging.Formatter("%(message)s")    

    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

logger = setup_logger()

if __name__ == '__main__':
    while(1):
        try:
            ws = create_connection("wss://api.huobipro.com/ws")
            break
        except:
            print('connect ws error,retry...')
            time.sleep(5)

    #Trade Detail
    tradeStr="""{"sub": "market.btcusdt.trade.detail", "id": "id10"}"""


    ws.send(tradeStr)
    while(1):
        compressData=ws.recv()
        result=gzip.decompress(compressData).decode('utf-8')
        if result[:7] == '{"ping"':
            ts=result[8:21]
            pong='{"pong":'+ts+'}'
            ws.send(pong)
            ws.send(tradeStr)
        else:
            tr = json.loads(result)
            if 'tick' in tr.keys():
               # print(tr)
                for i in range(len(tr['tick']['data'])):
                    timestamp = tr['tick']['data'][i]['ts']
                    amount = tr['tick']['data'][i]['amount']
                    price = tr['tick']['data'][i]['price']
                    direction = tr['tick']['data'][i]['direction']
                    logger.info("{}, {}, {}, {}".format(timestamp, amount, price, direction))
                    # break