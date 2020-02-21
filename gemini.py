import ssl
import websocket
import _thread as thread
import datetime
import csv

#transforming timestamps to datetime
def ms_to_datetime(timestampms):
    res = datetime.datetime.fromtimestamp(timestampms/1000.0).isoformat()
    return res[0:13]

def ts_to_datetime(timestampms):
    res = datetime.datetime.fromtimestamp(timestampms).isoformat()
    return res[0:13]

class gemini(object):
    def __init__(self):
        #initialzing websocket and use dictionary to store change of the order book
        self.logon_msg = '{"type": "subscribe","subscriptions":[{"name":"l2","symbols":["BTCUSD"]}]}'
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://api.gemini.com/v2/marketdata",
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_open=self.on_open)
        self.dict_change = {}
        self.last_date_hour = ''
        #prepare csv for storing the data
        with open(r"C:\Users\yuan\PycharmProjects\evisx\changes_gemini.csv",'w+',newline = '') as self.gemini_changes:
            self.writer = csv.writer(self.gemini_changes)
            self.writer.writerow(['change','date_hour'])
        with open(r"C:\Users\yuan\PycharmProjects\evisx\trades_gemini.csv",'w+',newline = '') as self.gemini_trades:
            self.writer2 = csv.writer(self.gemini_trades)
            self.writer2.writerow(['date_hour','price','volume'])
        self.ws.on_open = self.on_open
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def on_message(self, message):
        splited_mes = message[1:-1].split(',')
        
        #check if the data is trade data or order book changes
        if splited_mes[0] == '"type":"l2_updates"':
            try:
                change_list = eval(message[49:-1])
                for change in change_list:

                    volume = float(change[2])
                    date_hour = ts_to_datetime(datetime.datetime.now().timestamp())

                    if date_hour != self.last_date_hour and self.last_date_hour in self.dict_change:
                        #write the change data with the correct datetime
                        with open(r"C:\Users\yuan\PycharmProjects\evisx\changes_gemini.csv", 'a+',
                                  newline='') as self.gemini_changes:
                            self.writer = csv.writer(self.gemini_changes)
                            self.writer.writerow([self.dict_change[self.last_date_hour], self.last_date_hour])
                        self.last_date_hour = date_hour
                    elif date_hour != self.last_date_hour and self.last_date_hour not in self.dict_change:
                        self.last_date_hour = date_hour

                    else:
                        try:#if the datetime is in the dict and is the same hour, add the volume to the current one
                            self.dict_change[date_hour] += volume
                        except:#if there is no datetime in the dict, initialize the first entry
                            self.dict_change[date_hour] = 0
            except SyntaxError:#get rid of the first messege which contains information before our time of subscribe
                pass

        elif splited_mes[0] == '"type":"trade"':
            #if it is trade data, write to the trade data set


            timestamp = int(splited_mes[3].split(':')[-1])
            date_hour = ms_to_datetime(timestamp)
            price = float(splited_mes[4].split(':')[-1].strip('"'))
            quantity = float(splited_mes[5].split(':')[-1].strip('"'))

            with open(r"C:\Users\yuan\PycharmProjects\evisx\trades_gemini.csv", 'a+', newline='') as self.gemini_trades:
                self.writer2 = csv.writer(self.gemini_trades)
                self.writer2.writerow([date_hour, price,quantity])

    def on_error(self, error):
        print(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        def run(*args):
            self.ws.send(self.logon_msg)
        thread.start_new_thread(run, ())
