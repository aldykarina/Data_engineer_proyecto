import requests
import pandas as pd
import datetime
from io import StringIO
import logging

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::GetDataModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
    

class DataInformation:
    def __init__(self, function:str = "TIME_SERIES_DAILY", symbols:list = ['SPY', 'AAPL', 'GOOGL'], API_KEY:str = None) -> None:
        self.function = function
        self.symbols = symbols
        self.API_KEY = API_KEY
    
    def get_data(self, symbol, API_KEY):
        url = f'https://www.alphavantage.co/query?function={self.function}&symbol={symbol}&apikey={API_KEY}&outputsize=compact'
        response = requests.get(url)
        data = response.json()
        
        time_series = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.rename(columns={
            '1. open': 'open_price',
            '2. high': 'high_price',
            '3. low': 'low_price',
            '4. close': 'close_price',
            '5. volume': 'volume'
        })
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df.reset_index().rename(columns={'index': 'date'})
        df['symbol'] = symbol
        df['ingestion_time'] = datetime.datetime.now() 
              
        return df
          
    def get_all_data(self, API_KEY):
        
        all_data = []
        for symbol in self.symbols:
            df = self.get_data(symbol, API_KEY)
            if not df.empty:
                all_data.append(df)
            
        try:
            
            combined_data = pd.concat(all_data, ignore_index=True) 
            print(combined_data)       
           
            buffer = StringIO()
            combined_data.info(buf=buffer)
            s = buffer.getvalue()
            logging.info(s)
            logging.info(f"Data created")
            return combined_data
        
        except Exception as e:
            logging.error(f"Not able to import the data from the api\n{e}")                  
            raise