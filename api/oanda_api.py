import requests
from datetime import datetime as dt
import pandas as pd

from constants import ACCOUNT_ID, API_KEY, OANDA_URL


class OandaAPI():

    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self):
        self.session = requests.Session()
        self._session_headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        self.session.headers.update(self._session_headers)

    
    def make_request(self, urlpaths: str, verb='get', 
                     code=200, params=None, data=None, headers=None):
        
        full_url = f"{OANDA_URL}/{urlpaths}"

        try:
            response = None
            if verb == 'get':
                response = self.session.get(url=full_url, 
                                            headers=headers, data=data, params=params)
            
            if response is None:
                return False, {"error": f"specified verb {verb} not found!"}
            
            if response.status_code == code:
                return True, response.json()
            else:
                return False, response.json()

        except Exception as error: 
            return False, {'Exception': error}

        
    def get_account_ep(self, ep, data_key):
        url = f'accounts/{ACCOUNT_ID}/{ep}'
        ok, data = self.make_request(url)

        if ok and data_key in data:
            return data[data_key]
        
        else:
            print("ERROR OandaAPI.get_account_ep()", data)
            return
        

    def get_account_summary(self):
        return self.get_account_ep("summary", "account")
    

    def get_account_instruments(self):
        return self.get_account_ep("instruments", "instruments")
    

    def fetch_candles(self, pair_name: str, count: int = 10,
                      granularity: str = 'H1', price="MBA", date_from=None, date_to=None) -> tuple[int, list]:
        url = f"instruments/{pair_name}/candles"
        params = dict(
            granularity=granularity,
            price=price                 # mid, bid and ask prices
        )

        if date_from is not None and date_from is not None:
            params["from"] = date_from.strftime(self.DATE_FORMAT)
            params["to"] = date_to.strftime(self.DATE_FORMAT)
        else:
            params['count'] = count

        ok, data = self.make_request(url, params=params)
        
        if ok and "candles" in data:
            return data["candles"]
        
        else:
            print("ERROR OandaAPI.fetch_candles()", data)
            return




    def get_candles_df(self, pair_name: str | None,  data: list | None = None, **kwargs) -> pd.DataFrame:

        if pair_name is None and data is None:
            raise ValueError("OandaAPI.get_candles_df(...): provide either pair_name or data as list")

        if data is None:
            data = self.fetch_candles(pair_name=pair_name, **kwargs)

        if data is None: return None
        if len(data) == 0: return pd.DataFrame()       # in case data is empty

        final_data = []

        prices = ['mid', 'bid', 'ask']
        ohlc = ['o', 'h', 'l', 'c']

        for candle in data:
            if not candle['complete']: continue 
            new_dict = {}
            new_dict['time'] = candle['time']

            for p in prices:
                if p not in candle: continue
                for o in ohlc:
                    new_dict[f'{p}_{o}'] = float(candle[p][o])

            new_dict['volume'] = candle['volume']
            final_data.append(new_dict)

        df = pd.DataFrame(final_data)
        df['time'] = pd.to_datetime(df['time'])
        return df
    


