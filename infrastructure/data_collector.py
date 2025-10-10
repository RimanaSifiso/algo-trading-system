import pandas as pd
from infrastructure.instrument_collection import InstrumentCollection
from api.oanda_api import OandaAPI
import datetime
import random, time

MAX_ATTEMPTS = 5
THROTTLE_SECONDS = 0.5
CANDLE_COUNT = 3000
INCREMENTS = {
    'S5': 5 * CANDLE_COUNT,
    'M1': 60 * CANDLE_COUNT,
    'M5': 5 * 60 * CANDLE_COUNT,
    'H1': 60 * 60 * CANDLE_COUNT,
    'H4': 4 * 60 * 60 * CANDLE_COUNT,
    'D': 24 * 60 * 60 * CANDLE_COUNT
}


def save_file(final_df: pd.DataFrame, file_prefix, granularity, pair) -> None:
    filename = f"{file_prefix}{pair}_{granularity}.csv"
    final_df.drop_duplicates(subset=['time'], inplace=True)
    final_df.sort_values(by='time', inplace=True)
    final_df.reset_index(drop=True, inplace=True)
    final_df.to_csv(filename, index=False)

    print(f"collect_data(...) at data_collector.py: {pair} {granularity} {final_df.time.min()} {final_df.time.max()} =>  {final_df.shape[0]} CANDLES SAVED!")



def fetch_candles(pair: str, 
                  granularity: str, 
                  date_from: pd.DatetimeTZDtype,
                  date_to: pd.DatetimeTZDtype,
                  api: OandaAPI) -> pd.DataFrame:
    attempts = 0

    time.sleep(THROTTLE_SECONDS + random.uniform(0, 0.5))
    
    while attempts < MAX_ATTEMPTS:
        
        candles_df = api.get_candles_df(
            pair_name=pair,
            granularity=granularity,
            date_from=date_from,
            date_to=date_to
        )

        if candles_df is not None: break
        # retry in 5 seconds + attempts
        time.sleep(attempts + 5.0)    

        attempts += 1

    if (candles_df is not None) and (not candles_df.empty):
        return candles_df
    return



def collect_data(
        pair: str,
        granularity: str, date_from: pd.DatetimeTZDtype,
        date_to: pd.DatetimeTZDtype, 
        file_prefix: str,
        api: OandaAPI
):
    start_date = pd.to_datetime(date_from)
    end_date = pd.to_datetime(date_to)
    
    candles_dfs = []
    time_step = INCREMENTS[granularity]

    to_date = start_date
    while to_date < end_date:
        to_date = min(start_date + datetime.timedelta(seconds=time_step), end_date)

        df_candles = fetch_candles(
            pair=pair,
            granularity=granularity,
            date_from=start_date,
            date_to=to_date,
            api=api
        )

        if df_candles is not None:
            candles_dfs.append(df_candles)
            print(f"collect_data(...) at data_collector.py: {pair} {granularity} {start_date} {to_date} => {df_candles.shape[0]} CANDLES RETURNED!")
        else:
            print(f"collect_data(...) at data_collector.py: {pair} {granularity} {start_date} {to_date} => NO CANDLES RETURNED!")

        start_date = to_date
        
    if len(candles_dfs):
        final_df = pd.concat(candles_dfs)
        save_file(final_df=final_df, file_prefix=file_prefix, 
                  granularity=granularity, pair=pair)
    else: 
        print(f"collect_data(...) at data_collector.py: {pair} {granularity}  => NO DATA SAVED!")



def run_collection(instrument_collection: InstrumentCollection, api: OandaAPI) -> ...:
    currencies = ['EUR', 'GBP', 'AUD', 'NZD', 'AUD', 'ZAR', 'CAD']
    granularities = ['H1', 'H4', 'S5', 'M5', 'D']

    date_from = "2025-6-01T01:00:00Z"
    date_to = "2025-10-08T18:00:00Z"

    for p1 in currencies:
        for p2 in currencies:
            if p1 == p2: continue
            pair = f"{p1}_{p2}"
            if pair in instrument_collection.instruments_dict.keys():
                for granularity in granularities:
                    time.sleep(1.0)
                    print(f"run_collection(...) at data_collector.py: collecting {pair} {granularity} from {date_from} to {date_to} ...")
                    collect_data(pair, granularity, date_from, date_to, './data/history/', api)
                    print(f"run_collection(...) at data_collector.py: collecting {pair} {granularity} from {date_from} to {date_to} success!")
                    # create_data_file(pair_name=pair, count=4001, granularity=granularity)


