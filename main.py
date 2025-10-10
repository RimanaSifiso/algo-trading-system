from api.oanda_api import OandaAPI
from infrastructure.instrument_collection import instrument_collection
from pandas import to_datetime
from infrastructure.data_collector import run_collection

if __name__ == "__main__":
    api = OandaAPI()

    # data = api.get_instruments()
    # print([x['name'] for x in data])

    # instrument_collection.create_file(api.get_account_instruments(), './data')
    instrument_collection.load_instruments('./data')
    # instrument_collection.print_instruments()

    # print(api.fetch_candles("EUR_USD", granularity='D', count=2, price="A"))
    # date_from = to_datetime("2025-10-01T01:00:00Z")
    # date_to = to_datetime("2025-10-08T18:00:00Z")

    # df_candles = api.get_candles_df("EUR_USD", granularity='D', date_from=date_from, date_to=date_to)
    # print(df_candles.head())
    # print("----------------------------------------------------------------------")
    # print(df_candles.tail())
    run_collection(instrument_collection=instrument_collection, api=api)



    