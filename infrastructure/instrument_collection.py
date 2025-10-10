from models.instrument import Instrument
import json

class InstrumentCollection:
    FILENAME = 'instruments.json'
    KEYS_OF_INTEREST = ['name', 'type', 'displayName', 'pipLocation', 
                        'displayPrecision', 'tradeUnitsPrecision', 'marginRate']
    
    def __init__(self):
        self.instruments_dict = {}

    
    def load_instruments(self, path):
        self.instruments_dict = {}
        
        filename = f"{path}/{self.FILENAME}"

        with open(filename, 'r') as f:
            data = json.loads(f.read())
            for key, value in data.items():
                self.instruments_dict[key] = Instrument.from_api_object(value)

    def create_file(self, data, path):
        if data is None:
            print("Instrument file creation failed. Data is empty!")
            return
        
        for i in data:
            key = i['name']
            self.instruments_dict[key] = { k: i[k] for k in self.KEYS_OF_INTEREST}

        filename = f"{path}/{self.FILENAME}"

        with open(filename, 'w') as f:
            f.write(json.dumps(self.instruments_dict, indent=2))

        print(f"{filename} saved!")


    def print_instruments(self):
        [print(key, value) for key, value in self.instruments_dict.items()]
        print(len(self.instruments_dict.keys()))



instrument_collection = InstrumentCollection()

