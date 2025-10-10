import dataclasses


@dataclasses.dataclass
class Instrument:

    iname: str                  # instrument name
    itype: str                  # instrument type
    display_name: str
    pip_location: float
    trade_units_precicion: float
    margin_rate: float


    def __repr__(self):
        return str(vars(self))
    

    @classmethod
    def from_api_object(cls, ob):
        return Instrument(
            ob['name'],
            ob['type'],
            ob['displayName'],
            10**float(ob['pipLocation']),
            ob['tradeUnitsPrecision'],
            float(ob['marginRate']),
        )

