from psycopg2.extensions import adapt, new_array_type, new_type, register_adapter, register_type
from .. import HalfVector


class HalfvecAdapter:
    def __init__(self, value):
        self._value = value

    def getquoted(self):
        return adapt(HalfVector._to_db(self._value)).getquoted()


def cast_halfvec(value, cur):
    return HalfVector._from_db(value)


def register_halfvec_info(oid, array_oid, scope):
    halfvec = new_type((oid,), 'HALFVEC', cast_halfvec)
    register_type(halfvec, scope)

    if array_oid is not None:
        halfvecarray = new_array_type((array_oid,), 'HALFVECARRAY', halfvec)
        register_type(halfvecarray, scope)

    register_adapter(HalfVector, HalfvecAdapter)
