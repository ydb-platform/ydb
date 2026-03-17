import numpy as np
from psycopg2.extensions import adapt, new_array_type, new_type, register_adapter, register_type
from .. import Vector


class VectorAdapter:
    def __init__(self, value):
        self._value = value

    def getquoted(self):
        return adapt(Vector._to_db(self._value)).getquoted()


def cast_vector(value, cur):
    return Vector._from_db(value)


def register_vector_info(oid, array_oid, scope):
    vector = new_type((oid,), 'VECTOR', cast_vector)
    register_type(vector, scope)

    if array_oid is not None:
        vectorarray = new_array_type((array_oid,), 'VECTORARRAY', vector)
        register_type(vectorarray, scope)

    register_adapter(np.ndarray, VectorAdapter)
    register_adapter(Vector, VectorAdapter)
