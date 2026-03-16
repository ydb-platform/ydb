import numpy as np
from .. import Vector, HalfVector, SparseVector


def register_vector(conn):
    # use to_regtype to get first matching type in search path
    res = conn.run("SELECT typname, oid FROM pg_type WHERE oid IN (to_regtype('vector'), to_regtype('halfvec'), to_regtype('sparsevec'))")
    type_info = dict(res)

    if 'vector' not in type_info:
        raise RuntimeError('vector type not found in the database')

    conn.register_out_adapter(Vector, Vector._to_db)
    conn.register_out_adapter(np.ndarray, Vector._to_db)
    conn.register_in_adapter(type_info['vector'], Vector._from_db)

    if 'halfvec' in type_info:
        conn.register_out_adapter(HalfVector, HalfVector._to_db)
        conn.register_in_adapter(type_info['halfvec'], HalfVector._from_db)

    if 'sparsevec' in type_info:
        conn.register_out_adapter(SparseVector, SparseVector._to_db)
        conn.register_in_adapter(type_info['sparsevec'], SparseVector._from_db)
