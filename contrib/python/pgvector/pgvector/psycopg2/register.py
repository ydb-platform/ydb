import psycopg2
from psycopg2.extensions import cursor
from .halfvec import register_halfvec_info
from .sparsevec import register_sparsevec_info
from .vector import register_vector_info


# note: register_adapter is always global
def register_vector(conn_or_curs, globally=False, arrays=True):
    conn = conn_or_curs if hasattr(conn_or_curs, 'cursor') else conn_or_curs.connection
    cur = conn.cursor(cursor_factory=cursor)
    scope = None if globally else conn_or_curs

    # use to_regtype to get first matching type in search path
    cur.execute("SELECT typname, oid FROM pg_type WHERE oid IN (to_regtype('vector'), to_regtype('_vector'), to_regtype('halfvec'), to_regtype('_halfvec'), to_regtype('sparsevec'), to_regtype('_sparsevec'))")
    type_info = dict(cur.fetchall())

    if 'vector' not in type_info:
        raise psycopg2.ProgrammingError('vector type not found in the database')

    register_vector_info(type_info['vector'], type_info['_vector'] if arrays else None, scope)

    if 'halfvec' in type_info:
        register_halfvec_info(type_info['halfvec'], type_info['_halfvec'] if arrays else None, scope)

    if 'sparsevec' in type_info:
        register_sparsevec_info(type_info['sparsevec'], type_info['_sparsevec'] if arrays else None, scope)
