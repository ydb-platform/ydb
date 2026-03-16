import psycopg2.extensions
from psycopg2.extras import Inet
 

class Macaddr(Inet):
    """
    Wrap a string for the MACADDR type, like Inet
    """
    def getquoted(self):
        obj = psycopg2.extensions.adapt(self.addr)
        if hasattr(obj, 'prepare'):
            obj.prepare(self._conn)
        return obj.getquoted() + b"::macaddr"


class Macaddr8(Inet):
    """
    Wrap a string for the MACADDR8 type, like Inet
    """

    def getquoted(self):
        obj = psycopg2.extensions.adapt(self.addr)
        if hasattr(obj, 'prepare'):
            obj.prepare(self._conn)
        return obj.getquoted() + b"::macaddr8"



# Register array types for CIDR and MACADDR (Django already registers INET) 
CIDRARRAY_OID = 651
CIDRARRAY = psycopg2.extensions.new_array_type(
    (CIDRARRAY_OID,),
    'CIDRARRAY',
    psycopg2.extensions.UNICODE,
)
psycopg2.extensions.register_type(CIDRARRAY)

MACADDRARRAY_OID = 1040
MACADDRARRAY = psycopg2.extensions.new_array_type(
    (MACADDRARRAY_OID,),
    'MACADDRARRAY',
    psycopg2.extensions.UNICODE,
)
psycopg2.extensions.register_type(MACADDRARRAY)

# select typarray from pg_type where typname = 'macaddr8' -> 775
MACADDRARRAY8_OID = 775
MACADDRARRAY8 = psycopg2.extensions.new_array_type(
    (MACADDRARRAY8_OID,),
    'MACADDRARRAY8',
    psycopg2.extensions.UNICODE,
)
psycopg2.extensions.register_type(MACADDRARRAY8)