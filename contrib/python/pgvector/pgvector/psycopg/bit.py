from psycopg.adapt import Dumper
from psycopg.pq import Format
from .. import Bit


class BitDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj):
        return Bit._to_db(obj).encode('utf8')


class BitBinaryDumper(BitDumper):

    format = Format.BINARY

    def dump(self, obj):
        return Bit._to_db_binary(obj)


def register_bit_info(context, info):
    info.register(context)

    # add oid to anonymous class for set_types
    text_dumper = type('', (BitDumper,), {'oid': info.oid})
    binary_dumper = type('', (BitBinaryDumper,), {'oid': info.oid})

    adapters = context.adapters
    adapters.register_dumper(Bit, text_dumper)
    adapters.register_dumper(Bit, binary_dumper)
