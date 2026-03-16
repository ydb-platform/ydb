from psycopg.adapt import Loader, Dumper
from psycopg.pq import Format
from .. import HalfVector


class HalfVectorDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj):
        return HalfVector._to_db(obj).encode('utf8')


class HalfVectorBinaryDumper(HalfVectorDumper):

    format = Format.BINARY

    def dump(self, obj):
        return HalfVector._to_db_binary(obj)


class HalfVectorLoader(Loader):

    format = Format.TEXT

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return HalfVector._from_db(data.decode('utf8'))


class HalfVectorBinaryLoader(HalfVectorLoader):

    format = Format.BINARY

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return HalfVector._from_db_binary(data)


def register_halfvec_info(context, info):
    info.register(context)

    # add oid to anonymous class for set_types
    text_dumper = type('', (HalfVectorDumper,), {'oid': info.oid})
    binary_dumper = type('', (HalfVectorBinaryDumper,), {'oid': info.oid})

    adapters = context.adapters
    adapters.register_dumper(HalfVector, text_dumper)
    adapters.register_dumper(HalfVector, binary_dumper)
    adapters.register_loader(info.oid, HalfVectorLoader)
    adapters.register_loader(info.oid, HalfVectorBinaryLoader)
