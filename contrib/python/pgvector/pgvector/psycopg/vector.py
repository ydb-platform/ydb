import psycopg
from psycopg.adapt import Loader, Dumper
from psycopg.pq import Format
from .. import Vector


class VectorDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj):
        return Vector._to_db(obj).encode('utf8')


class VectorBinaryDumper(VectorDumper):

    format = Format.BINARY

    def dump(self, obj):
        return Vector._to_db_binary(obj)


class VectorLoader(Loader):

    format = Format.TEXT

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return Vector._from_db(data.decode('utf8'))


class VectorBinaryLoader(VectorLoader):

    format = Format.BINARY

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return Vector._from_db_binary(data)


def register_vector_info(context, info):
    if info is None:
        raise psycopg.ProgrammingError('vector type not found in the database')
    info.register(context)

    # add oid to anonymous class for set_types
    text_dumper = type('', (VectorDumper,), {'oid': info.oid})
    binary_dumper = type('', (VectorBinaryDumper,), {'oid': info.oid})

    adapters = context.adapters
    adapters.register_dumper('numpy.ndarray', text_dumper)
    adapters.register_dumper('numpy.ndarray', binary_dumper)
    adapters.register_dumper(Vector, text_dumper)
    adapters.register_dumper(Vector, binary_dumper)
    adapters.register_loader(info.oid, VectorLoader)
    adapters.register_loader(info.oid, VectorBinaryLoader)
