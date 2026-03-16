from psycopg.adapt import Loader, Dumper
from psycopg.pq import Format
from .. import SparseVector


class SparseVectorDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj):
        return SparseVector._to_db(obj).encode('utf8')


class SparseVectorBinaryDumper(SparseVectorDumper):

    format = Format.BINARY

    def dump(self, obj):
        return SparseVector._to_db_binary(obj)


class SparseVectorLoader(Loader):

    format = Format.TEXT

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return SparseVector._from_db(data.decode('utf8'))


class SparseVectorBinaryLoader(SparseVectorLoader):

    format = Format.BINARY

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return SparseVector._from_db_binary(data)


def register_sparsevec_info(context, info):
    info.register(context)

    # add oid to anonymous class for set_types
    text_dumper = type('', (SparseVectorDumper,), {'oid': info.oid})
    binary_dumper = type('', (SparseVectorBinaryDumper,), {'oid': info.oid})

    adapters = context.adapters
    adapters.register_dumper(SparseVector, text_dumper)
    adapters.register_dumper(SparseVector, binary_dumper)
    adapters.register_loader(info.oid, SparseVectorLoader)
    adapters.register_loader(info.oid, SparseVectorBinaryLoader)
