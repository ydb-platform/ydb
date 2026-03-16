from psycopg.adapt import Dumper, Loader
from psycopg.postgres import adapters


class Inet(str):
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)


class Macaddr(Inet):
    pass


class Macaddr8(Inet):
    pass


class _MacaddrDumper(Dumper):
    oid = adapters.types['macaddr'].oid

    def dump(self, obj):
        return str(obj).encode()


class _MacaddrLoader(Loader):
    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return Macaddr(data.decode())


class _Macaddr8Dumper(Dumper):
    oid = adapters.types['macaddr8'].oid

    def dump(self, obj):
        return str(obj).encode()


class _Macaddr8Loader(Loader):
    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return Macaddr8(data.decode())


adapters.register_loader('macaddr', _MacaddrLoader)
adapters.register_loader('macaddr8', _Macaddr8Loader)
adapters.register_dumper(Macaddr, _MacaddrDumper)
adapters.register_dumper(Macaddr8, _Macaddr8Dumper)
