import etcd3.etcdrpc as etcdrpc
import etcd3.utils as utils

_OPERATORS = {
    etcdrpc.Compare.EQUAL: "==",
    etcdrpc.Compare.NOT_EQUAL: "!=",
    etcdrpc.Compare.LESS: "<",
    etcdrpc.Compare.GREATER: ">"
}


class BaseCompare(object):
    def __init__(self, key, range_end=None):
        self.key = key
        self.range_end = range_end
        self.value = None
        self.op = None

    # TODO check other is of correct type for compare
    # Version, Mod and Create can only be ints
    def __eq__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.EQUAL
        return self

    def __ne__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.NOT_EQUAL
        return self

    def __lt__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.LESS
        return self

    def __gt__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.GREATER
        return self

    def __repr__(self):
        if self.range_end is None:
            keys = self.key
        else:
            keys = "[{}, {})".format(self.key, self.range_end)
        return "{}: {} {} '{}'".format(self.__class__, keys,
                                       _OPERATORS.get(self.op),
                                       self.value)

    def build_message(self):
        compare = etcdrpc.Compare()
        compare.key = utils.to_bytes(self.key)
        if self.range_end is not None:
            compare.range_end = utils.to_bytes(self.range_end)

        if self.op is None:
            raise ValueError('op must be one of =, !=, < or >')

        compare.result = self.op

        self.build_compare(compare)
        return compare


class Value(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.VALUE
        compare.value = utils.to_bytes(self.value)


class Version(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.VERSION
        compare.version = int(self.value)


class Create(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.CREATE
        compare.create_revision = int(self.value)


class Mod(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.MOD
        compare.mod_revision = int(self.value)


class Put(object):
    def __init__(self, key, value, lease=None, prev_kv=False):
        self.key = key
        self.value = value
        self.lease = lease
        self.prev_kv = prev_kv


class Get(object):
    def __init__(self, key, range_end=None):
        self.key = key
        self.range_end = range_end


class Delete(object):
    def __init__(self, key, range_end=None, prev_kv=False):
        self.key = key
        self.range_end = range_end
        self.prev_kv = prev_kv


class Txn(object):
    def __init__(self, compare, success=None, failure=None):
        self.compare = compare
        self.success = success
        self.failure = failure
