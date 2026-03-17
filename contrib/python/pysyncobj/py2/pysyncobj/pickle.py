import sys

is_py3 = sys.version_info >= (3, 0)

if is_py3:
    import pickle
    from struct import unpack

    # python3 sometimes failes to unpickle data pickled by python2, it happens
    # because it is trying to decode binary data into the string and fails.
    # UnicodeDecodeError exception is raised in this case. Instead of simply
    # giving up we will retry decoding with with "slow" _Unpickler implemented
    # in pure python with overriden following methods.
    # The main idea is - treat object as binary if the decoding has failed.
    # Such approach will not affect performance when we run all nodes with
    # the same python version, beacuse it will never retry.
    def _load_short_binstring(self):
        len = ord(self.read(1))
        data = self.read(len)
        try:
            data = str(data, self.encoding, self.errors)
        except:
            pass
        self.append(data)

    def _load_binstring(self):
        len, = unpack('<i', self.read(4))
        if len < 0:
            raise pickle.UnpicklingError("BINSTRING pickle has negative byte count")
        data = self.read(len)
        try:
            data = str(data, self.encoding, self.errors)
        except:
            pass
        self.append(data)

    pickle._Unpickler.dispatch[pickle.SHORT_BINSTRING[0]] = _load_short_binstring
    pickle._Unpickler.dispatch[pickle.BINSTRING[0]] = _load_binstring
else:
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

__protocol = 2


def to_bytes(data):
    return data if isinstance(data, bytes) else data.encode('utf-8')


def load(file):
    try:
        return pickle.load(file)
    except:
        if is_py3:
            file.seek(0, 0)
            return pickle._load(file)
        raise


def loads(data):
    data = to_bytes(data)
    try:
        return pickle.loads(data)
    except:
        if is_py3:
            return pickle._loads(data)
        raise


def dump(obj, file, protocol=None):
    pickle.dump(obj, file, __protocol)


def dumps(obj, protocol=None):
    return pickle.dumps(obj, __protocol)
