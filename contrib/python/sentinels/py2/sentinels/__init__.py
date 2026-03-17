from .__version__ import __version__

try:
    # python3 renamed copy_reg to copyreg
    import copyreg
except ImportError:
    import copy_reg as copyreg

class Sentinel(object):
    _existing_instances = {}
    def __init__(self, name):
        super(Sentinel, self).__init__()
        self._name = name
        self._existing_instances[self._name] = self
    def __repr__(self):
        return "<{0}>".format(self._name)
    def __getnewargs__(self):
        return (self._name,)
    def __new__(cls, name, obj_id=None): # obj_id is for compatibility with previous versions
        existing_instance = cls._existing_instances.get(name)
        if existing_instance is not None:
            return existing_instance
        return super(Sentinel, cls).__new__(cls)

def _sentinel_unpickler(name, obj_id=None): # obj_id is for compat. with prev. versions
    if name in Sentinel._existing_instances:
        return Sentinel._existing_instances[name]
    return Sentinel(name)
def _sentinel_pickler(sentinel):
    return _sentinel_unpickler, sentinel.__getnewargs__()


copyreg.pickle(Sentinel, _sentinel_pickler, _sentinel_unpickler)

NOTHING = Sentinel('NOTHING')
