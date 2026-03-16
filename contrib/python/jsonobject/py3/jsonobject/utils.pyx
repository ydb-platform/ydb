from jsonobject.exceptions import BadValueError


def check_type(obj, item_type, message):
    if obj is None:
        return item_type()
    elif not isinstance(obj, item_type):
        raise BadValueError('{}. Found object of type: {}'.format(message, type(obj)))
    else:
        return obj


class SimpleDict(dict):
    """
    Re-implements destructive methods of dict
    to use only setitem and getitem and delitem
    """
    def update(self, E=None, **F):
        for dct in (E, F):
            if dct:
                for key, value in dct.items():
                    self[key] = value

    def clear(self):
        for key in list(self.keys()):
            del self[key]

    def pop(self, key, *args):
        if len(args) > 1:
            raise TypeError('pop expected at most 2 arguments, got 3')
        try:
            val = self[key]
            del self[key]
            return val
        except KeyError:
            try:
                return args[0]
            except IndexError:
                raise KeyError(key)

    def popitem(self):
        try:
            arbitrary_key = list(self.keys())[0]
        except IndexError:
            raise KeyError('popitem(): dictionary is empty')
        val = self[arbitrary_key]
        del self[arbitrary_key]
        return (arbitrary_key, val)

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default
