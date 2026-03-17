class Data(object):
    def __init__(self, attributes):
        self._keys = list(attributes.keys())
        for key in attributes:
            setattr(self, key, attributes[key])

    def __str__(self):
        return "Data({0})".format(", ".join(
            "{0}={1!r}".format(key, getattr(self, key))
            for key in self._keys
        ))
    
    def __repr__(self):
        return str(self)

def data(**kwargs):
    return Data(kwargs)
