class Formatter(object):

    def validate(self, value):
        return True

    def unmarshal(self, value):
        return value

    @classmethod
    def from_callables(cls, validate=None, unmarshal=None):
        attrs = {}
        if validate is not None:
            attrs['validate'] = staticmethod(validate)
        if unmarshal is not None:
            attrs['unmarshal'] = staticmethod(unmarshal)

        klass = type('Formatter', (cls, ), attrs)
        return klass()
