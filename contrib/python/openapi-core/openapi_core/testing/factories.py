class FactoryClassMock(object):

    _instances = {}

    def __new__(cls, obj):
        if obj not in cls._instances:
            cls._instances[obj] = object.__new__(cls)
        return cls._instances[obj]

    def __init__(self, obj):
        self.obj = obj
