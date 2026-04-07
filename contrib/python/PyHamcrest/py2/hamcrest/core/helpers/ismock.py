MOCKTYPES = ()
try:
    from mock import Mock
    MOCKTYPES += (Mock,)
except ImportError:
    pass
try:
    from unittest.mock import Mock
    MOCKTYPES += (Mock,)
except ImportError:
    pass


def ismock(obj):
    return isinstance(obj, MOCKTYPES)
