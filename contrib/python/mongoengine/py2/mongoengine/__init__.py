# Import submodules so that we can expose their __all__
from mongoengine import connection
from mongoengine import document
from mongoengine import errors
from mongoengine import fields
from mongoengine import queryset
from mongoengine import signals

# Import everything from each submodule so that it can be accessed via
# mongoengine, e.g. instead of `from mongoengine.connection import connect`,
# users can simply use `from mongoengine import connect`, or even
# `from mongoengine import *` and then `connect('testdb')`.
from mongoengine.connection import *
from mongoengine.document import *
from mongoengine.errors import *
from mongoengine.fields import *
from mongoengine.queryset import *
from mongoengine.signals import *


__all__ = (list(document.__all__) + list(fields.__all__) +
           list(connection.__all__) + list(queryset.__all__) +
           list(signals.__all__) + list(errors.__all__))


VERSION = (0, 17, 0)


def get_version():
    """Return the VERSION as a string, e.g. for VERSION == (0, 10, 7),
    return '0.10.7'.
    """
    return '.'.join(map(str, VERSION))


__version__ = get_version()
