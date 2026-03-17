from sys import modules as __modules

from .exceptions import UwsgiconfException as __DummyException
from .settings import FORCE_STUB as __FORCE_STUB

if False:  # pragma: nocover
    from .uwsgi_stub import *  # noqa  # Give IDEs a chance to load stub symbols.


try:  # pragma: nocover
    if __FORCE_STUB:
        raise __DummyException('uWSGI stub instruction is found in env.')

    import uwsgi

    uwsgi.is_stub = False
    """Indicates whether stub is used instead of real `uwsgi` module."""

    # The following allows proper dynamic attributes (e.g. ``env``) addressing.
    __modules[__name__] = uwsgi

except (ImportError, __DummyException):

    from . import uwsgi_stub
    __modules[__name__] = uwsgi_stub
