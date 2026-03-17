
# Drop this when minimum supported version will be 3.7.
try:
    import threading
except ImportError:
    import dummy_threading as threading  # noqa: F401

import json  # noqa: F401
try:
    import orjson as json  # noqa: F811
except ImportError:
    pass

try:
    import ujson as json  # noqa: F811,F401
except ImportError:
    pass


try:
    # since tzlocal 4.0+
    # this will avoid warning for get_localzone().key
    from tzlocal import get_localzone_name

    def get_localzone_name_compat():
        try:
            return get_localzone_name()
        except Exception:
            return None
except ImportError:
    from tzlocal import get_localzone

    def get_localzone_name_compat():
        try:
            return get_localzone().key
        except AttributeError:
            return get_localzone().zone
        except Exception:
            return None
