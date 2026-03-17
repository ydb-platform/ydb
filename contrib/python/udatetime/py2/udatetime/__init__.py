# -*- coding: utf-8 -*-
try:
    import __pypy__
except ImportError:
    __pypy__ = None

if __pypy__:
    from udatetime._pure import (
        utcnow,
        now,
        from_rfc3339_string as from_string,
        to_rfc3339_string as to_string,
        utcnow_to_string,
        now_to_string,
        from_timestamp as fromtimestamp,
        from_utctimestamp as utcfromtimestamp,
        TZFixedOffset
    )
else:
    from udatetime.rfc3339 import (
        utcnow,
        now,
        from_rfc3339_string as from_string,
        to_rfc3339_string as to_string,
        utcnow_to_string,
        now_to_string,
        from_timestamp as fromtimestamp,
        from_utctimestamp as utcfromtimestamp,
        TZFixedOffset
    )

__all__ = [
    'utcnow', 'now', 'from_string', 'to_string', 'utcnow_to_string',
    'now_to_string', 'fromtimestamp', 'utcfromtimestamp', 'TZFixedOffset'
]
