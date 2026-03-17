# -*- coding: utf-8 -*-
import sys


if sys.version_info[0] == 2:  # pragma: no cover  # py2
    from functools32 import wraps  # noqa: F401
else:  # pragma: no cover  # py3+
    from functools import wraps  # noqa: F401


try:
    from collections.abc import Mapping  # noqa: F401  # pragma: no cover  # py3.3+
except ImportError:  # pragma: no cover
    from collections import Mapping  # noqa: F401  # py3.2 or older


if sys.version_info[0:2] <= (3, 4):  # pragma: no cover  # py3.4 or older
    from inspect import getargspec as get_function_spec  # noqa: F401
else:  # pragma: no cover  # py3.5+
    from inspect import getfullargspec as get_function_spec  # noqa: F401
