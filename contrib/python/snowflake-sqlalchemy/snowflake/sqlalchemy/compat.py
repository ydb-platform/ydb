#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

import functools
from typing import Callable

from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import util

string_types = (str,)
returns_unicode = util.symbol("RETURNS_UNICODE")

IS_VERSION_20 = tuple(int(v) for v in SA_VERSION.split(".")) >= (2, 0, 0)


def args_reducer(positions_to_drop: tuple):
    """Removes args at positions provided in tuple positions_to_drop.

    For example tuple (3, 5) will remove items at third and fifth position.
    Keep in mind that on class methods first postion is cls or self.
    """

    def fn_wrapper(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args):
            reduced_args = args
            if not IS_VERSION_20:
                reduced_args = tuple(
                    arg for idx, arg in enumerate(args) if idx not in positions_to_drop
                )
            fn(*reduced_args)

        return wrapper

    return fn_wrapper
