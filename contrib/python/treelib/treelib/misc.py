#!/usr/bin/env python
# Copyright (C) 2011
# Brett Alistair Kromkamp - brettkromkamp@gmail.com
# Copyright (C) 2012-2025
# Xiaming Chen - chenxm35@gmail.com
# and other contributors.
# All rights reserved.
import functools
from warnings import simplefilter, warn


def deprecated(alias):
    def real_deco(func):
        """This is a decorator which can be used to mark functions
        as deprecated. It will result in a warning being emmitted
        when the function is used.
        Derived from answer by Leando: https://stackoverflow.com/a/30253848
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            simplefilter("always", DeprecationWarning)  # turn off filter
            warn(
                'Call to deprecated function "{}"; use "{}" instead.'.format(func.__name__, alias),
                category=DeprecationWarning,
                stacklevel=2,
            )
            simplefilter("default", DeprecationWarning)  # reset filter
            return func(*args, **kwargs)

        return wrapper

    return real_deco
