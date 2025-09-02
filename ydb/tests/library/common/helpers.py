#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yatest.common


def unpack_list(list_of_tuples_of_tuples_and_values):
    """
    >>> unpack_list([((1, 2), 3), ((5, 6), 7)])
    [(1, 2, 3), (5, 6, 7)]
    >>> unpack_list([((1, 2, 3), 4), ((5, 6), (7, 8))])
    [(1, 2, 3, 4), (5, 6, 7, 8)]
    >>> unpack_list(unpack_list([((1, 2, 3), 4), ((5, 6), (7, 8))]))
    [(1, 2, 3, 4), (5, 6, 7, 8)]

    Only one level of nesting gets unpacked
    >>> unpack_list([(1, ), (2, 3), (4, 5, (6, (7, (8, ), 9), 10))])
    [(1,), (2, 3), (4, 5, 6, (7, (8,), 9), 10)]

    More unpacking, means more levels will be unpacked
    >>> unpack_list(unpack_list([(1, ), (2, 3), (4, 5, (6, (7, (8, ), 9), 10))]))
    [(1,), (2, 3), (4, 5, 6, 7, (8,), 9, 10)]

    More unpacking, means more levels will be unpacked
    >>> unpack_list(unpack_list(unpack_list([(1, ), (2, 3), (4, 5, (6, (7, (8, ), 9), 10))])))
    [(1,), (2, 3), (4, 5, 6, 7, 8, 9, 10)]
    >>> unpack_list(unpack_list(unpack_list(unpack_list([(1, ), (2, 3), (4, 5, (6, (7, (8, ), 9), 10))]))))
    [(1,), (2, 3), (4, 5, 6, 7, 8, 9, 10)]
    """
    res = []
    for tuple_of_tuples_and_values in list_of_tuples_of_tuples_and_values:
        unpacked_tuple = []
        for x in tuple_of_tuples_and_values:
            if isinstance(x, tuple):
                unpacked_tuple += list(x)
            else:
                unpacked_tuple += [x]
        res.append(tuple(unpacked_tuple))
    return res


def wrap_in_list(item):
    """
    >>> wrap_in_list(1)
    [1]
    >>> wrap_in_list([1])
    [1]
    >>> wrap_in_list([1, 2])
    [1, 2]

    Beware the None case and other alike
    >>> wrap_in_list(None)
    [None]
    >>> wrap_in_list('')
    ['']
    >>> wrap_in_list(())
    [()]

    :return: list of items
    """
    if isinstance(item, list):
        return item
    else:
        return [item]


def plain_or_under_sanitizer(plain, sanitized):
    """
    Meant to be used in test code for constants (timeouts, etc)
    See also arcadia/util/system/sanitizers.h

    :return: plain if no sanitizer enabled or sanitized otherwise
    """
    return plain if not yatest.common.context.sanitize else sanitized
