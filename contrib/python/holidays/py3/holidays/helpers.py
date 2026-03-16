#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)


def _normalize_arguments(cls, value):
    """Normalize arguments.

    :param cls:
        A type of arguments to normalize.

    :param value:
        Either a single item or an iterable of `cls` type.

    :return:
        A set created from `value` argument.

    """
    if value is None:
        return set()

    if isinstance(value, str):
        return {cls(value)}

    try:
        return {cls(v) for v in value}
    except TypeError:  # non-iterable
        return {cls(value)}


def _normalize_tuple(value):
    """Normalize tuple.

    :param data:
        Either a tuple or a tuple of tuples.

    :return:
        An unchanged object for tuple of tuples, e.g., ((JAN, 10), (DEC, 31)).
        An object put into a tuple otherwise, e.g., ((JAN, 10),).
    """
    return value if not value or isinstance(value[0], tuple) else (value,)
