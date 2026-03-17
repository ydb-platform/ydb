#
# Copyright (C) 2017 Satoru SATOH <ssato redhat.com>
# License: MIT
#
r"""anyconfig.query module to support query data with JMESPath expressions.

Changelog:

.. versionadded:: 0.8.3

   - Added to query config data with JMESPath expression, http://jmespath.org
"""
from __future__ import absolute_import
try:
    import jmespath
except ImportError:
    pass


def query(data, jexp, **options):
    """
    Filter data with given JMESPath expression.

    See also: https://github.com/jmespath/jmespath.py and http://jmespath.org.

    :param data: Target object (a dict or a dict-like object) to query
    :param jexp: a string represents JMESPath expression
    :param options: Keyword optios

    :return: A tuple of query result and maybe exception if failed
    """
    exc = None
    try:
        pexp = jmespath.compile(jexp)
        return (pexp.search(data), exc)

    except ValueError as exc:  # jmespath.exceptions.*Error inherit from it.
        return (data, exc)

    except (NameError, AttributeError):
        raise ValueError("Required 'jmespath' module is not available.")

    return (None, exc)  # It should not reach here.

# vim:sw=4:ts=4:et:
