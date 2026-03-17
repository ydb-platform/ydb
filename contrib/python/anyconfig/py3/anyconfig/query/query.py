#
# Copyright (C) 2017 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=bare-except
"""anyconfig.query module to support query data with JMESPath expressions.

Changelog:

.. versionadded:: 0.8.3

   - Added to query config data with JMESPath expression, http://jmespath.org
"""
from __future__ import annotations

import typing
import warnings

import jmespath

from ..utils import is_dict_like
if typing.TYPE_CHECKING:
    from .datatypes import MaybeJexp

    from ..common import (
        InDataExT, InDataT,
    )


def try_query(
    data: InDataExT, jexp: MaybeJexp = None,
    **options: typing.Any,
) -> InDataExT:
    """Try to query data with JMESPath expression `jexp`."""
    if jexp is None or not jexp:
        return data

    if not is_dict_like(data):  # Some primitive types like int, str.
        warnings.warn(
            "Could not query because given data is not "
            f"a mapping object (type? {type(data)}",
            stacklevel=2,
        )
        return data

    (odata, exc) = query(
        typing.cast("InDataT", data), typing.cast("str", jexp), **options,
    )
    if exc:
        raise exc

    return odata


def query(
    data: InDataT, jexp: str, **_options: typing.Any,
) -> tuple[InDataT | None, Exception | None]:
    """Filter data with given JMESPath expression.

    See also: https://github.com/jmespath/jmespath.py and http://jmespath.org.

    :param data: Target object (a dict or a dict-like object) to query
    :param jexp: a string represents JMESPath expression
    :param options: Keyword options

    :return: A tuple of query result and maybe exception if failed
    """
    exc: Exception | None = None
    try:
        pexp = jmespath.compile(jexp)
        return (pexp.search(data), exc)

    except ValueError as exc:  # jmespath.exceptions.*Error inherit from it.
        return (data, exc)
