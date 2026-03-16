#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
#  pylint: disable=deprecated-method
r"""A backend module to load and dump INI files.

- Format to support: INI or INI like ones
- Requirements: The following standard module which should be available always.

  - ConfigParser in python 2 standard library:
    https://docs.python.org/2.7/library/configparser.html

  - configparser in python 3 standard library:
    https://docs.python.org/3/library/configparser.html

- Development Status :: 4 - Beta
- Limitations: It cannot process nested configuration dicts correctly due to
  the limitation of the module and the format ifself.

- Special options:

  - Use 'ac_parse_value' boolean keyword option if you want to parse values by
    custom parser, anyconfig.backend.ini.parse.

Changelog:

.. versionchanged:: 0.14.0 Re-organized to allow supporting other parser

.. versionchanged:: 0.3

   - Introduce 'ac_parse_value' keyword option to switch behaviors, same as
     original configparser and rich backend parsing each parameter values.
"""
from __future__ import annotations

import configparser
import re
import typing

from ... import parser, utils
from .. import base

if typing.TYPE_CHECKING:
    import collections.abc


_SEP = ","
try:
    DEFAULTSECT: str = configparser.DEFAULTSECT
except AttributeError:
    DEFAULTSECT: str = "DEFAULT"  # type: ignore[no-redef]


_QUOTED_RE: re.Pattern = re.compile(
    r"^("
    r'".*"'
    r"|"
    r"'.*'"
    r")$",
)


def parse(
    val_s: str, sep: str = _SEP, quoted_re: re.Pattern = _QUOTED_RE,
) -> typing.Any:
    """Parse expression.

    FIXME: May be too naive implementation.

    :param val_s: A string represents some value to parse
    :param sep: separator between values
    """
    if quoted_re.match(val_s):
        return val_s[1:-1]

    if sep in val_s:
        return [
            parser.parse(typing.cast("str", x))
            for x in parser.parse_list(val_s)
        ]

    return parser.parse(val_s)


def _to_s(val: typing.Any, sep: str = ", ") -> str:
    """Convert any object to string.

    :param val: An object
    :param sep: separator between values
    """
    if utils.is_iterable(val):
        return sep.join(str(x) for x in val)

    return str(val)


def parsed_items(
    items: collections.abc.Iterable[tuple[str, typing.Any]],
    sep: str = _SEP, **options: typing.Any,
) -> collections.abc.Iterator[tuple[str, typing.Any]]:
    """Parse an iterable of items.

    :param items: List of pairs, [(key, value)], or generator yields pairs
    :param sep: Seprator string
    :return: Generator to yield (key, value) pair of 'dic'
    """
    __parse = parse if options.get("ac_parse_value") else utils.noop
    for key, val in items:
        yield (key, __parse(val, sep))  # type: ignore[operator]


def _make_parser(
    **kwargs: typing.Any,
) -> tuple[dict[str, typing.Any], configparser.ConfigParser]:
    """Make an instance of configparser.ConfigParser."""
    # Optional arguments for configparser.ConfigParser{,readfp}
    kwargs_0 = utils.filter_options(
        ("defaults", "dict_type", "allow_no_value", "strict"), kwargs,
    )
    kwargs_1 = utils.filter_options(("filename", ), kwargs)

    try:
        psr = configparser.ConfigParser(**kwargs_0)
    except TypeError:
        # .. note::
        #    It seems ConfigParser.*ConfigParser in python 2.6 does not support
        #    'allow_no_value' option parameter, and TypeError will be thrown.
        kwargs_0 = utils.filter_options(("defaults", "dict_type"), kwargs)
        psr = configparser.ConfigParser(**kwargs_0)

    return (kwargs_1, psr)


def _load(
    stream: typing.IO, container: base.GenContainerT,
    sep: str = _SEP, dkey: str = DEFAULTSECT,
    **kwargs: typing.Any,
) -> base.InDataT:
    """Load data from ``stream`` of which file should be in INI format.

    :param stream: File or file-like object provides ini-style conf
    :param container: any callable to make container
    :param sep: Seprator string
    :param dkey: Default section name

    :return: Dict or dict-like object represents config values
    """
    (kwargs_1, psr) = _make_parser(**kwargs)
    psr.read_file(stream, **kwargs_1)

    cnf = container()
    kwargs["sep"] = sep

    defaults = psr.defaults()
    if defaults:
        cnf[dkey] = container(parsed_items(defaults.items(), **kwargs))

    for sect in psr.sections():
        cnf[sect] = container(parsed_items(psr.items(sect), **kwargs))

    return cnf


def _dumps_itr(
    cnf: dict[str, typing.Any], dkey: str = DEFAULTSECT,
) -> collections.abc.Iterator[str]:
    """Dump data iterably.

    :param cnf: Configuration data to dump
    """
    for sect, params in cnf.items():
        yield f"[{sect}]"

        for key, val in params.items():
            if sect != dkey and dkey in cnf and cnf[dkey].get(key) == val:
                continue  # It should be in [DEFAULT] section.

            yield f"{key!s} = {_to_s(val)}"

        yield ""  # it will be a separator between each sections.


def _dumps(
    cnf: dict[str, typing.Any], **_kwargs: typing.Any,
) -> str:
    """Dump data as a str.

    :param cnf: Configuration data to dump
    :param _kwargs: optional keyword parameters to be sanitized :: dict

    :return: String representation of 'cnf' object in INI format
    """
    # python looks converting \n to \r\n so use \n instead of os.linesep.
    return "\n".join(line for line in _dumps_itr(cnf))


class Parser(base.Parser, base.FromStreamLoaderMixin,
             base.ToStringDumperMixin):
    """Ini config files parser."""

    _cid: typing.ClassVar[str] = "ini.configparser"
    _type: typing.ClassVar[str] = "ini"
    _extensions: tuple[str, ...] = ("ini", )
    _load_opts: tuple[str, ...] = (
        "defaults", "dict_type", "allow_no_value", "filename",
        "ac_parse_value", "strict",
    )
    _dict_opts: tuple[str, ...] = ("dict_type", )

    dump_to_string = base.to_method(_dumps)
    load_from_stream = base.to_method(_load)
