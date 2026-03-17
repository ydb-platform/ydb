#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=unused-import,import-error,invalid-name
"""Provides the API to load objects from given files."""
from __future__ import annotations

import typing
import warnings

from .. import ioinfo
from ..dicts import (
    convert_to as dicts_convert_to,
    merge as dicts_merge,
)
from ..parsers import find as parsers_find
from ..query import try_query
from ..schema import is_valid
from ..template import try_render
from ..utils import is_dict_like
from .datatypes import (
    ParserT,
)
from .utils import are_same_file_types

if typing.TYPE_CHECKING:
    import collections.abc

    from ..common import (
        InDataT, InDataExT,
    )


MappingT = dict[str, typing.Any]
MaybeParserOrIdOrTypeT = typing.Optional[typing.Union[str, ParserT]]


def try_to_load_schema(**options: typing.Any) -> InDataT | None:
    """Try to load a schema object for validation.

    :param options: Optional keyword arguments such as

        - ac_template: Assume configuration file may be a template file and try
          to compile it AAR if True
        - ac_context: Mapping object presents context to instantiate template
        - ac_schema: JSON schema file path to validate configuration files

    :return: Mapping object or None means some errors
    """
    ac_schema = options.get("ac_schema")
    if ac_schema is not None:
        # Try to detect the appropriate parser to load the schema data as it
        # may be different from the original config file's format, perhaps.
        options["ac_parser"] = None
        options["ac_schema"] = None  # Avoid infinite loop.
        res = load(ac_schema, **options)
        if not res or not is_dict_like(res):
            return None

        return res

    return None


def _single_load(
    ioi: ioinfo.IOInfo, *,
    ac_parser: MaybeParserOrIdOrTypeT = None,
    ac_template: bool = False,
    ac_context: MappingT | None = None,
    **options: typing.Any,
) -> InDataExT:
    """Load data from a given ``ioi``.

    :param input_:
        File path or file or file-like object or pathlib.Path object represents
        the file or a namedtuple 'anyconfig.ioinfo.IOInfo' object represents
        some input to load some data from
    :param ac_parser: Forced parser type or parser object itself
    :param ac_template:
        Assume configuration file may be a template file and try to compile it
        AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options:
        Optional keyword arguments :func:`single_load` supports except for
        ac_schema and ac_query

    :return: Mapping object
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    psr: ParserT = parsers_find(ioi, forced_type=ac_parser)
    filepath = ioi.path

    if ac_template and filepath:
        content = try_render(filepath=filepath, ctx=ac_context, **options)
        if content is not None:
            return psr.loads(content, **options)

    return psr.load(ioi, **options)


def single_load(
    input_: ioinfo.PathOrIOInfoT,
    ac_parser: MaybeParserOrIdOrTypeT = None,
    *,
    ac_template: bool = False,
    ac_context: MappingT | None = None,
    **options: typing.Any,
) -> InDataExT:
    r"""Load from single input ``input\_``.

    .. note::

       :func:`load` is a preferable alternative and this API should be used
       only if there is a need to emphasize given input 'input\_' is single
       one.

    :param input\_:
        File path or file or file-like object or pathlib.Path object represents
        the file or a namedtuple 'anyconfig.ioinfo.IOInfo' object represents
        some input to load some data from
    :param ac_parser: Forced parser type or parser object itself
    :param ac_template:
        Assume configuration file may be a template file and try to compile it
        AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options: Optional keyword arguments such as:

        - Options common in :func:`single_load`, :func:`multi_load`,
          :func:`load` and :func:`loads`:

          - ac_dict: callable (function or class) to make mapping objects from
            loaded data if the selected backend can customize that such as JSON
            which supports that with 'object_pairs_hook' option, or None. If
            this option was not given or None, dict or
            :class:`collections.OrderedDict` will be used to make result as
            mapping object depends on if ac_ordered (see below) is True and
            selected backend can keep the order of items loaded. See also
            :meth:`_container_factory` of
            :class:`anyconfig.backend.base.Parser` for more implementation
            details.

          - ac_ordered: True if you want to keep resuls ordered. Please note
            that order of items may be lost depends on the selected backend.

          - ac_schema: JSON schema file path to validate given config file
          - ac_query: JMESPath expression to query data

          - ac_parse_value: Parse given string as a value in some loaders if
            True

        - Common backend options:

          - ac_ignore_missing:
            Ignore and just return empty result if given file 'input\_' does
            not exist actually.

        - Backend specific options such as {"indent": 2} for JSON backend

    :return: Mapping object
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    ioi = ioinfo.make(input_)
    cnf = _single_load(ioi, ac_parser=ac_parser, ac_template=ac_template,
                       ac_context=ac_context, **options)
    schema = try_to_load_schema(
        ac_template=ac_template, ac_context=ac_context, **options,
    )
    if schema and not is_valid(cnf, schema, **options):
        return None

    return try_query(cnf, options.get("ac_query", False), **options)


def multi_load(
    inputs: typing.Union[  # noqa: UP007
        collections.abc.Iterable[ioinfo.PathOrIOInfoT],
        ioinfo.PathOrIOInfoT,
    ],
    ac_parser: MaybeParserOrIdOrTypeT = None,
    *,
    ac_template: bool = False,
    ac_context: MappingT | None = None,
    **options: typing.Any,
) -> InDataExT:
    r"""Load data from multiple inputs ``inputs``.

    .. note::

       :func:`load` is a preferable alternative and this API should be used
       only if there is a need to emphasize given inputs are multiple ones.

    The first argument 'inputs' may be a list of a file paths or a glob pattern
    specifying them or a pathlib.Path object represents file[s] or a namedtuple
    'anyconfig.ioinfo.IOInfo' object represents some inputs to load some data
    from.

    About glob patterns, for example, is, if a.yml, b.yml and c.yml are in the
    dir /etc/foo/conf.d/, the followings give same results::

      multi_load(["/etc/foo/conf.d/a.yml", "/etc/foo/conf.d/b.yml",
                  "/etc/foo/conf.d/c.yml", ])

      multi_load("/etc/foo/conf.d/*.yml")

    :param inputs:
        A list of file path or a glob pattern such as r'/a/b/\*.json'to list of
        files, file or file-like object or pathlib.Path object represents the
        file or a namedtuple 'anyconfig.ioinfo.IOInfo' object represents some
        inputs to load some data from
    :param ac_parser: Forced parser type or parser object
    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: Mapping object presents context to instantiate template
    :param options: Optional keyword arguments:

        - ac_dict, ac_ordered, ac_schema and ac_query are the options common in
          :func:`single_load`, :func:`multi_load`, :func:`load`: and
          :func:`loads`. See the descriptions of them in :func:`single_load`.

        - Options specific to this function and :func:`load`:

          - ac_merge (merge): Specify strategy of how to merge results loaded
            from multiple configuration files. See the doc of
            :mod:`dicts` for more details of strategies. The default
            is dicts.MS_DICTS.

        - Common backend options:

          - ignore_missing: Ignore and just return empty result if given file
            'path' does not exist.

        - Backend specific options such as {"indent": 2} for JSON backend

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    schema = try_to_load_schema(
        ac_template=ac_template, ac_context=ac_context, **options,
    )
    options["ac_schema"] = None  # Avoid to load schema more than twice.

    iois = ioinfo.makes(inputs)
    if are_same_file_types(iois):
        ac_parser = parsers_find(iois[0], forced_type=ac_parser)

    cnf = None
    ctx = dicts_convert_to({}, **options)
    if ac_context:
        ctx = ac_context.copy()

    for ioi in iois:
        cups = _single_load(
            ioi, ac_parser=ac_parser, ac_template=ac_template,
            ac_context=ctx, **options,
        )
        if cups:
            if cnf is None:
                cnf = cups

            if is_dict_like(cups):
                dicts_merge(
                    typing.cast("MappingT", cnf),
                    typing.cast("MappingT", cups),
                    **options,
                )
                dicts_merge(ctx, typing.cast("MappingT", cups), **options)
            elif len(iois) > 1:
                msg = (
                    f"Object loaded from {ioi!r} is not a mapping object and "
                    "cannot be merged with later ones will be loaded from "
                    "other inputs."
                )
                raise ValueError(msg)

    if cnf is None:
        return dicts_convert_to({}, **options)

    if schema and not is_valid(cnf, schema, **options):
        return None

    return try_query(cnf, options.get("ac_query", False), **options)


def load(
    path_specs: typing.Union[  # noqa: UP007
        collections.abc.Iterable[ioinfo.PathOrIOInfoT],
        ioinfo.PathOrIOInfoT,
    ],
    ac_parser: str | None = None,
    *,
    ac_dict: collections.abc.Callable | None = None,
    ac_template: bool = False,
    ac_context: MappingT | None = None,
    **options: typing.Any,
) -> InDataExT:
    r"""Load from a file or files specified as ``path_specs``.

    Load single or multiple config files or multiple config files specified in
    given paths pattern or pathlib.Path object represents config files or a
    namedtuple 'anyconfig.ioinfo.IOInfo' object represents some inputs.

    :param path_specs:
        A list of file path or a glob pattern such as r'/a/b/\*.json'to list of
        files, file or file-like object or pathlib.Path object represents the
        file or a namedtuple 'anyconfig.ioinfo.IOInfo' object represents some
        inputs to load some data from.
    :param ac_parser: Forced parser type or parser object
    :param ac_dict:
        callable (function or class) to make mapping object will be returned as
        a result or None. If not given or ac_dict is None, default mapping
        object used to store resutls is dict or
        :class:`collections.OrderedDict` if ac_ordered is True and selected
        backend can keep the order of items in mapping objects.

    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options:
        Optional keyword arguments. See also the description of 'options' in
        :func:`single_load` and :func:`multi_load`

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    iois = ioinfo.makes(path_specs)
    if not iois:
        msg = f"Maybe invalid input: {path_specs!r}"
        raise ValueError(msg)

    if len(iois) == 1:
        return single_load(
            iois[0], ac_parser=ac_parser, ac_dict=ac_dict,
            ac_template=ac_template, ac_context=ac_context,
            **options,
        )

    return multi_load(
        iois, ac_parser=ac_parser, ac_dict=ac_dict,
        ac_template=ac_template, ac_context=ac_context,
        **options,
    )


def loads(
    content: str,
    ac_parser: MaybeParserOrIdOrTypeT = None,
    *,
    ac_dict: collections.abc.Callable | None = None,
    ac_template: str | bool = False,
    ac_context: MappingT | None = None,
    **options: typing.Any,
) -> InDataExT:
    """Load data from a str, ``content``.

    :param content: Configuration file's content (a string)
    :param ac_parser: Forced parser type or ID or parser object
    :param ac_dict:
        callable (function or class) to make mapping object will be returned as
        a result or None. If not given or ac_dict is None, default mapping
        object used to store resutls is dict or
        :class:`collections.OrderedDict` if ac_ordered is True and selected
        backend can keep the order of items in mapping objects.
    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: Context dict to instantiate template
    :param options:
        Optional keyword arguments. See also the description of 'options' in
        :func:`single_load` function.

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError
    """
    if ac_parser is None:
        warnings.warn("ac_parser was not given but it's must to find correct "
                      "parser to load configurations from string.",
                      stacklevel=2)
        return None

    psr: ParserT = parsers_find(None, forced_type=ac_parser)
    schema = None
    ac_schema = options.get("ac_schema")
    if ac_schema is not None:
        options["ac_schema"] = None
        schema = loads(
            ac_schema, ac_parser=psr, ac_dict=ac_dict,
            ac_template=ac_template, ac_context=ac_context,
            **options,
        )

    if ac_template:
        compiled = try_render(content=content, ctx=ac_context, **options)
        if compiled is not None:
            content = compiled

    cnf = psr.loads(content, ac_dict=ac_dict, **options)
    if not is_valid(cnf, schema, **options):
        return None

    return try_query(cnf, options.get("ac_query", False), **options)
