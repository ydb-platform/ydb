#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""Abstract implementation of backend modules.

Backend module must implement a parser class inherits :class:`Parser` or its
children classes of this module and override all or some of the methods as
needed:

  - :meth:`load_from_string`: Load config from string
  - :meth:`load_from_stream`: Load config from a file or file-like object
  - :meth:`load_from_path`: Load config from file of given path
  - :meth:`dump_to_string`: Dump config as a string
  - :meth:`dump_to_stream`: Dump config to a file or file-like object
  - :meth:`dump_to_path`: Dump config to a file of given path
"""
from __future__ import annotations

import collections.abc
import typing

from ...models import processor
from ...utils import is_dict_like
from .datatypes import (
    InDataExT, GenContainerT,
)
from .dumpers import (
    DumperMixin, ToStringDumperMixin, ToStreamDumperMixin,
)
from .loaders import (
    LoaderMixin, FromStringLoaderMixin, FromStreamLoaderMixin,
)


class Parser(LoaderMixin, DumperMixin, processor.Processor):
    """Abstract parser to provide basic implementation.

    The following members will be expected to be overridden.

    - _type: Parser type indicate which format it supports
    - _priority: Priority to select it if there are other parsers of same type
    - _extensions: File extensions of formats it supports
    - _open_flags: Opening flags to read and write files

    .. seealso:: the doc of :class:`anyconfig.models.processor.Processor`
    """

    _cid = "base"


class StringParser(Parser, FromStringLoaderMixin, ToStringDumperMixin):
    """Abstract parser based on the following methods.

    - :meth:`load_from_string`
    - :meth:`dump_to_string`.

    Parser classes inherit this class must define these methods.
    """


class StreamParser(Parser, FromStreamLoaderMixin, ToStreamDumperMixin):
    """Abstract parser based on the following methods.

    - :meth:`load_from_stream`
    - :meth:`dump_to_stream`.

    Parser classes inherit this class must define these methods.
    """


LoadFnT = collections.abc.Callable[..., InDataExT]
DumpFnT = collections.abc.Callable[..., str]


def load_with_fn(
    load_fn: LoadFnT | None,
    content_or_strm: str | bytes | typing.IO,
    container: GenContainerT, *,
    allow_primitives: bool = False,
    **options: dict[str, typing.Any],
) -> InDataExT:
    """Load data from given string or stream 'content_or_strm'.

    :param load_fn: Callable to load data
    :param content_or_strm: data content or stream provides it
    :param container: callble to make a container object
    :param allow_primitives:
        True if the parser.load* may return objects of primitive data types
        other than mapping types such like JSON parser
    :param options: keyword options passed to 'load_fn'

    :return: container object holding data
    """
    if load_fn is None:
        msg = "The first argument 'load_fn' must be a callable!"
        raise TypeError(msg)

    ret = load_fn(content_or_strm, **options)
    if is_dict_like(ret):
        return container() if (ret is None or not ret) else container(ret)

    return ret if allow_primitives else container(ret)


def dump_with_fn(
    dump_fn: DumpFnT | None,
    data: InDataExT, stream: typing.IO | None,
    **options: dict[str, typing.Any],
) -> str:
    """Dump 'data' to a string.

    If 'stream' is None, or dump 'data' to a file or file-like object 'stream'.

    :param dump_fn: Callable to dump data
    :param data: Data to dump
    :param stream:  File or file like object or None
    :param options: optional keyword parameters

    :return: String represents data if stream is None or None
    """
    if dump_fn is None:
        msg = "The first argument 'dump_fn' must be a callable!"
        raise TypeError(msg)

    if stream is None:
        return dump_fn(data, **options)

    return dump_fn(data, stream, **options)


class StringStreamFnParser(Parser, FromStreamLoaderMixin, ToStreamDumperMixin):
    """Abstract parser utilizes load and dump functions.

    Each backend module should provide functions like json.load{,s} and
    json.dump{,s} in JSON backend.

    Parser classes inherit this class must define the followings.

    - _load_from_string_fn: Callable to load data from string
    - _load_from_stream_fn: Callable to load data from stream (file object)
    - _dump_to_string_fn: Callable to dump data to string
    - _dump_to_stream_fn: Callable to dump data to stream (file object)

    .. note::
       Callables have to be wrapped with :func:`to_method` to make 'self'
       passed to the methods created from them ignoring it.

    :seealso: :class:`anyconfig.backend.json.Parser`
    """

    _load_from_string_fn: LoadFnT | None = None
    _load_from_stream_fn: LoadFnT | None = None
    _dump_to_string_fn: DumpFnT | None = None
    _dump_to_stream_fn: DumpFnT | None = None

    def load_from_string(
        self, content: str | bytes, container: GenContainerT,
        **options: dict[str, typing.Any],
    ) -> InDataExT:
        """Load configuration data from given string 'content'.

        :param content: Configuration string
        :param container: callble to make a container object
        :param options: keyword options passed to '_load_from_string_fn'

        :return: container object holding the configuration data
        """
        return load_with_fn(
            self._load_from_string_fn, content, container,
            allow_primitives=self.allow_primitives(),
            **options,
        )

    def load_from_stream(
        self, stream: typing.IO, container: GenContainerT,
        **options: dict[str, typing.Any],
    ) -> InDataExT:
        """Load data from given stream 'stream'.

        :param stream: Stream provides configuration data
        :param container: callble to make a container object
        :param options: keyword options passed to '_load_from_stream_fn'

        :return: container object holding the configuration data
        """
        return load_with_fn(
            self._load_from_stream_fn, stream, container,
            allow_primitives=self.allow_primitives(),
            **options,
        )

    def dump_to_string(
        self, cnf: InDataExT, **options: dict[str, typing.Any],
    ) -> str:
        """Dump config 'cnf' to a string.

        :param cnf: Configuration data to dump
        :param options: optional keyword parameters to be sanitized :: dict

        :return: string represents the configuration
        """
        return dump_with_fn(self._dump_to_string_fn, cnf, None,
                            **options)

    def dump_to_stream(
        self, cnf: InDataExT, stream: typing.IO,
        **options: dict[str, typing.Any],
    ) -> None:
        """Dump config 'cnf' to a file-like object 'stream'.

        TODO: How to process socket objects same as file objects ?

        :param cnf: Configuration data to dump
        :param stream:  Config file or file like object
        :param options: optional keyword parameters to be sanitized :: dict
        """
        dump_with_fn(self._dump_to_stream_fn, cnf, stream, **options)
