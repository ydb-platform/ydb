#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=consider-using-with, unspecified-encoding
"""Abstract and basic dumpes."""
from __future__ import annotations

import io
import pathlib
import typing

from ... import ioinfo, utils
from .utils import (
    ensure_outdir_exists, not_implemented,
)

if typing.TYPE_CHECKING:
    from .datatypes import (
        InDataExT, IoiT, PathOrStrT,
    )


_ENCODING = ioinfo.get_encoding()


class DumperMixin:
    """Mixin class to dump data.

    Inherited classes must implement the following methods.

    - :meth:`dump_to_string`: Dump config as a string
    - :meth:`dump_to_stream`: Dump config to a file or file-like object
    - :meth:`dump_to_path`: Dump config to a file of given path

    Member variables:

    - _dump_opts: Backend specific options on dump
    - _open_write_mode: Backend option to specify write mode passed to open()
    """

    _dump_opts: tuple[str, ...] = ()
    _open_write_mode: typing.ClassVar[str] = "w"

    def wopen(
        self, filepath: PathOrStrT, **options: typing.Any,
    ) -> typing.IO:
        """Open file ``filepath`` with the write mode ``_open_write_mode``."""
        if "encoding" not in options and self._open_write_mode == "w":
            options["encoding"] = _ENCODING

        return pathlib.Path(filepath).open(
            self._open_write_mode, **options,
        )

    def dump_to_string(
        self, cnf: InDataExT, **options: typing.Any,
    ) -> str:
        """Dump config 'cnf' to a string.

        :param cnf: Configuration data to dump
        :param options: optional keyword parameters to be sanitized :: dict

        :return: string represents the configuration
        """
        not_implemented(self, cnf, **options)
        return ""

    def dump_to_path(
        self, cnf: InDataExT, filepath: PathOrStrT,
        **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to a file 'filepath'.

        :param cnf: Configuration data to dump
        :param filepath: Config file path
        :param options: optional keyword parameters to be sanitized :: dict
        """
        not_implemented(self, cnf, filepath, **options)

    def dump_to_stream(
        self, cnf: InDataExT, stream: typing.IO,
        **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to a file-like object 'stream'.

        TODO: How to process socket objects same as file objects ?

        :param cnf: Configuration data to dump
        :param stream:  Config file or file like object
        :param options: optional keyword parameters to be sanitized :: dict
        """
        not_implemented(self, cnf, stream, **options)

    def dumps(
        self, cnf: InDataExT, **options: typing.Any,
    ) -> str:
        """Dump config 'cnf' to a string.

        :param cnf: Configuration data to dump
        :param options: optional keyword parameters to be sanitized :: dict

        :return: string represents the configuration
        """
        options = utils.filter_options(self._dump_opts, options)
        return self.dump_to_string(cnf, **options)

    def dump(
        self, cnf: InDataExT, ioi: IoiT, **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to output object of which 'ioi' referring.

        :param cnf: Configuration data to dump
        :param ioi:
            an 'anyconfig.cmmon.IOInfo' namedtuple object provides various
            info of input object to load data from

        :param options: optional keyword parameters to be sanitized :: dict
        :raises IOError, OSError, AttributeError: When dump failed.
        """
        options = utils.filter_options(self._dump_opts, options)

        if ioinfo.is_stream(ioi):
            self.dump_to_stream(
                cnf, typing.cast("typing.IO", ioi.src), **options,
            )
        else:
            ensure_outdir_exists(ioi.path)
            self.dump_to_path(cnf, ioi.path, **options)


class BinaryDumperMixin(DumperMixin):
    """Mixin class to dump binary (byte string) configuration data."""

    _open_write_mode: typing.ClassVar[str] = "wb"


class ToStringDumperMixin(DumperMixin):
    """Abstract config parser provides the followings.

    - a method to dump configuration to a file or file-like object (stream) and
      a file of given path to help implement parser of which backend lacks of
      such functions.

    Parser classes inherit this class have to override the method
    :meth:`dump_to_string` at least.
    """

    def dump_to_path(
        self, cnf: InDataExT, filepath: PathOrStrT,
        **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to a file 'filepath'.

        :param cnf: Configuration data to dump
        :param filepath: Config file path
        :param options: optional keyword parameters to be sanitized :: dict
        """
        with self.wopen(filepath) as out:
            out.write(self.dump_to_string(cnf, **options))

    def dump_to_stream(
        self, cnf: InDataExT, stream: typing.IO,
        **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to a file-like object 'stream'.

        TODO: How to process socket objects same as file objects ?

        :param cnf: Configuration data to dump
        :param stream:  Config file or file like object
        :param options: optional keyword parameters to be sanitized :: dict
        """
        stream.write(self.dump_to_string(cnf, **options))


class ToStreamDumperMixin(DumperMixin):
    """Abstract config parser provides the following methods.

    - to dump configuration to a string content or a file of given path to help
      implement parser of which backend lacks of such functions.

    Parser classes inherit this class have to override the method
    :meth:`dump_to_stream` at least.
    """

    def dump_to_string(
        self, cnf: InDataExT, **options: typing.Any,
    ) -> str:
        """Dump config 'cnf' to a string.

        :param cnf: Configuration data to dump
        :param options: optional keyword parameters to be sanitized :: dict

        :return: Dict-like object holding config parameters
        """
        stream = io.StringIO()
        self.dump_to_stream(cnf, stream, **options)
        return stream.getvalue()

    def dump_to_path(
        self, cnf: InDataExT, filepath: PathOrStrT,
        **options: typing.Any,
    ) -> None:
        """Dump config 'cnf' to a file 'filepath`.

        :param cnf: Configuration data to dump
        :param filepath: Config file path
        :param options: optional keyword parameters to be sanitized :: dict
        """
        with self.wopen(filepath) as out:
            self.dump_to_stream(cnf, out, **options)
