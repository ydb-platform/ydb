#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=unused-import,import-error,invalid-name
r"""Public APIs of anyconfig module.

.. versionchanged:: 0.10.2

   - Re-structured APIs and split into sub modules
   - Added type hints

.. versionchanged:: 0.9.9

   - Removed the API 'find_loader'
   - Added new APIs :func:`find` and :func:`findall` to :func:`find parsers`
     (loaders and dumpers) to suppport to find multiple parsers, and replace
     the API 'find_loader'
   - Added new APIs :func:`list_by_cid`, :func:`list_by_type` and
     :func:`list_by_extension` to list parsers by various viewpoints.

.. versionadded:: 0.9.8

   - Added new API load_plugins to [re-]load plugins

.. versionadded:: 0.9.5

   - Added pathlib support. Now all of load and dump APIs can process
     pathlib.Path object basically.
   - 'ignore_missing' keyword option for load APIs are now marked as deprecated
     and will be removed soon.
   - Allow to load data other than mapping objects for some backends such as
     JSON and YAML.

.. versionadded:: 0.8.3

   - Added ac_dict keyword option to pass dict factory (any callable like
     function or class) to make dict-like object in backend parsers.
   - Added ac_query keyword option to query data with JMESPath expression.
   - Added experimental query api to query data with JMESPath expression.
   - Removed ac_namedtuple keyword option.
   - Export :func:`merge`.
   - Stop exporting :func:`to_container` which was deprecated and removed.

.. versionadded:: 0.8.2

   - Added new API, version to provide version information.

.. versionadded:: 0.8.0

   - Removed set_loglevel API as it does not help much.
   - Added :func:`open` API to open files with appropriate open mode.
   - Added custom exception classes, :class:`UnknownProcessorTypeError` and
     :class:`UnknownFileTypeError` to express specific errors.
   - Change behavior of the API :func:`find_loader` and others to make them
     fail firt and raise exceptions (ValueError, UnknownProcessorTypeError or
     UnknownFileTypeError) as much as possible if wrong parser type for uknown
     file type was given.

.. versionadded:: 0.5.0

   - Most keyword arguments passed to APIs are now position independent.
   - Added ac_namedtuple parameter to \*load and \*dump APIs.

.. versionchanged:: 0.3

   - Replaced 'forced_type' optional argument of some public APIs with
     'ac_parser' to allow skip of config parser search by passing parser object
     previously found and instantiated.

     Also removed some optional arguments, 'ignore_missing', 'merge' and
     'marker', from definitions of some public APIs as these may not be changed
     from default in common use cases.

.. versionchanged:: 0.2

   - Now APIs :func:`find_loader`, :func:`single_load`, :func:`multi_load`,
     :func:`load` and :func:`dump` can process a file/file-like object or a
     list of file/file-like objects instead of a file path or a list of file
     paths.

.. versionadded:: 0.2

   - Export factory method (create) of anyconfig.mergeabledict.MergeableDict
"""
from __future__ import annotations

from .datatypes import MaybeDataT
from ._dump import (
    dump, dumps,
)
from ._load import (
    single_load, multi_load, load, loads,
)
from ._open import open  # pylint: disable=redefined-builtin

# Export some more APIs originally from other sub modules.
from ..backend import ParserT
from ..common import (
    InDataT, InDataExT,
    UnknownFileTypeError, UnknownParserTypeError,
    UnknownProcessorTypeError, ValidationError,
)
from ..dicts import (
    MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS, MERGE_STRATEGIES,
    merge, get, set_,
)
from ..ioinfo import (
    IOInfo, make as ioinfo_make, makes as ioinfo_makes,
)
from ..parsers import (
    load_plugins, list_types, list_by_cid, list_by_type, list_by_extension,
    findall, find, MaybeParserT,
)
from ..query import try_query
from ..schema import (
    validate, is_valid, gen_schema,
)


__version__ = "0.15.1"


def version() -> list[str]:
    """Version info.

    :return: A list of version info, [major, minor, release[, e.g. [0, 8, 2]
    """
    return __version__.split(".")


__all__ = [
    "MaybeDataT",
    "dump", "dumps",
    "single_load", "multi_load", "load", "loads",
    "open", "version",

    # anyconfig.backend
    "ParserT",

    # anyconfig.common
    "InDataT", "InDataExT",
    "UnknownFileTypeError", "UnknownParserTypeError",
    "UnknownProcessorTypeError", "ValidationError",

    # anyconfig.dicsts
    "MS_REPLACE", "MS_NO_REPLACE", "MS_DICTS", "MS_DICTS_AND_LISTS",
    "MERGE_STRATEGIES",
    "merge", "get", "set_",

    # anyconfig.ioinfo
    "IOInfo", "ioinfo_make", "ioinfo_makes",

    # anyconfig.parsers
    "load_plugins", "list_types", "list_by_cid", "list_by_type",
    "list_by_extension", "findall", "find",
    "MaybeParserT",

    # anyconfig.query
    "try_query",

    # anyconfig.validate
    "validate", "is_valid", "gen_schema",
]
