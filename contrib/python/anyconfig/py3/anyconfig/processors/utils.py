#
# Copyright (C) 2018 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=unidiomatic-typecheck
#
# TODO(ssato): #189 fix the mypy error, type-var.
# mypy: disable-error-code=type-var
"""Utility functions for anyconfig.processors."""
from __future__ import annotations

import contextlib
import operator
import typing
import warnings

import importlib.metadata

from .. import (
    common, ioinfo, models, utils,
)

if typing.TYPE_CHECKING:
    import collections.abc

    from .datatypes import (
        ProcT, ProcsT, ProcClsT, MaybeProcT,
    )


def sort_by_prio(prs: collections.abc.Iterable[ProcT]) -> ProcsT:
    """Sort an iterable of processor classes by each priority.

    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: Sambe as above but sorted by priority
    """
    return sorted(prs, key=operator.methodcaller("priority"), reverse=True)


def select_by_key(
    items: collections.abc.Iterable[
        tuple[tuple[str, ...], typing.Any]
    ],
    sort_fn: collections.abc.Callable[..., typing.Any] = sorted,
) -> list[tuple[str, list[typing.Any]]]:
    """Select items from ``items`` by key.

    :param items: A list of tuples of keys and values, [([key], val)]
    :return: A list of tuples of key and values, [(key, [val])]

    >>> select_by_key([(["a", "aaa"], 1), (["b", "bb"], 2), (["a"], 3)])
    [('a', [1, 3]), ('aaa', [1]), ('b', [2]), ('bb', [2])]
    """
    itr = utils.concat(((k, v) for k in ks) for ks, v in items)
    return [
        (k, sort_fn(t[1] for t in g))
        for k, g in utils.groupby(itr, operator.itemgetter(0))
    ]


def list_by_x(
    prs: collections.abc.Iterable[ProcT], key: str,
) -> list[tuple[str, ProcsT]]:
    """List items by the factor 'x'.

    :param key: Grouping key, 'type' or 'extensions'
    :return:
        A list of :class:`Processor` or its children classes grouped by
        given 'item', [(cid, [:class:`Processor`)]] by default
    """
    if key == "type":
        kfn = operator.methodcaller(key)
        res = sorted(((k, sort_by_prio(g)) for k, g
                      in utils.groupby(prs, kfn)),
                     key=operator.itemgetter(0))

    elif key == "extensions":
        res = select_by_key(
            ((p.extensions(), p) for p in prs), sort_fn=sort_by_prio,
        )
    else:
        msg = f"Argument 'key' must be 'type' or 'extensions' [{key}]"
        raise ValueError(msg)

    return res


def findall_with_pred(
    predicate: collections.abc.Callable[..., bool], prs: ProcsT,
) -> ProcsT:
    """Find all of the items match with given predicates.

    :param predicate: any callable to filter results
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: A list of appropriate processor classes or []
    """
    return sorted((p for p in prs if predicate(p)),
                  key=operator.methodcaller("priority"), reverse=True)


def maybe_processor(
    type_or_id: ProcT | ProcClsT,
    cls: ProcClsT = models.processor.Processor,
) -> ProcT | None:
    """Try to get the processor.

    :param type_or_id:
        Type of the data to process or ID of the processor class or
        :class:`anyconfig.models.processor.Processor` class object or its
        instance
    :param cls: A class object to compare with 'type_or_id'
    :return: Processor instance or None
    """
    if isinstance(type_or_id, cls):
        return type_or_id

    with contextlib.suppress(TypeError):
        maybe_cls = typing.cast("ProcClsT", type_or_id)
        if issubclass(maybe_cls, cls):
            return maybe_cls()

    return None


def find_by_type_or_id(type_or_id: str, prs: ProcsT) -> ProcsT:
    """Find the processor by types or IDs.

    :param type_or_id: Type of the data to process or ID of the processor class
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return:
        A list of processor classes to process files of given data type or
        processor 'type_or_id' found by its ID
    :raises: anyconfig.common.UnknownProcessorTypeError
    """
    def pred(pcls: ProcT) -> bool:
        """Provide a predicate."""
        return pcls.cid() == type_or_id or pcls.type() == type_or_id

    pclss = findall_with_pred(pred, prs)
    if not pclss:
        raise common.UnknownProcessorTypeError(type_or_id)

    return pclss


def find_by_fileext(fileext: str, prs: ProcsT) -> ProcsT:
    """Find the processor by file extensions.

    :param fileext: File extension
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: A list of processor class to processor files with given extension
    :raises: common.UnknownFileTypeError
    """
    def pred(pcls: ProcT) -> bool:
        """Provide a predicate."""
        return fileext in pcls.extensions()

    pclss = findall_with_pred(pred, prs)
    if not pclss:
        msg = f"file extension={fileext}"
        raise common.UnknownFileTypeError(msg)

    return pclss  # :: [Processor], never []


def find_by_maybe_file(obj: ioinfo.PathOrIOInfoT, prs: ProcsT) -> ProcsT:
    """Find the processor appropariate for the given file ``obj``.

    :param obj:
        a file path, file or file-like object, pathlib.Path object or an
        'anyconfig.ioinfo.IOInfo' (namedtuple) object
    :param cps_by_ext: A list of processor classes
    :return: A list of processor classes to process given (maybe) file
    :raises: common.UnknownFileTypeError
    """
    # :: [Processor], never []
    return find_by_fileext(ioinfo.make(obj).extension, prs)


def findall(
    obj: ioinfo.PathOrIOInfoT | None, prs: ProcsT,
    forced_type: str | None = None,
) -> ProcsT:
    """Find all of the processors match with the conditions.

    :param obj:
        a file path, file, file-like object, pathlib.Path object or an
        'anyconfig.ioinfo.IOInfo` (namedtuple) object
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :param forced_type:
        Forced processor type of the data to process or ID of the processor
        class or None

    :return: A list of instances of processor classes to process 'obj' data
    :raises:
        ValueError, common.UnknownProcessorTypeError,
        common.UnknownFileTypeError
    """
    if (obj is None or not obj) and forced_type is None:
        msg = (
            "The first argument 'obj' or the second argument 'forced_type' "
            "must be something other than None or False."
        )
        raise ValueError(msg)

    if forced_type is None:
        pclss = find_by_maybe_file(
            typing.cast("ioinfo.PathOrIOInfoT", obj), prs,
        )  # :: [Processor], never []
    else:
        pclss = find_by_type_or_id(forced_type, prs)  # Do.

    return pclss


def find(
    obj: ioinfo.PathOrIOInfoT | None, prs: ProcsT,
    forced_type: MaybeProcT = None,
) -> ProcT:
    """Find the processors best match with the conditions.

    :param obj:
        a file path, file, file-like object, pathlib.Path object or an
        'anyconfig.ioinfo.IOInfo' (namedtuple) object
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :param forced_type:
        Forced processor type of the data to process or ID of the processor
        class or :class:`anyconfig.models.processor.Processor` class object or
        its instance itself
    :param cls: A class object to compare with 'forced_type' later

    :return: an instance of processor class to process 'obj' data
    :raises:
        ValueError, common.UnknownProcessorTypeError,
        common.UnknownFileTypeError
    """
    if forced_type is not None and not isinstance(forced_type, str):
        proc = maybe_processor(
            typing.cast("ProcT | ProcClsT", forced_type),
        )
        if proc is None:
            msg = (
                "Wrong processor class or instance "
                f"was given: {forced_type!r}"
            )
            raise ValueError(msg)

        return proc

    procs = findall(obj, prs, forced_type=typing.cast("str", forced_type))
    return procs[0]


def load_plugins(pgroup: str) -> collections.abc.Iterator[ProcClsT]:
    """Load processor plugins.

    A generator function to yield a class object of
    :class:`anyconfig.models.processor.Processor`.

    :param pgroup: A string represents plugin type, e.g. anyconfig_backends
    """
    eps = importlib.metadata.entry_points()
    for res in (eps.get(pgroup, [])  # type: ignore[attr-defined]
                if isinstance(eps, dict)
                else eps.select(group=pgroup)):
        try:
            yield res.load()
        except ImportError as exc:  # noqa: PERF203
            warnings.warn(f"Failed to load plugin, exc={exc!s}", stacklevel=2)
