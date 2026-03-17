#
# Copyright (C) 2018 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# pylint: disable=unidiomatic-typecheck
r"""Abstract processor module.

.. versionadded:: 0.9.5

   - Add to abstract processors such like Parsers (loaders and dumpers).
"""
from __future__ import absolute_import

import operator
import pkg_resources

import anyconfig.compat
import anyconfig.ioinfo
import anyconfig.models.processor
import anyconfig.utils

from anyconfig.globals import (
    UnknownProcessorTypeError, UnknownFileTypeError, IOInfo
)


def _load_plugins_itr(pgroup, safe=True):
    """
    .. seealso:: the doc of :func:`load_plugins`
    """
    for res in pkg_resources.iter_entry_points(pgroup):
        try:
            yield res.load()
        except ImportError:
            if safe:
                continue
            raise


def load_plugins(pgroup, safe=True):
    """
    :param pgroup: A string represents plugin type, e.g. anyconfig_backends
    :param safe: Do not raise ImportError during load if True
    :raises: ImportError
    """
    return list(_load_plugins_itr(pgroup, safe=safe))


def sort_by_prio(prs):
    """
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: Sambe as above but sorted by priority
    """
    return sorted(prs, key=operator.methodcaller("priority"), reverse=True)


def select_by_key(items, sort_fn=sorted):
    """
    :param items: A list of tuples of keys and values, [([key], val)]
    :return: A list of tuples of key and values, [(key, [val])]

    >>> select_by_key([(["a", "aaa"], 1), (["b", "bb"], 2), (["a"], 3)])
    [('a', [1, 3]), ('aaa', [1]), ('b', [2]), ('bb', [2])]
    """
    itr = anyconfig.utils.concat(((k, v) for k in ks) for ks, v in items)
    return list((k, sort_fn(t[1] for t in g))
                for k, g
                in anyconfig.utils.groupby(itr, operator.itemgetter(0)))


def list_by_x(prs, key):
    """
    :param key: Grouping key, "type" or "extensions"
    :return:
        A list of :class:`Processor` or its children classes grouped by
        given 'item', [(cid, [:class:`Processor`)]] by default
    """
    if key == "type":
        kfn = operator.methodcaller(key)
        res = sorted(((k, sort_by_prio(g)) for k, g
                      in anyconfig.utils.groupby(prs, kfn)),
                     key=operator.itemgetter(0))

    elif key == "extensions":
        res = select_by_key(((p.extensions(), p) for p in prs),
                            sort_fn=sort_by_prio)
    else:
        raise ValueError("Argument 'key' must be 'type' or "
                         "'extensions' but it was '%s'" % key)

    return res


def findall_with_pred(predicate, prs):
    """
    :param predicate: any callable to filter results
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: A list of appropriate processor classes or []
    """
    return sorted((p for p in prs if predicate(p)),
                  key=operator.methodcaller("priority"), reverse=True)


def maybe_processor(type_or_id, cls=anyconfig.models.processor.Processor):
    """
    :param type_or_id:
        Type of the data to process or ID of the processor class or
        :class:`anyconfig.models.processor.Processor` class object or its
        instance
    :param cls: A class object to compare with 'type_or_id'
    :return: Processor instance or None
    """
    if isinstance(type_or_id, cls):
        return type_or_id

    if type(type_or_id) == type(cls) and issubclass(type_or_id, cls):
        return type_or_id()

    return None


def find_by_type_or_id(type_or_id, prs):
    """
    :param type_or_id: Type of the data to process or ID of the processor class
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return:
        A list of processor classes to process files of given data type or
        processor 'type_or_id' found by its ID
    :raises: UnknownProcessorTypeError
    """
    def pred(pcls):
        """Predicate"""
        return pcls.cid() == type_or_id or pcls.type() == type_or_id

    pclss = findall_with_pred(pred, prs)
    if not pclss:
        raise UnknownProcessorTypeError(type_or_id)

    return pclss


def find_by_fileext(fileext, prs):
    """
    :param fileext: File extension
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :return: A list of processor class to processor files with given extension
    :raises: UnknownFileTypeError
    """
    def pred(pcls):
        """Predicate"""
        return fileext in pcls.extensions()

    pclss = findall_with_pred(pred, prs)
    if not pclss:
        raise UnknownFileTypeError("file extension={}".format(fileext))

    return pclss  # :: [Processor], never []


def find_by_maybe_file(obj, prs):
    """
    :param obj:
        a file path, file or file-like object, pathlib.Path object or an
        'anyconfig.globals.IOInfo' (namedtuple) object
    :param cps_by_ext: A list of processor classes
    :return: A list of processor classes to process given (maybe) file
    :raises: UnknownFileTypeError
    """
    if not isinstance(obj, IOInfo):
        obj = anyconfig.ioinfo.make(obj)

    return find_by_fileext(obj.extension, prs)  # :: [Processor], never []


def findall(obj, prs, forced_type=None,
            cls=anyconfig.models.processor.Processor):
    """
    :param obj:
        a file path, file, file-like object, pathlib.Path object or an
        'anyconfig.globals.IOInfo` (namedtuple) object
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :param forced_type:
        Forced processor type of the data to process or ID of the processor
        class or None
    :param cls: A class object to compare with 'forced_type' later

    :return: A list of instances of processor classes to process 'obj' data
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    if (obj is None or not obj) and forced_type is None:
        raise ValueError("The first argument 'obj' or the second argument "
                         "'forced_type' must be something other than "
                         "None or False.")

    if forced_type is None:
        pclss = find_by_maybe_file(obj, prs)  # :: [Processor], never []
    else:
        pclss = find_by_type_or_id(forced_type, prs)  # Do.

    return pclss


def find(obj, prs, forced_type=None, cls=anyconfig.models.processor.Processor):
    """
    :param obj:
        a file path, file, file-like object, pathlib.Path object or an
        'anyconfig.globals.IOInfo' (namedtuple) object
    :param prs: A list of :class:`anyconfig.models.processor.Processor` classes
    :param forced_type:
        Forced processor type of the data to process or ID of the processor
        class or :class:`anyconfig.models.processor.Processor` class object or
        its instance itself
    :param cls: A class object to compare with 'forced_type' later

    :return: an instance of processor class to process 'obj' data
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    if forced_type is not None:
        processor = maybe_processor(forced_type, cls=cls)
        if processor is not None:
            return processor

    pclss = findall(obj, prs, forced_type=forced_type, cls=cls)
    return pclss[0]()


class Processors(object):
    """An abstract class of which instance holding processors.
    """
    _pgroup = None  # processor group name to load plugins

    def __init__(self, processors=None):
        """
        :param processors:
            A list of :class:`Processor` or its children class objects or None
        """
        self._processors = dict()  # {<processor_class_id>: <processor_class>}
        if processors is not None:
            self.register(*processors)

        self.load_plugins()

    def register(self, *pclss):
        """
        :param pclss: A list of :class:`Processor` or its children classes
        """
        for pcls in pclss:
            if pcls.cid() not in self._processors:
                self._processors[pcls.cid()] = pcls

    def load_plugins(self):
        """Load and register pluggable processor classes internally.
        """
        if self._pgroup:
            self.register(*load_plugins(self._pgroup))

    def list(self, sort=False):
        """
        :param sort: Result will be sorted if it's True
        :return: A list of :class:`Processor` or its children classes
        """
        prs = self._processors.values()
        if sort:
            return sorted(prs, key=operator.methodcaller("cid"))

        return prs

    def list_by_cid(self):
        """
        :return:
            A list of :class:`Processor` or its children classes grouped by
            each cid, [(cid, [:class:`Processor`)]]
        """
        prs = self._processors
        return sorted(((cid, [prs[cid]]) for cid in sorted(prs.keys())),
                      key=operator.itemgetter(0))

    def list_by_type(self):
        """
        :return:
            A list of :class:`Processor` or its children classes grouped by
            each type, [(type, [:class:`Processor`)]]
        """
        return list_by_x(self.list(), "type")

    def list_by_x(self, item=None):
        """
        :param item: Grouping key, one of "cid", "type" and "extensions"
        :return:
            A list of :class:`Processor` or its children classes grouped by
            given 'item', [(cid, [:class:`Processor`)]] by default
        """
        prs = self._processors

        if item is None or item == "cid":  # Default.
            res = [(cid, [prs[cid]]) for cid in sorted(prs.keys())]

        elif item in ("type", "extensions"):
            res = list_by_x(prs.values(), item)
        else:
            raise ValueError("keyword argument 'item' must be one of "
                             "None, 'cid', 'type' and 'extensions' "
                             "but it was '%s'" % item)
        return res

    def list_x(self, key=None):
        """
        :param key: Which of key to return from "cid", "type", and "extention"
        :return: A list of x 'key'
        """
        if key in ("cid", "type"):
            return sorted(set(operator.methodcaller(key)(p)
                              for p in self._processors.values()))
        if key == "extension":
            return sorted(k for k, _v in self.list_by_x("extensions"))

        raise ValueError("keyword argument 'key' must be one of "
                         "None, 'cid', 'type' and 'extension' "
                         "but it was '%s'" % key)

    def findall(self, obj, forced_type=None,
                cls=anyconfig.models.processor.Processor):
        """
        :param obj:
            a file path, file, file-like object, pathlib.Path object or an
            'anyconfig.globals.IOInfo' (namedtuple) object
        :param forced_type: Forced processor type to find
        :param cls: A class object to compare with 'ptype'

        :return: A list of instances of processor classes to process 'obj'
        :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
        """
        return [p() for p in findall(obj, self.list(),
                                     forced_type=forced_type, cls=cls)]

    def find(self, obj, forced_type=None,
             cls=anyconfig.models.processor.Processor):
        """
        :param obj:
            a file path, file, file-like object, pathlib.Path object or an
            'anyconfig.globals.IOInfo' (namedtuple) object
        :param forced_type: Forced processor type to find
        :param cls: A class object to compare with 'ptype'

        :return: an instance of processor class to process 'obj'
        :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
        """
        return find(obj, self.list(), forced_type=forced_type, cls=cls)

# vim:sw=4:ts=4:et:
