"""Dictpath paths module"""
from contextlib import contextmanager

from six import text_type

from dictpath.accessors import DictOrListAccessor
from dictpath.parsers import parse_args

SEPARATOR = '/'


class BasePath(object):

    def __init__(self, *args, **kwargs):
        separator = kwargs.pop('separator', SEPARATOR)
        self.parts = parse_args(args)
        self.separator = separator

    @classmethod
    def _from_parts(cls, args, separator=SEPARATOR):
        self = cls(separator=separator)
        self.parts = parse_args(args)
        return self

    @classmethod
    def _from_parsed_parts(cls, parts, separator=SEPARATOR):
        self = cls(separator=separator)
        self.parts = parts
        return self

    @property
    def _cparts(self):
        # Cached casefolded parts, for hashing and comparison
        try:
            return self._cparts_cached
        except AttributeError:
            self._cparts_cached = self._get_cparts()
            return self._cparts_cached

    def _get_cparts(self):
        return [text_type(p) for p in self.parts]

    def _make_child(self, args):
        parts = parse_args(args, self.separator)
        parts_joined = self.parts + parts
        return self._from_parsed_parts(parts_joined, self.separator)

    def __str__(self):
        return self.separator.join(self._cparts)

    def __repr__(self):
        return "{}({!r})".format(
            self.__class__.__name__, str(self))

    def __hash__(self):
        return hash(tuple(self._cparts))

    def __truediv__(self, key):
        try:
            return self._make_child((key, ))
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, key):
        try:
            return self._from_parts([key] + self.parts)
        except TypeError:
            return NotImplemented

    def __eq__(self, other):
        if not isinstance(other, BasePath):
            return NotImplemented
        return self._cparts == other._cparts

    def __lt__(self, other):
        if not isinstance(other, BasePath):
            return NotImplemented
        return self._cparts < other._cparts

    def __le__(self, other):
        if not isinstance(other, BasePath):
            return NotImplemented
        return self._cparts <= other._cparts

    def __gt__(self, other):
        if not isinstance(other, BasePath):
            return NotImplemented
        return self._cparts > other._cparts

    def __ge__(self, other):
        if not isinstance(other, BasePath):
            return NotImplemented
        return self._cparts >= other._cparts


class AccessorPath(BasePath):

    def __init__(self, accessor, *args, **kwargs):
        separator = kwargs.pop('separator', SEPARATOR)
        super(AccessorPath, self).__init__(
            *args, separator=separator)
        self.accessor = accessor

    @classmethod
    def _from_parsed_parts(cls, accessor, parts, separator=SEPARATOR):
        self = cls(accessor, separator=separator)
        self.parts = parts
        return self

    def __iter__(self):
        return self.iter()

    def __getitem__(self, key):
        with self.open() as d:
            return d[key]

    def __contains__(self, key):
        with self.open() as d:
            return key in d

    def __len__(self):
        return self.accessor.len(self.parts)

    def keys(self):
        return self.accessor.keys(self.parts)

    def getkey(self, key, default=None):
        with self.open() as d:
            try:
                return d[key]
            except KeyError:
                return default

    @contextmanager
    def open(self):
        # Cached path content
        try:
            yield self._content_cached
        except AttributeError:
            with self._open() as content:
                self._content_cached = content
                yield self._content_cached

    def _open(self):
        return self.accessor.open(self.parts)

    def iter(self):
        for idx in range(self.accessor.len(self.parts)):
            yield self._make_child_relpath(idx)

    def iteritems(self):
        return self.items()

    def items(self):
        for key in self.accessor.keys(self.parts):
            yield key, self._make_child_relpath(key)

    def content(self):
        with self.open() as d:
            return d

    def get(self, key, default=None):
        if key in self:
            return self.__truediv__(key)
        return default

    def _make_child(self, args):
        parts = parse_args(args, self.separator)
        parts_joined = self.parts + parts
        return self._from_parsed_parts(
            self.accessor, parts_joined, self.separator)

    def _make_child_relpath(self, part):
        # This is an optimization used for dir walking.  `part` must be
        # a single part relative to this path.
        parts = self.parts + [part]
        return self._from_parsed_parts(
            self.accessor, parts, self.separator)


class DictOrListPath(AccessorPath):

    def __init__(self, dict_or_list, *args, **kwargs):
        accessor = DictOrListAccessor(dict_or_list)
        return super(DictOrListPath, self).__init__(
            accessor, *args, **kwargs)
