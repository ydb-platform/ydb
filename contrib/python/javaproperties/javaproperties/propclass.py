from __future__ import annotations
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, BinaryIO, IO, MutableMapping, Optional, TextIO, TypeVar
from .reading import load
from .writing import dump
from .xmlprops import dump_xml, load_xml

T = TypeVar("T")


class Properties(MutableMapping[str, str]):
    """
    A port of |java8properties|_ that tries to match its behavior as much as is
    Pythonically possible.  `Properties` behaves like a normal
    `~collections.abc.MutableMapping` class (i.e., you can do ``props[key] =
    value`` and so forth), except that it may only be used to store `str`
    values.

    Two `Properties` instances compare equal iff both their key-value pairs and
    :attr:`defaults` attributes are equal.  When comparing a `Properties`
    instance to any other type of mapping, only the key-value pairs are
    considered.

    .. versionchanged:: 0.5.0
         `Properties` instances can now compare equal to `dict`\\s and other
         mapping types

    :param data: A mapping or iterable of ``(key, value)`` pairs with which to
        initialize the `Properties` instance.  All keys and values in ``data``
        must be text strings.
    :type data: mapping or `None`
    :param Optional[Properties] defaults: a set of default properties that will
        be used as fallback for `getProperty`

    .. |java8properties| replace:: Java 8's ``java.util.Properties``
    .. _java8properties: https://docs.oracle.com/javase/8/docs/api/java/util/Properties.html
    """

    def __init__(
        self,
        data: None | Mapping[str, str] | Iterable[tuple[str, str]] = None,
        defaults: Optional["Properties"] = None,
    ) -> None:
        self.data: dict[str, str] = {}
        #: A `Properties` subobject used as fallback for `getProperty`.  Only
        #: `getProperty`, `propertyNames`, `stringPropertyNames`, and `__eq__`
        #: use this attribute; all other methods (including the standard
        #: mapping methods) ignore it.
        self.defaults = defaults
        if data is not None:
            self.update(data)

    def __getitem__(self, key: str) -> str:
        return self.data[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.data[key] = value

    def __delitem__(self, key: str) -> None:
        del self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return (
            "{0.__module__}.{0.__name__}({1.data!r}, defaults={1.defaults!r})".format(
                type(self), self
            )
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Properties):
            return self.data == other.data and self.defaults == other.defaults
        elif isinstance(other, Mapping):
            return dict(self) == other
        else:
            return NotImplemented

    def getProperty(self, key: str, defaultValue: Optional[T] = None) -> str | T | None:
        """
        Fetch the value associated with the key ``key`` in the `Properties`
        instance.  If the key is not present, `defaults` is checked, and then
        *its* `defaults`, etc., until either a value for ``key`` is found or
        the next `defaults` is `None`, in which case `defaultValue` is
        returned.

        :param str key: the key to look up the value of
        :param Any defaultValue: the value to return if ``key`` is not found in
            the `Properties` instance
        :rtype: str (if ``key`` was found)
        """
        try:
            return self[key]
        except KeyError:
            if self.defaults is not None:
                return self.defaults.getProperty(key, defaultValue)
            else:
                return defaultValue

    def load(self, inStream: IO) -> None:
        """
        Update the `Properties` instance with the entries in a ``.properties``
        file or file-like object.

        ``inStream`` may be either a text or binary filehandle, with or without
        universal newlines enabled.  If it is a binary filehandle, its contents
        are decoded as Latin-1.

        .. versionchanged:: 0.5.0
            Invalid ``\\uXXXX`` escape sequences will now cause an
            `InvalidUEscapeError` to be raised

        :param IO inStream: the file from which to read the ``.properties``
            document
        :return: `None`
        :raises InvalidUEscapeError: if an invalid ``\\uXXXX`` escape sequence
            occurs in the input
        """
        self.data.update(load(inStream))

    def propertyNames(self) -> Iterator[str]:
        r"""
        Returns a generator of all distinct keys in the `Properties` instance
        and its `defaults` (and its `defaults`\’s `defaults`, etc.) in
        unspecified order

        :rtype: Iterator[str]
        """
        for k in self.data:
            yield k
        if self.defaults is not None:
            for k in self.defaults.propertyNames():
                if k not in self.data:
                    yield k

    def setProperty(self, key: str, value: str) -> None:
        """Equivalent to ``self[key] = value``"""
        self[key] = value

    def store(self, out: TextIO, comments: Optional[str] = None) -> None:
        """
        Write the `Properties` instance's entries (in unspecified order) in
        ``.properties`` format to ``out``, including the current timestamp.

        :param TextIO out: A file-like object to write the properties to.  It
            must have been opened as a text file with a Latin-1-compatible
            encoding.
        :param Optional[str] comments: If non-`None`, ``comments`` will be
            written to ``out`` as a comment before any other content
        :return: `None`
        """
        dump(self.data, out, comments=comments)

    def stringPropertyNames(self) -> set[str]:
        r"""
        Returns a `set` of all keys in the `Properties` instance and its
        `defaults` (and its `defaults`\ ’s `defaults`, etc.)

        :rtype: set[str]
        """
        names = set(self.data)
        if self.defaults is not None:
            names.update(self.defaults.stringPropertyNames())
        return names

    def loadFromXML(self, inStream: IO) -> None:
        """
        Update the `Properties` instance with the entries in the XML properties
        file ``inStream``.

        Beyond basic XML well-formedness, `loadFromXML` only checks that the
        root element is named ``properties`` and that all of its ``entry``
        children have ``key`` attributes; no further validation is performed.

        :param IO inStream: the file from which to read the XML properties
            document
        :return: `None`
        :raises ValueError: if the root of the XML tree is not a
            ``<properties>`` tag or an ``<entry>`` element is missing a ``key``
            attribute
        """
        self.data.update(load_xml(inStream))

    def storeToXML(
        self,
        out: BinaryIO,
        comment: Optional[str] = None,
        encoding: str = "UTF-8",
    ) -> None:
        """
        Write the `Properties` instance's entries (in unspecified order) in XML
        properties format to ``out``.

        :param BinaryIO out: a file-like object to write the properties to
        :param Optional[str] comment: if non-`None`, ``comment`` will be output
            as a ``<comment>`` element before the ``<entry>`` elements
        :param str encoding: the name of the encoding to use for the XML
            document (also included in the XML declaration)
        :return: `None`
        """
        dump_xml(self.data, out, comment=comment, encoding=encoding)

    def copy(self) -> Properties:
        """
        .. versionadded:: 0.5.0

        Create a shallow copy of the mapping.  The copy's `defaults` attribute
        will be the same instance as the original's `defaults`.
        """
        return type(self)(self.data, self.defaults)
