""" Facilities for reading and writing Debian substvars files

The aim of this module is to provide programmatic access to Debian substvars
files to query and manipulate them. The format for the substvars files is
defined in `deb-substvars(5)
<https://manpages.debian.org/unstable/dpkg-dev/deb-substvars.5.html>`_

Overview
========

The most common use-case for substvars is for package helpers to add or update
a substvars (e.g., to add a dependency).  This would look something like:

    >>> from debian.substvars import Substvars
    >>> from tempfile import TemporaryDirectory
    >>> import os
    >>> # Using a tmp dir for the sake of doctests
    >>> with TemporaryDirectory() as debian_dir:
    ...    filename = os.path.join(debian_dir, "foo.substvars")
    ...    with Substvars.load_from_path(filename, missing_ok=True) as svars:
    ...        svars.add_dependency("misc:Depends", "bar (>= 1.0)")

By default, the module creates new substvars as "mandatory" substvars (that
triggers a warning by dpkg-gecontrol if not used.  However, it does also
support the "optional" substvars introduced in dpkg 1.21.8.  See
`Substvars.as_substvars` for an example of how to use the "optional"
substvars.


The :class:`Substvars` class is the key class within this module.

Substvars Classes
-----------------
"""
from __future__ import annotations


import contextlib
import errno
import re
import sys
import typing
from abc import ABC
from collections import OrderedDict
from collections.abc import MutableMapping
from os import PathLike
from types import TracebackType
from typing import Union, Iterator, IO, Iterable, TYPE_CHECKING

try:
    if TYPE_CHECKING:
        from typing import Self   # needs Python 3.11
        AnyPath = Union[PathLike[str], PathLike[bytes], str, bytes]   # needs Python 3.9
except ImportError:
    pass

T = typing.TypeVar('T')

_SUBSTVAR_PATTERN = re.compile(
    r"^(?P<name>\w[-:\dA-Za-z]*)(?P<assignment_type>[?]?=)(?P<value>.*)$"
)


class Substvar:

    __slots__ = ['_assignment_operator', '_value']

    def __init__(self, initial_value: str = "",
                 assignment_operator: str = '=',
                 ) -> None:

        # We have 2 values for _value:
        # 1) string: The variable is set to a fixed string.  This variant is
        #    round-trip safe.
        # 2) set: The variable is dependency-like field.  This variant is *NOT*
        #    round-trip safe.
        #
        # When reading substvars from files, we always use variant 1) and then
        # lazily convert to 2) when necessary.  This choice makes the substvars
        # round-trip safe by default until someone messes with a substvar.
        self._value: str | set[str] = initial_value
        self.assignment_operator: str = assignment_operator

    @property
    def assignment_operator(self) -> str:
        return self._assignment_operator

    @assignment_operator.setter
    def assignment_operator(self, new_operator: str) -> None:
        if new_operator not in {'=', '?='}:
            raise ValueError('Operator must be one of: "=", or "?=" - got: ' + new_operator)
        self._assignment_operator = new_operator

    def add_dependency(self, dependency_clause: str) -> None:
        if self._value == "":
            self._value = {dependency_clause}
            return
        if isinstance(self._value, str):
            # Convert to dependency format
            self._value = {v.strip() for v in self._value.split(',')}
        self._value.add(dependency_clause)

    def resolve(self) -> str:
        if isinstance(self._value, set):
            return ", ".join(sorted(self._value))
        return self._value

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Substvar):
            return False
        if self.assignment_operator != other.assignment_operator:
            return False
        return self.resolve() == other.resolve()


if sys.version_info >= (3, 9) or TYPE_CHECKING:
    class _Substvars_Base(contextlib.AbstractContextManager[T], MutableMapping[str, str], ABC):
        pass
else:
    # Python 3.5 - 3.8 compat - we are not allowed to subscript the abc.MutableMapping
    # - use this little hack to work around it
    # Note that Python 3.5 is so old that it does not have AbstractContextManager,
    # so we re-implement it here as well.
    class _Substvars_Base(typing.Generic[T], MutableMapping, ABC):

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None


class Substvars(_Substvars_Base['Substvars']):
    """Substvars is a dict-like object containing known substvars for a given package.

    >>> substvars = Substvars()
    >>> substvars['foo'] = 'bar, golf'
    >>> substvars['foo']
    'bar, golf'
    >>> substvars.add_dependency('foo', 'dpkg (>= 1.20.0)')
    >>> substvars['foo']
    'bar, dpkg (>= 1.20.0), golf'
    >>> 'foo' in substvars
    True
    >>> sorted(substvars)
    ['foo']
    >>> del substvars['foo']
    >>> substvars['foo']
    Traceback (most recent call last):
        ...
    KeyError: 'foo'
    >>> substvars.get('foo')
    >>> # None
    >>> substvars['foo'] = ""
    >>> substvars['foo']
    ''

    The Substvars object also provide methods for serializing and deserializing
    the substvars into and from the format used by dpkg-gencontrol.

    The Substvars object can be used as a context manager, which causes the substvars
    to be saved when the context manager exits successfully (i.e., no exceptions are raised).
    """

    __slots__ = ['_vars_dict', '_substvars_path']

    def __init__(self) -> None:
        self._vars_dict: dict[str, Substvar] = OrderedDict()
        self._substvars_path: AnyPath | None = None

    @classmethod
    def load_from_path(cls, substvars_path: AnyPath, missing_ok: bool = False) -> Self:
        """Shorthand for initializing a Substvars from a file

        The return substvars will have `substvars_path` set to the provided path enabling
        `save()` to work out of the box. This also makes it easy to combine this with the
        context manager interface to automatically save the file again.

        >>> import os
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmpdir:
        ...    filename = os.path.join(tmpdir, "foo.substvars")
        ...    # Obviously, this does not exist
        ...    print("Exists before: " + str(os.path.exists(filename)))
        ...    with Substvars.load_from_path(filename, missing_ok=True) as svars:
        ...        svars.add_dependency("misc:Depends", "bar (>= 1.0)")
        ...    print("Exists after: " + str(os.path.exists(filename)))
        Exists before: False
        Exists after: True

        :param substvars_path: The path to load from
        :param missing_ok: If True, then the path does not have to exist (i.e.
          FileNotFoundError causes an empty Substvars object to be returned).  Combined
          with the context manager, this is useful for packaging helpers that want to
          append / update to the existing if it exists or create it if it does not exist.
        """
        substvars = cls()
        try:
            with open(substvars_path, encoding='utf-8') as fd:
                substvars.read_substvars(fd)
        except OSError as e:
            if e.errno != errno.ENOENT or not missing_ok:
                raise
        substvars.substvars_path = substvars_path
        return substvars

    @property
    def _vars(self) -> dict[str, Substvar]:
        # Indirection to support subclasses that want to provide lazy loading or other "fun stuff"
        return self._vars_dict

    @_vars.setter
    def _vars(self, vars_dict: dict[str, Substvar]) -> None:
        # Indirection to support subclasses that want to provide lazy loading or other "fun stuff"
        self._vars_dict = vars_dict

    @property
    def substvars_path(self) -> AnyPath | None:
        return self._substvars_path

    @substvars_path.setter
    def substvars_path(self, new_path: AnyPath | None) -> None:
        self._substvars_path = new_path

    def add_dependency(self, substvar: str, dependency_clause: str) -> None:
        """Add a dependency clause to a given substvar

        >>> substvars = Substvars()
        >>> # add_dependency automatically creates variables
        >>> 'misc:Recommends' not in substvars
        True
        >>> substvars.add_dependency('misc:Recommends', "foo (>= 1.0)")
        >>> substvars['misc:Recommends']
        'foo (>= 1.0)'
        >>> # It can be appended to other variables
        >>> substvars['foo'] = 'bar, golf'
        >>> substvars.add_dependency('foo', 'dpkg (>= 1.20.0)')
        >>> substvars['foo']
        'bar, dpkg (>= 1.20.0), golf'
        >>> # Exact duplicates are ignored
        >>> substvars.add_dependency('foo', 'dpkg (>= 1.20.0)')
        >>> substvars['foo']
        'bar, dpkg (>= 1.20.0), golf'

        """
        try:
            variable = self._vars[substvar]
        except KeyError:
            variable = Substvar()
            self._vars[substvar] = variable
        variable.add_dependency(dependency_clause)

    def __exit__(self,
                 exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: TracebackType | None,
                 ) -> bool | None:
        if exc_type is None:
            self.save()
        return super().__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self) -> Iterator[str]:
        return iter(self._vars)

    def __len__(self) -> int:
        return len(self._vars_dict)

    def __contains__(self, item: object) -> bool:
        return item in self._vars

    def __getitem__(self, key: str) -> str:
        return self._vars[key].resolve()

    def __delitem__(self, key: str) -> None:
        del self._vars[key]

    def __setitem__(self, key: str, value: str) -> None:
        self._vars[key] = Substvar(value)

    @property
    def as_substvar(self) -> MutableMapping[str, Substvar]:
        """Provides a mapping to the Substvars object for more advanced operations

        Treating a substvars file mostly as a "str -> str" mapping is sufficient for many cases.
        But when full control over the substvars (like fiddling with the assignment operator) is
        needed this attribute is useful.

        >>> content = '''
        ... # Some comment (which is allowed but no one uses them - also, they are not preserved)
        ... shlib:Depends=foo (>= 1.0), libbar2 (>= 2.1-3~)
        ... random:substvar?=With the new assignment operator from dpkg 1.21.8
        ... '''
        >>> substvars = Substvars()
        >>> substvars.read_substvars(content.splitlines())
        >>> substvars.as_substvar["shlib:Depends"].assignment_operator
        '='
        >>> substvars.as_substvar["random:substvar"].assignment_operator
        '?='
        >>> # Mutation is also possible
        >>> substvars.as_substvar["shlib:Depends"].assignment_operator = '?='
        >>> print(substvars.dump(), end="")
        shlib:Depends?=foo (>= 1.0), libbar2 (>= 2.1-3~)
        random:substvar?=With the new assignment operator from dpkg 1.21.8
        """
        # This is an indirection of `_vars` to avoid exposing the `_vars` setter
        return self._vars

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Substvars):
            return False
        return self._vars == other._vars

    def dump(self) -> str:
        """Debug aid that generates a string representation of the content

        For persisting the contents, please consider `save()` or `write_substvars`.
        """
        return "".join(f"{k}{v.assignment_operator}{v.resolve()}\n"
                       for k, v in self._vars.items()
                       )

    def save(self) -> None:
        """Save the substvars file

        Replace the path denoted by the `substvars_path` attribute with the
        in-memory version of the substvars.  Note that the `substvars_path`
        property must be not None for this method to work.
        """
        if self._substvars_path is None:
            raise TypeError("The substvar does not have a substvars_path: Please"
                            " set substvars_path first or use write_substvars")

        with open(self._substvars_path, 'w', encoding='utf-8') as fd:
            return self.write_substvars(fd)

    def write_substvars(self, fileobj: IO[str]) -> None:
        """Write a copy of the substvars to an open text file

        :param fileobj: The open file (should open in text mode using the UTF-8 encoding)
        """
        fileobj.writelines(f"{k}{v.assignment_operator}{v.resolve()}\n"
                           for k, v in self._vars.items()
                           )

    def read_substvars(self, fileobj: Iterable[str]) -> None:
        """Read substvars from an open text file in the format supported by dpkg-gencontrol

        On success, all existing variables will be discarded and only variables
        from the file will be present after this method completes.  In case of
        any IO related errors, the object retains its state prior to the call
        of this method.

        >>> content = '''
        ... # Some comment (which is allowed but no one uses them - also, they are not preserved)
        ... shlib:Depends=foo (>= 1.0), libbar2 (>= 2.1-3~)
        ... random:substvar?=With the new assignment operator from dpkg 1.21.8
        ... '''
        >>> substvars = Substvars()
        >>> substvars.read_substvars(content.splitlines())
        >>> substvars["shlib:Depends"]
        'foo (>= 1.0), libbar2 (>= 2.1-3~)'
        >>> substvars["random:substvar"]
        'With the new assignment operator from dpkg 1.21.8'

        :param fileobj: An open file (in text mode using the UTF-8 encoding) or an
          iterable of str that provides line by line content.
        """
        vars_dict = OrderedDict()
        for line in fileobj:
            if line.strip() == '' or line[0] == '#':
                continue
            m = _SUBSTVAR_PATTERN.match(line.rstrip("\r\n"))
            if not m:
                continue
            varname, assignment_operator, value = m.groups()
            vars_dict[varname] = Substvar(value, assignment_operator=assignment_operator)
        self._vars = vars_dict
