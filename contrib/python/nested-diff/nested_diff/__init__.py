# Copyright 2018-2026 Michael Samoglyadov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Recursive diff and patch for nested structures."""

import pickle

import nested_diff.handlers

__all__ = ['Differ', 'Iterator', 'Patcher', 'diff', 'patch']

__version__ = '1.10.0'
__author__ = 'Michael Samoglyadov'
__license__ = 'Apache License, Version 2.0'
__website__ = 'https://github.com/mr-mixas/Nested-Diff.py'

DEFAULT_HANDLER = nested_diff.handlers.TypeHandler()

TYPE_HANDLERS = (
    nested_diff.handlers.DictHandler(),
    nested_diff.handlers.ListHandler(),
    nested_diff.handlers.TupleHandler(),
    nested_diff.handlers.SetHandler(),
    nested_diff.handlers.FrozenSetHandler(),
    nested_diff.handlers.IntHandler(),
    nested_diff.handlers.FloatHandler(),
    nested_diff.handlers.StrHandler(),
    nested_diff.handlers.BytesHandler(),
)


class Differ:
    """Compute recursive diff for two passed objects.

    Dicts, lists, tuples, sets and frozensets traversed recursively, other
    types compared by values. Any type diff may be customized by user-defined
    type handlers.

    Diff is a dict and may contain status keys:

    `A` stands for 'added', it's value - added item.
    `D` means 'different' and contains subdiff.
    `N` is a new value for changed item.
    `O` is a changed item's old value.
    `R` key used for removed item.
    `U` represent unchanged item.

    and auxiliary keys:

    `C` comment; optional, value - arbitrary string.
    `E` extension ID (optional).
    `I` index for sequence item, used only when prior item was omitted.

    Diff metadata alternates with actual data; simple types specified as is,
    dicts, lists and tuples contain subdiffs for their items with native for
    such types addressing: indexes for lists and tuples, keys for dictionaries.
    Any status key, except `D` may be omitted during diff computation. `E` key
    is used with `D` when entity unable to contain diff by itself (set,
    frozenset for example); `D` contain a list of subdiffs in this case.

    Example:
    a:  {"one": [5,7]}
    b:  {"one": [5], "two": 2}
    opts: U=False  # omit unchanged items

    diff:
    {"D": {"one": {"D": [{"I": 1, "R": 7}]}, "two": {"A": 2}}}
    | |   |  |    | |   || |   |   |   |       |    | |   |
    | |   |  |    | |   || |   |   |   |       |    | |   +- with value 2
    | |   |  |    | |   || |   |   |   |       |    | +- key 'two' was added
    | |   |  |    | |   || |   |   |   |       |    +- subdiff for it
    | |   |  |    | |   || |   |   |   |       +- another key from top-level
    | |   |  |    | |   || |   |   |   +- what it was (item's value: 7)
    | |   |  |    | |   || |   |   +- what happened to item (removed)
    | |   |  |    | |   || |   +- list item's actual index
    | |   |  |    | |   || +- prior item was omitted
    | |   |  |    | |   |+- subdiff for list item
    | |   |  |    | |   +- it's value - list
    | |   |  |    | +- it is deeply changed
    | |   |  |    +- subdiff for key 'one'
    | |   |  +- it has key 'one'
    | |   +- top-level thing is a dict
    | +- changes somewhere deeply inside
    +- diff is always a dict

    """

    default_differ = DEFAULT_HANDLER.diff

    def __init__(  # noqa: PLR0913
        self,
        *,
        A=True,  # noqa: N803
        N=True,  # noqa: N803
        O=True,  # noqa: E741 N803
        R=True,  # noqa: N803
        U=True,  # noqa: N803
        trimR=False,  # noqa: N803
        dumper=None,
        handlers=None,
    ):
        """Initialize Differ.

        Args:
            A: Enable/disable added items.
            N: Enable/disable new items.
            O: Enable/disable old items.
            R: Enable/disable removed items.
            U: Enable/disable unchanged items.
            trimR: When enabled will replace removed data by None.
            dumper: Optional objects serialiser.
            handlers: A list of type handlers.

        """
        self.op_a = A
        self.op_n = N
        self.op_o = O
        self.op_r = R
        self.op_u = U
        self.op_trim_r = trimR

        self.dump = dumper or pickle.dumps

        self._differs = {}

        for handler in TYPE_HANDLERS if handlers is None else handlers:
            self.set_handler(handler)

    def diff(self, a, b):
        """Calculate diff for two objects.

        This method calls registered handler according diffed objects type.
        Default handler called for objects with different types or when no
        handler registered for such type.

        Args:
            a: First object to diff.
            b: Second object to diff.

        Returns:
            Tuple: equality flag and nested diff.

        """
        if a is b:
            return True, {'U': a} if self.op_u else {}

        differ = self.default_differ

        if a.__class__ is b.__class__:
            try:
                differ = self._differs[a.__class__]
            except KeyError:
                pass

        return differ(self, a, b)

    def set_handler(self, handler):
        """Set handler.

        Args:
            handler: Instance of handlers.TypeHandler.

        """
        self._differs[handler.handled_type] = handler.diff


class Patcher:
    """Patch objects using nested diff."""

    default_patcher = DEFAULT_HANDLER.patch

    def __init__(self, handlers=None):
        """Initialize Patcher.

        Args:
            handlers: List of type handlers.

        """
        self._patchers_by_cls = {}
        self._patchers_by_ext = {}

        for handler in TYPE_HANDLERS if handlers is None else handlers:
            self.set_handler(handler)

        if handlers is None:
            self.set_handler(nested_diff.handlers.TextHandler())

    def patch(self, target, ndiff):
        """Patch object using nested diff.

        This method calls appropriate handler for target value according to
        it's type.

        Args:
            target: Object to patch.
            ndiff: Nested diff.

        Returns:
            Patched object.

        Raises:
            ValueError: Unsupported patch type passed.

        """
        if 'D' in ndiff:
            try:
                extension_id = ndiff['E']
                try:
                    patcher = self._patchers_by_ext[extension_id]
                except KeyError:
                    raise ValueError(
                        f'unsupported extension: {extension_id}',
                    ) from None
            except KeyError:
                cls = ndiff['D'].__class__

                try:
                    patcher = self._patchers_by_cls[cls]
                except KeyError:
                    raise ValueError(
                        f'unsupported patch type: {cls.__name__}',
                    ) from None

            return patcher(self, target, ndiff)

        return self.default_patcher(self, target, ndiff)

    def set_handler(self, handler):
        """Set handler.

        Args:
            handler: Instance of handlers.TypeHandler.

        """
        self._patchers_by_cls[handler.handled_type] = handler.patch

        if handler.extension_id is not None:
            self._patchers_by_ext[handler.extension_id] = handler.patch


class Iterator:
    """Nested diff iterator."""

    default_iterator = DEFAULT_HANDLER.iterate_diff

    def __init__(self, *, handlers=None, sort_keys=False):
        """Initialize iterator.

        Args:
            sort_keys: Sort diff items if applicable.
            handlers: List of type handlers.

        """
        self.sort_keys = sort_keys

        self._iters_by_cls = {}
        self._iters_by_ext = {}

        for handler in TYPE_HANDLERS if handlers is None else handlers:
            self.set_handler(handler)

    def _get_iterator(self, ndiff):
        """Return appropriate iterator for passed nested diff."""
        try:
            extension_id = ndiff['E']
            try:
                iterator = self._iters_by_ext[extension_id]
            except KeyError:
                raise ValueError(
                    f'unsupported extension: {extension_id}',
                ) from None
        except KeyError:
            try:
                iterator = self._iters_by_cls[ndiff['D'].__class__]
            except KeyError:
                iterator = self.default_iterator

        return iterator(self, ndiff)

    def iterate(self, ndiff):
        """Iterate over nested diff.

        Args:
            ndiff: Nested diff to iterate.

        Yields:
            Tuples with diff, key and subdiff for each nested diff.

        """
        stack = [self._get_iterator(ndiff)]

        while stack:
            try:
                ndiff, key, subdiff = next(stack[-1])
            except StopIteration:
                stack.pop()
                continue

            yield ndiff, key, subdiff

            if subdiff is None:
                continue

            stack.append(self._get_iterator(subdiff))

    def set_handler(self, handler):
        """Set handler.

        Args:
            handler: Instance of handlers.TypeHandler.

        """
        self._iters_by_cls[handler.handled_type] = handler.iterate_diff

        if handler.extension_id is not None:
            self._iters_by_ext[handler.extension_id] = handler.iterate_diff


def diff(a, b, extra_handlers=(), **kwargs):
    """Calculate diff for two objects.

    Args:
        a: First object to diff.
        b: Second object to diff.
        extra_handlers: List of additional type handlers.
        kwargs: Passed to Differ's constructor as is.

    Returns:
        Nested diff.

    """
    differ = Differ(**kwargs)

    for handler in extra_handlers:
        differ.set_handler(handler)

    return differ.diff(a, b)[1]


def patch(target, ndiff, **kwargs):
    """Patch object using nested diff.

    Args:
        target: Object to patch.
        ndiff: Nested diff.
        kwargs: Passed to Patcher's constructor as is.

    Returns:
        Patched object.

    """
    return Patcher(**kwargs).patch(target, ndiff)
