# -*- coding: utf-8 -*-
"""
Method chaining interface.

.. versionadded:: 1.0.0
"""

from __future__ import absolute_import, print_function

import pydash as pyd

from .helpers import NoValue


__all__ = (
    "chain",
    "tap",
    "thru",
)


class Chain(object):
    """Enables chaining of :attr:`module` functions."""

    #: Object that contains attribute references to available methods.
    module = pyd

    def __init__(self, value=NoValue):
        self._value = value

    def value(self):
        """
        Return current value of the chain operations.

        Returns:
            mixed: Current value of chain operations.
        """
        return self(self._value)

    def to_string(self):
        """
        Return current value as string.

        Returns:
            str: Current value of chain operations casted to ``str``.
        """
        return self.module.to_string(self.value())

    def commit(self):
        """
        Executes the chained sequence and returns the wrapped result.

        Returns:
            Chain: New instance of :class:`Chain` with resolved value from
                previous :class:`Class`.
        """
        return Chain(self.value())

    def plant(self, value):
        """
        Return a clone of the chained sequence planting `value` as the wrapped value.

        Args:
            value (mixed): Value to plant as the initial chain value.
        """
        # pylint: disable=no-member,maybe-no-member
        wrapper = self._value
        wrappers = []

        if hasattr(wrapper, "_value"):
            wrappers = [wrapper]

            while isinstance(wrapper._value, ChainWrapper):
                wrapper = wrapper._value
                wrappers.insert(0, wrapper)

        clone = Chain(value)

        for wrap in wrappers:
            clone = ChainWrapper(clone._value, wrap.method)(*wrap.args, **wrap.kwargs)

        return clone

    @classmethod
    def get_method(cls, name):
        """
        Return valid :attr:`module` method.

        Args:
            name (str): Name of pydash method to get.

        Returns:
            function: :attr:`module` callable.

        Raises:
            InvalidMethod: Raised if `name` is not a valid :attr:`module` method.
        """
        # Python 3.5 issue with pytest doctest call where inspect module tries
        # to unwrap this class. If we don't return here, we get an
        # InvalidMethod exception.
        if name in ("__wrapped__",):  # pragma: no cover
            return cls

        method = getattr(cls.module, name, None)

        if not callable(method) and not name.endswith("_"):
            # Alias method names not ending in underscore to their underscore
            # counterpart. This allows chaining of functions like "map_()"
            # using "map()" instead.
            method = getattr(cls.module, name + "_", None)

        if not callable(method):
            raise cls.module.InvalidMethod(("Invalid pydash method: {0}".format(name)))

        return method

    def __getattr__(self, attr):
        """
        Proxy attribute access to :attr:`module`.

        Args:
            attr (str): Name of :attr:`module` function to chain.

        Returns:
            ChainWrapper: New instance of :class:`ChainWrapper` with value passed on.

        Raises:
            InvalidMethod: Raised if `attr` is not a valid function.
        """
        return ChainWrapper(self._value, self.get_method(attr))

    def __call__(self, value):
        """
        Return result of passing `value` through chained methods.

        Args:
            value (mixed): Initial value to pass through chained methods.

        Returns:
            mixed: Result of method chain evaluation of `value`.
        """
        if isinstance(self._value, ChainWrapper):
            # pylint: disable=maybe-no-member
            value = self._value.unwrap(value)
        return value


class ChainWrapper(object):
    """Wrap :class:`Chain` method call within a :class:`ChainWrapper` context."""

    def __init__(self, value, method):
        self._value = value
        self.method = method
        self.args = ()
        self.kwargs = {}

    def _generate(self):
        """Generate a copy of this instance."""
        # pylint: disable=attribute-defined-outside-init
        new = self.__class__.__new__(self.__class__)
        new.__dict__ = self.__dict__.copy()
        return new

    def unwrap(self, value=NoValue):
        """
        Execute :meth:`method` with :attr:`_value`, :attr:`args`, and :attr:`kwargs`.

        If :attr:`_value` is an instance of :class:`ChainWrapper`, then unwrap it before calling
        :attr:`method`.
        """
        # Generate a copy of ourself so that we don't modify the chain wrapper
        # _value directly. This way if we are late passing a value, we don't
        # "freeze" the chain wrapper value when a value is first passed.
        # Otherwise, we'd locked the chain wrapper value permanently and not be
        # able to reuse it.
        wrapper = self._generate()

        if isinstance(wrapper._value, ChainWrapper):
            # pylint: disable=no-member,maybe-no-member
            wrapper._value = wrapper._value.unwrap(value)
        elif not isinstance(value, ChainWrapper) and value is not NoValue:
            # Override wrapper's initial value.
            wrapper._value = value

        if wrapper._value is not NoValue:
            value = wrapper._value

        return wrapper.method(value, *wrapper.args, **wrapper.kwargs)

    def __call__(self, *args, **kwargs):
        """
        Invoke the :attr:`method` with :attr:`value` as the first argument and return a new
        :class:`Chain` object with the return value.

        Returns:
            Chain: New instance of :class:`Chain` with the results of :attr:`method` passed in as
                value.
        """
        self.args = args
        self.kwargs = kwargs
        return Chain(self)


class _Dash(object):
    """Class that provides attribute access to valid :mod:`pydash` methods and callable access to
    :mod:`pydash` method chaining."""

    def __getattr__(self, attr):
        """Proxy to :meth:`Chain.get_method`."""
        return Chain.get_method(attr)

    def __call__(self, value=NoValue):
        """Return a new instance of :class:`Chain` with `value` as the seed."""
        return Chain(value)


def chain(value=NoValue):
    """
    Creates a :class:`Chain` object which wraps the given value to enable intuitive method chaining.
    Chaining is lazy and won't compute a final value until :meth:`Chain.value` is called.

    Args:
        value (mixed): Value to initialize chain operations with.

    Returns:
        :class:`Chain`: Instance of :class:`Chain` initialized with `value`.

    Example:

        >>> chain([1, 2, 3, 4]).map(lambda x: x * 2).sum().value()
        20
        >>> chain().map(lambda x: x * 2).sum()([1, 2, 3, 4])
        20

        >>> summer = chain([1, 2, 3, 4]).sum()
        >>> new_summer = summer.plant([1, 2])
        >>> new_summer.value()
        3
        >>> summer.value()
        10

        >>> def echo(item): print(item)
        >>> summer = chain([1, 2, 3, 4]).for_each(echo).sum()
        >>> committed = summer.commit()
        1
        2
        3
        4
        >>> committed.value()
        10
        >>> summer.value()
        1
        2
        3
        4
        10

    .. versionadded:: 1.0.0

    .. versionchanged:: 2.0.0
        Made chaining lazy.

    .. versionchanged:: 3.0.0

        - Added support for late passing of `value`.
        - Added :meth:`Chain.plant` for replacing initial chain value.
        - Added :meth:`Chain.commit` for returning a new :class:`Chain` instance initialized with
          the results from calling :meth:`Chain.value`.
    """
    return Chain(value)


def tap(value, interceptor):
    """
    Invokes `interceptor` with the `value` as the first argument and then returns `value`. The
    purpose of this method is to "tap into" a method chain in order to perform operations on
    intermediate results within the chain.

    Args:
        value (mixed): Current value of chain operation.
        interceptor (callable): Function called on `value`.

    Returns:
        mixed: `value` after `interceptor` call.

    Example:

        >>> data = []
        >>> def log(value): data.append(value)
        >>> chain([1, 2, 3, 4]).map(lambda x: x * 2).tap(log).value()
        [2, 4, 6, 8]
        >>> data
        [[2, 4, 6, 8]]

    .. versionadded:: 1.0.0
    """
    interceptor(value)
    return value


def thru(value, interceptor):
    """
    Returns the result of calling `interceptor` on `value`. The purpose of this method is to pass
    `value` through a function during a method chain.

    Args:
        value (mixed): Current value of chain operation.
        interceptor (callable): Function called with `value`.

    Returns:
        mixed: Results of ``interceptor(value)``.

    Example:

        >>> chain([1, 2, 3, 4]).thru(lambda x: x * 2).value()
        [1, 2, 3, 4, 1, 2, 3, 4]

    .. versionadded:: 2.0.0
    """
    return interceptor(value)
