"""
Reactive API for Dynamic Expression Pipelines.

`rx` provides a wrapper around Python objects, enabling the creation of
reactive expression pipelines that dynamically update based on changes to their
underlying parameters or widgets. This approach simplifies the implementation of
dynamic, interactive behavior in Python code, particularly for data manipulation
and visualization tasks.

Overview
--------

An `rx` instance tracks and records the operations applied to a wrapped object,
constructing a chain of transformations that can reactively update as inputs change.
These operations are recorded and evaluated lazily, ensuring that the object state
remains up-to-date while enabling efficient re-computation.

The original input object is stored in a shared mutable list accessible via the
`_obj` property. This shared reference allows all `rx` instances derived from the
same object to reflect updates, whether through the `.value` property or due to
modifications in the underlying data.

Core Features
-------------

The `rx` implementation intercepts and records various operations performed on an
object, including:

- **Attribute Access**: Captured using `__getattribute__` to watch for method or
  property usage.
- **Method Calls**: Handled through `__call__` after an attribute is accessed.
- **Indexing**: Tracked via `__getitem__` for slicing or item access.
- **Operators**: Supports all standard operators (e.g., `__add__`, `__gt__`) to
  enable arithmetic and logical expressions.
- **NumPy Integration**: Overrides `__array_ufunc__` to capture NumPy universal
  function calls.

This functionality makes `rx` objects particularly well-suited for interactive
environments such as Jupyter notebooks or Python REPLs. Lazy evaluation ensures that
pipeline operations are executed only when their result is needed, enabling dynamic
inspection and interactive exploration. For instance, tab-completion and docstring
retrieval (`dfi.A.max?`) work seamlessly with reactive expressions.

Reactive Expression Mechanics
-----------------------------

When an operation is applied to an `rx` instance:

1. The operation is recorded in a structured format (stored in `_operation`).
2. A new `rx` instance is created using the `_clone` method.
3. The `_dirty` attribute is set to `True` for dependent objects, indicating the
   need for re-evaluation.

Each `_operation` dictionary includes:

- `fn`: The function or method to execute.
- `args`: Positional arguments for the function or method.
- `kwargs`: Keyword arguments for the function or method.
- `reverse`: A flag indicating whether the input object and first argument should
  be swapped during execution.

Chain Tracking and Dependencies
-------------------------------

Reactive pipelines consist of chains of `rx` instances, with each instance linked
to its predecessor. The `_depth` attribute indicates the chain depth, starting at
0 for the root object. Note that `_depth` reflects the chain's outer structure, not
the total number of reactive instances in a pipeline.

Instances also track their dependencies to ensure accurate updates:
- `_method`: Temporarily stores the method or attribute accessed (e.g., `'head'`
  in `dfi.head()`).
- `_dirty`: Indicates whether the current value needs re-computation.
- `_current`: Stores the result of the most recent computation.

Benefits and Use Cases
----------------------

The reactive approach eliminates the need for explicit callbacks and manual state
management, making it ideal for:

- Real-time data processing and visualization pipelines.
- Interactive dashboards and applications.
- Simplifying complex workflows with dynamic dependencies.

By leveraging Python's operator overloading and lazy evaluation, `rx` provides a
powerful and intuitive way to manage dynamic behavior in Python applications.
"""
from __future__ import annotations

import inspect
import math
import operator

from collections.abc import Iterable, Iterator
from functools import partial
from types import FunctionType, MethodType
from typing import Any, Callable, Optional

from .depends import depends
from .display import _display_accessors, _reactive_display_objs
from .parameterized import (
    Parameter, Parameterized, Skip, Undefined, eval_function_with_deps, get_method_owner,
    register_reference_transform, resolve_ref, resolve_value, transform_reference
)
from .parameters import Boolean, Event
from ._utils import _to_async_gen, iscoroutinefunction, full_groupby


class Wrapper(Parameterized):
    """Helper class to allow updating literal values easily."""

    object = Parameter(allow_refs=False)


class GenWrapper(Parameterized):
    """Helper class to allow streaming from generator functions."""

    object = Parameter(allow_refs=True)


class Trigger(Parameterized):
    """Helper class to allow triggering an event under some condition."""

    value = Event()

    def __init__(self, parameters=None, internal=False, **params):
        super().__init__(**params)
        self.internal = internal
        self.parameters = parameters

class Resolver(Parameterized):
    """Helper class to allow (recursively) resolving references."""

    object = Parameter(allow_refs=True)

    recursive = Boolean(default=False)

    value = Parameter()

    def __init__(self, **params):
        self._watchers = []
        super().__init__(**params)

    def _resolve_value(self, *events):
        nested = self.param.object.nested_refs
        refs = resolve_ref(self.object, nested)
        value = resolve_value(self.object, nested)
        if self.recursive:
            new_refs = [r for r in resolve_ref(value, nested) if r not in refs]
            while new_refs:
                refs += new_refs
                value = resolve_value(value, nested)
                new_refs = [r for r in resolve_ref(value, nested) if r not in refs]
            if events:
                self._update_refs(refs)
        self.value = value
        return refs

    @depends('object', watch=True, on_init=True)
    def _resolve_object(self):
        refs = self._resolve_value()
        self._update_refs(refs)

    def _update_refs(self, refs):
        for w in self._watchers:
            (w.inst or w.cls).param.unwatch(w)
        self._watchers = []
        for _, params in full_groupby(refs, lambda x: id(x.owner)):
            self._watchers.append(
                params[0].owner.param.watch(self._resolve_value, [p.name for p in params])
            )


class NestedResolver(Resolver):

    object = Parameter(allow_refs=True, nested_refs=True)


class reactive_ops:
    """
    The reactive operations namespace.

    Provides reactive versions of operations that cannot be made reactive through
    operator overloading. This includes operations such as ``.rx.and_`` and ``.rx.bool``.

    Calling this namespace (``()``) creates and returns a reactive expression, enabling
    dynamic updates and computation tracking.

    Returns
    -------
    rx
        A reactive expression representing the operation applied to the current value.

    References
    ----------
    For more details, see the user guide:
    https://param.holoviz.org/user_guide/Reactive_Expressions.html#special-methods-on-rx

    Examples
    --------
    Create a Parameterized instance and access its reactive operations property:

    >>> import param
    >>> class P(param.Parameterized):
    ...     a = param.Number()
    >>> p = P(a=1)

    Retrieve the current value reactively:

    >>> a_value = p.param.a.rx.value

    Create a reactive expression by calling the namespace:

    >>> rx_expression = p.param.a.rx()

    Use special methods from the reactive ops namespace for reactive operations:

    >>> condition = p.param.a.rx.and_(True)
    >>> piped = p.param.a.rx.pipe(lambda x: x * 2)

    """

    def __init__(self, reactive):
        self._reactive = reactive

    def _as_rx(self):
        return self._reactive if isinstance(self._reactive, rx) else self()

    def __call__(self) -> 'rx':
        """Create a reactive expression."""
        rxi = self._reactive
        return rxi if isinstance(rxi, rx) else rx(rxi)

    def and_(self, other) -> 'rx':
        """
        Perform a logical AND operation with the given operand.

        This method computes a logical AND (``and``) operation between the current
        value of the reactive expression and the provided operand. The result is
        returned as a new reactive expression.

        Parameters
        ----------
        other : any
            The operand to combine with the current value using the AND operation.

        Returns
        -------
        rx
            A new reactive expression representing the result of the AND operation.

        Examples
        --------
        Create two reactive expressions and combine them using ``and_``:

        >>> import param
        >>> a = param.rx(True)
        >>> b = param.rx(False)
        >>> result = a.rx.and_(b)
        >>> result.rx.value
        False

        Combine a reactive expression with a static value:

        >>> result = a.rx.and_(True)
        >>> result.rx.value
        True
        """
        return self._as_rx()._apply_operator(lambda obj, other: obj and other, other)

    def bool(self) -> 'rx':
        """
        Evaluate the truthiness of the current object.

        This method computes the boolean value of the current reactive expression.
        The result is returned as a new reactive expression, allowing the truthiness
        of the object to be used in further reactive operations.

        Returns
        -------
        rx
            A new reactive expression representing the boolean value of the object.

        Examples
        --------
        Create a reactive expression and evaluate its truthiness:

        >>> import param
        >>> rx_value = param.rx(5)
        >>> rx_bool = rx_value.rx.bool()
        >>> rx_bool.rx.value
        True

        Evaluate the truthiness of an empty reactive expression:

        >>> rx_empty = param.rx([])
        >>> rx_bool = rx_empty.rx.bool()
        >>> rx_bool.rx.value
        False
        """
        return self._as_rx()._apply_operator(bool)

    def buffer(self, n) -> 'rx':
        """
        Collect the last ``n`` items emitted by the reactive expression.

        This method creates a new reactive expression that maintains a buffer of the
        most recent ``n`` items emitted by the current reactive expression. As new values
        are emitted, older values are discarded to keep the buffer size constant.

        Parameters
        ----------
        n : int
            The maximum number of items to retain in the buffer.

        Returns
        -------
        rx
            A new reactive expression containing the buffered items as a list.

        Examples
        --------
        Create a reactive expression and buffer the last 3 emitted items:

        >>> import param
        >>> rx_value = param.rx(1)
        >>> rx_buffer = rx_value.rx.buffer(3)
        >>> rx_buffer.rx.value
        [1]

        Emit new values and observe the buffered results:

        >>> rx_value.rx.value = 2
        >>> rx_value.rx.value = 3
        >>> rx_value.rx.value = 4
        >>> rx_buffer.rx.value
        [2, 3, 4]
        """
        items = []
        def collect(new, n):
            items.append(new)
            while len(items) > n:
                items.pop(0)
            return items
        return self._as_rx()._apply_operator(collect, n)

    def in_(self, other) -> 'rx':
        """
        Check if the current object is contained "in" the given operand.

        This method performs a containment check, equivalent to the ``in`` keyword in Python,
        but within a reactive expression. The result is returned as a new reactive expression.

        Parameters
        ----------
        other : any
            The collection or operand to check for containment.

        Returns
        -------
        rx
            A new reactive expression representing the result of the containment check.

        Examples
        --------
        Check if a reactive value is in a list:

        >>> import param
        >>> rx_value = param.rx(2)
        >>> rx_in = rx_value.rx.in_([1, 2, 3])
        >>> rx_in.rx.value
        True

        Check containment with a string:

        >>> rx_char = param.rx('a')
        >>> rx_in = rx_char.rx.in_('alphabet')
        >>> rx_in.rx.value
        True

        Update the reactive value and observe changes:

        >>> rx_char.rx.value = 'c'
        >>> rx_in.rx.value
        False
        """
        return self._as_rx()._apply_operator(operator.contains, other, reverse=True)

    def is_(self, other) -> 'rx':
        """
        Perform a logical "is" comparison with the given operand.

        This method checks if the current object is the same object (i.e., identical in
        memory) as the given operand. The result is returned as a new reactive expression.

        Parameters
        ----------
        other : any
            The operand to compare for object identity.

        Returns
        -------
        rx
            A new reactive expression representing the result of the "is" comparison.

        Examples
        --------
        Check if a reactive value refers to the same object:

        >>> import param
        >>> obj1 = object()
        >>> obj2 = object()
        >>> rx_obj = param.rx(obj1)
        >>> rx_is = rx_obj.rx.is_(obj1)
        >>> rx_is.rx.value
        True

        Compare with a different object:

        >>> rx_is = rx_obj.rx.is_(obj2)
        >>> rx_is.rx.value
        False

        Update the reactive value and re-check identity:

        >>> rx_obj.rx.value = obj2
        >>> rx_is.rx.value
        False
        """
        return self._as_rx()._apply_operator(operator.is_, other)

    def is_not(self, other) -> 'rx':
        """
        Perform a logical "is not" comparison with the given operand.

        This method checks if the current object is not the same object (i.e., not
        identical in memory) as the given operand. The result is returned as a new
        reactive expression.

        Parameters
        ----------
        other : any
            The operand to compare for non-identity.

        Returns
        -------
        rx
            A new reactive expression representing the result of the "is not" comparison.

        Examples
        --------
        Check if a reactive value is not the same object:

        >>> import param
        >>> obj1 = object()
        >>> obj2 = object()
        >>> rx_obj = param.rx(obj1)
        >>> rx_is_not = rx_obj.rx.is_not(obj2)
        >>> rx_is_not.rx.value
        True

        Compare with the same object:

        >>> rx_is_not = rx_obj.rx.is_not(obj1)
        >>> rx_is_not.rx.value
        False

        Update the reactive value and re-check non-identity:

        >>> rx_obj.rx.value = obj2
        >>> rx_is_not.rx.value
        True
        """
        return self._as_rx()._apply_operator(operator.is_not, other)

    def len(self) -> 'rx':
        """
        Return the length of the current object as a reactive expression.

        Since the ``__len__`` method cannot be overloaded reactively, this method
        provides a way to compute the length of the current reactive expression.
        The result is returned as a new reactive expression.

        Returns
        -------
        rx
            A new reactive expression representing the length of the object.

        Examples
        --------
        Compute the length of a reactive list:

        >>> import param
        >>> rx_list = param.rx([1, 2, 3])
        >>> rx_len = rx_list.rx.len()
        >>> rx_len.rx.value
        3

        Update the reactive list and observe the length update:

        >>> rx_list.rx.value = [1, 2, 3, 4, 5]
        >>> rx_len.rx.value
        5

        Compute the length of a reactive string:

        >>> rx_string = param.rx("Hello World")
        >>> rx_len = rx_string.rx.len()
        >>> rx_len.rx.value
        11
        """
        return self._as_rx()._apply_operator(len)

    def map(self, func, /, *args, **kwargs) -> 'rx':
        """
        Apply a function to each item in the reactive collection.

        This method applies a given function to every item in the current reactive
        collection and returns a new reactive expression containing the results.

        Parameters
        ----------
        func : callable
            The function to apply to each item in the collection.
        *args : iterable, optional
            Positional arguments to pass to the function.
        **kwargs : dict, optional
            Keyword arguments to pass to the function.

        Returns
        -------
        rx
            A new reactive expression containing the results of applying ``func``
            to each item.

        Raises
        ------
        TypeError
            If ``func`` is a generator or asynchronous generator function, which
            are not supported.

        Examples
        --------
        Apply a function to double each item in a reactive list:

        >>> import param
        >>> reactive_list = param.rx([1, 2, 3])
        >>> reactive_doubled = reactive_list.rx.map(lambda x: x * 2)
        >>> reactive_doubled.rx.value
        [2, 4, 6]

        Use positional arguments in the function:

        >>> reactive_offset = reactive_list.rx.map(lambda x, offset: x + offset, offset=10)
        >>> reactive_offset.rx.value
        [11, 12, 13]

        Use keyword arguments in the function:

        >>> reactive_power = reactive_list.rx.map(lambda x, power=1: x ** power, power=3)
        >>> reactive_power.rx.value
        [1, 8, 27]
        """
        if inspect.isasyncgenfunction(func) or inspect.isgeneratorfunction(func):
            raise TypeError(
                "Cannot map a generator function. Only regular function "
                "or coroutine functions are permitted."
            )
        if inspect.iscoroutinefunction(func):
            import asyncio
            async def apply(vs, *args, **kwargs):
                return list(await asyncio.gather(*(func(v, *args, **kwargs) for v in vs)))
        else:
            def apply(vs, *args, **kwargs):
                return [func(v, *args, **kwargs) for v in vs]
        return self._as_rx()._apply_operator(apply, *args, **kwargs)

    def not_(self) -> 'rx':
        """
        Perform a logical NOT operation on the current reactive value.

        This method computes the logical negation (``not``) of the current reactive
        expression and returns the result as a new reactive expression.

        Returns
        -------
        rx
            A new reactive expression representing the result of the NOT operation.

        Examples
        --------
        Apply a logical NOT operation to a reactive boolean:

        >>> import param
        >>> rx_bool = param.rx(True)
        >>> rx_not = rx_bool.rx.not_()
        >>> rx_not.rx.value
        False

        Update the reactive value and observe the change:

        >>> rx_bool.rx.value = False
        >>> rx_not.rx.value
        True
        """
        return self._as_rx()._apply_operator(operator.not_)

    def or_(self, other)-> 'rx':
        """
        Perform a logical OR operation with the given operand.

        This method computes a logical OR (``or``) operation between the current
        reactive value and the provided operand. The result is returned as a new
        reactive expression.

        Parameters
        ----------
        other : any
            The operand to combine with the current value using the OR operation.

        Returns
        -------
        rx
            A new reactive expression representing the result of the OR operation.

        Examples
        --------
        Combine two reactive boolean values using OR:

        >>> import param
        >>> rx_bool1 = param.rx(False)
        >>> rx_bool2 = param.rx(True)
        >>> rx_or = rx_bool1.rx.or_(rx_bool2)
        >>> rx_or.rx.value
        True

        Combine a reactive value with a static boolean:

        >>> rx_or_static = rx_bool1.rx.or_(True)
        >>> rx_or_static.rx.value
        True

        Update the reactive value and observe the change:

        >>> rx_bool1.rx.value = True
        >>> rx_or.rx.value
        True
        """
        return self._as_rx()._apply_operator(lambda obj, other: obj or other, other)

    def pipe(self, func, /, *args, **kwargs)-> 'rx':
        """
        Apply a chainable function to the current reactive value.

        This method allows applying a custom function to the current reactive
        expression. The result is returned as a new reactive expression, making
        it possible to create dynamic and chainable pipelines.

        Parameters
        ----------
        func : callable
            The function to apply to the current value.
        *args : iterable, optional
            Positional arguments to pass to the function.
        **kwargs : dict, optional
            Keyword arguments to pass to the function.

        Returns
        -------
        rx
            A new reactive expression representing the result of applying ``func``.

        Examples
        --------
        Apply a custom function to transform a reactive value:

        >>> import param
        >>> rx_value = param.rx(10)
        >>> rx_result = rx_value.rx.pipe(lambda x: x * 2)
        >>> rx_result.rx.value
        20

        Use positional arguments with the function:

        >>> def add(x, y):
        ...     return x + y
        >>> rx_result = rx_value.rx.pipe(add, 5)
        >>> rx_result.rx.value
        15

        Use keyword arguments with the function:

        >>> def multiply(x, factor=1):
        ...     return x * factor
        >>> rx_result = rx_value.rx.pipe(multiply, factor=3)
        >>> rx_result.rx.value
        30
        """
        return self._as_rx()._apply_operator(func, *args, **kwargs)

    def resolve(self, nested=True, recursive=False) -> 'rx':
        """
        Resolve references held by the reactive expression.

        This method resolves references within the reactive expression, replacing
        any references with their actual values. For example, if the expression
        contains a list of reactive parameters, this operation returns a list of
        their resolved values.

        Parameters
        ----------
        nested : bool, optional
            Whether to resolve references within nested objects such as tuples,
            lists, sets, and dictionaries. Default is True.
        recursive : bool, optional
            Whether to recursively resolve references. If a resolved reference
            itself contains further references, this option enables resolving
            them until no references remain. Default is False.

        Returns
        -------
        rx
            A new reactive expression containing the fully resolved values.

        Examples
        --------
        Resolve a simple reactive list of values:

        >>> import param
        >>> rx_list = param.rx([param.rx(1), param.rx(2), param.rx(3)])
        >>> resolved = rx_list.rx.resolve()
        >>> resolved.rx.value
        [1, 2, 3]

        Enable recursive resolution for deeper references:

        >>> rx_nested = param.rx({'key': param.rx([param.rx(10), param.rx(20)])})
        >>> resolved_nested = rx_nested.rx.resolve(recursive=True)
        >>> resolved_nested.rx.value
        {'key': [10, 20]}
        """
        resolver_type = NestedResolver if nested else Resolver
        resolver = resolver_type(object=self._reactive, recursive=recursive)
        return resolver.param.value.rx()

    def updating(self) -> 'rx':
        """
        Return a new expression that indicates whether the current expression is updating.

        This method creates a reactive expression that evaluates to ``True`` while the
        current expression is in the process of updating and ``False`` otherwise. This
        can be useful for tracking or reacting to the update state of an expression,
        such as displaying loading indicators or triggering conditional logic.

        Returns
        -------
        ReactiveExpression
            A reactive expression that is ``True`` while the current expression is updating
            and ``False`` otherwise.

        Examples
        --------
        Create a reactive expression and track its update state:

        >>> import param
        >>> rx_value = param.rx(1)

        Create an updating tracker:

        >>> updating = rx_value.rx.updating()
        >>> updating.rx.value
        False

        Simulate an update and observe the change in the updating tracker:

        >>> rx_value.rx.value = 2
        >>> updating.rx.value  # Becomes True during the update process, then False.
        False
        """
        wrapper = Wrapper(object=False)
        self._watch(lambda e: wrapper.param.update(object=True), precedence=-999)
        self._watch(lambda e: wrapper.param.update(object=False), precedence=999)
        return wrapper.param.object.rx()

    def when(self, *dependencies, initial=Undefined) -> 'rx':
        """
        Create a reactive expression that updates only when specified dependencies change.

        This method creates a new reactive expression that emits the value of the
        current expression only when one of the provided dependencies changes. If
        all dependencies are of type :class:`param.Event` and an initial value is provided,
        the expression will not be evaluated until the first event is triggered.

        Parameters
        ----------
        dependencies : Parameter or reactive expression rx
            Dependencies that trigger an update in the reactive expression.
        initial : object, optional
            A placeholder value that is used until a dependency event is triggered.
            Defaults to ``Undefined``.

        Returns
        -------
        rx
            A reactive expression that updates when the specified dependencies change.

        Examples
        --------
        Use ``.when`` to control when a reactive expression updates:

        >>> import param
        >>> from param import rx
        >>> from time import sleep

        Define an expensive function:

        >>> def expensive_function(a, b):
        ...     print(f'multiplying {a=} and {b=}')
        ...     sleep(1)
        ...     return a * b

        Create reactive values:

        >>> a = rx(1)
        >>> b = rx(2)

        Define a state with an event parameter:

        >>> class State(param.Parameterized):
        ...     submit = param.Event()

        >>> state = State()

        Create a gated reactive expression that only updates when the ``submit`` event is triggered:

        >>> gated_expr = rx(expensive_function)(a, b).rx.when(state.param.submit, initial="Initial Value")
        >>> gated_expr.rx.value
        'Initial Value'

        Trigger the update by setting the `submit` event:

        >>> state.submit = True
        >>> gated_expr.rx.value
        multiplying a=1 and b=2
        2
        """
        deps = [p for d in dependencies for p in resolve_ref(d)]
        is_event = all(isinstance(dep, Event) for dep in deps)
        def eval(*_, evaluated=[]):
            if is_event and initial is not Undefined and not evaluated:
                # Abuse mutable default value to keep track of evaluation state
                evaluated.append(True)
                return initial
            else:
                return self.value
        return bind(eval, *deps).rx()

    def where(self, x, y) -> 'rx':
        """
        Return either ``x`` or ``y`` depending on the current state of the expression.

        This method implements a reactive version of a ternary conditional expression.
        It evaluates the current reactive expression as a condition and returns ``x``
        if the condition is ``True``, or ``y`` if the condition is ``False``.

        Parameters
        ----------
        x : object
            The value to return if the reactive condition evaluates to ``True``.
        y : object
            The value to return if the reactive condition evaluates to ``False``.

        Returns
        -------
        rx
            A reactive expression that evaluates to ``x`` or ``y`` based on the current
            state of the condition.

        Examples
        --------
        Use ``.where`` to implement a reactive conditional:

        >>> import param
        >>> rx_value = param.rx(True)
        >>> rx_result = rx_value.rx.where("Condition is True", "Condition is False")

        Check the result when the condition is ``True``:

        >>> rx_result.rx.value
        'Condition is True'

        Change the reactive condition and observe the updated result:

        >>> rx_value.rx.value = False
        >>> rx_result.rx.value
        'Condition is False'

        Combine ``.where`` with reactive expressions for dynamic updates:

        >>> rx_num = param.rx(10)
        >>> rx_condition = rx_num > 5
        >>> rx_result = rx_condition.rx.where("Above 5", "5 or below")
        >>> rx_result.rx.value
        'Above 5'

        Update the reactive value and see the conditional result change:

        >>> rx_num.rx.value = 3
        >>> rx_result.rx.value
        '5 or below'
        """
        xrefs = resolve_ref(x)
        yrefs = resolve_ref(y)
        if isinstance(self._reactive, rx):
            params = self._reactive._params
        else:
            params = resolve_ref(self._reactive)
        trigger = Trigger(parameters=params)
        if xrefs:
            def trigger_x(*args):
                if self.value:
                    trigger.param.trigger('value')
            bind(trigger_x, *xrefs, watch=True)
        if yrefs:
            def trigger_y(*args):
                if not self.value:
                    trigger.param.trigger('value')
            bind(trigger_y, *yrefs, watch=True)

        def ternary(condition, _):
            return resolve_value(x) if condition else resolve_value(y)
        return bind(ternary, self._reactive, trigger.param.value)

    # Operations to get the output and set the input of an expression

    def set(self, value):
        """
        Set the input of the pipeline to a new value. Equivalent
        to ``.rx.value = value``.

        Parameters
        ----------
        value : object
            The value to set the pipeline input to.
        """
        self.value = value

    @property
    def value(self):
        """
        Get or set the current state of the reactive expression.

        When getting the value it evaluates the reactive expression, resolving all operations
        and dependencies to return the current value. The value reflects the
        latest state of the expression after applying all transformations or updates.

        Returns
        -------
        any
            The current value of the reactive expression, resolved based on the
            operations and dependencies in its pipeline.

        Examples
        --------
        Access the value of a basic reactive expression:

        >>> import param
        >>> rx_value = param.rx(10)
        >>> rx_value.rx.value
        10

        Update the reactive expression and retrieve the updated value:

        >>> rx_value.rx.value = 20
        >>> rx_value.rx.value
        20

        Evaluate a reactive pipeline:

        >>> rx_pipeline = rx_value.rx.pipe(lambda x: x * 2)
        >>> rx_pipeline.rx.value
        40
        """
        if isinstance(self._reactive, rx):
            return self._reactive._resolve()
        elif isinstance(self._reactive, Parameter):
            return getattr(self._reactive.owner, self._reactive.name)
        else:
            return self._reactive()

    @value.setter
    def value(self, new):
        """
        Get or set the current state of the reactive expression.

        When getting the value it evaluates the reactive expression, resolving all operations
        and dependencies to return the current value. The value reflects the
        latest state of the expression after applying all transformations or updates.

        Returns
        -------
        any
            The current value of the reactive expression, resolved based on the
            operations and dependencies in its pipeline.

        Examples
        --------
        Access the value of a basic reactive expression:

        >>> import param
        >>> rx_value = param.rx(10)
        >>> rx_value.rx.value
        10

        Update the reactive expression and retrieve the updated value:

        >>> rx_value.rx.value = 20
        >>> rx_value.rx.value
        20

        Evaluate a reactive pipeline:

        >>> rx_pipeline = rx_value.rx.pipe(lambda x: x * 2)
        >>> rx_pipeline.rx.value
        40
        """
        if isinstance(self._reactive, Parameter):
            raise AttributeError(
                "`Parameter.rx.value = value` is not supported. Cannot override "
                "parameter value."
            )
        elif not isinstance(self._reactive, rx):
            raise AttributeError(
                "`bind(...).rx.value = value` is not supported. Cannot override "
                "the output of a function."
            )
        elif self._reactive._root is not self._reactive:
            raise AttributeError(
                "The value of a derived expression cannot be set. Ensure you "
                "set the value on the root node wrapping a concrete value, e.g.:"
                "\n\n    a = rx(1)\n    b = a + 1\n    a.rx.value = 2\n\n "
                "is valid but you may not set `b.rx.value = 2`."
            )
        if self._reactive._wrapper is None:
            raise AttributeError(
                "Setting the value of a reactive expression is only "
                "supported if it wraps a concrete value. A reactive "
                "expression wrapping a Parameter or another dynamic "
                "reference cannot be updated."
            )
        self._reactive._wrapper.object = resolve_value(new)

    def watch(self, fn=None, onlychanged=True, queued=False, precedence=0):
        """
        Add a callback to observe changes in the reactive expression's output.

        This method allows you to attach a callable function (``fn``) that will be
        invoked whenever the output of the reactive expression changes. The callback
        can be either a regular or asynchronous function. If no callable is provided,
        the expression is eagerly evaluated whenever it updates.

        Parameters
        ----------
        fn : callable or coroutine function, optional
            The function to be called whenever the reactive expression changes.
            For function should accept a single argument, which is the new value
            of the reactive expression. If no function provided, the expression
            is simply evaluated eagerly.

        Raises
        ------
        ValueError
            If ``precedence`` is negative, as negative precedences are reserved
            for internal watchers.

        Examples
        --------
        Attach a regular callback to print the updated value:

        >>> import param
        >>> rx_value = param.rx(10)
        >>> rx_value.rx.watch(lambda v: print(f"Updated value: {v}"))

        Update the reactive value to trigger the callback:

        >>> rx_value.rx.value = 20
        Updated value: 20

        Attach an asynchronous callback:

        >>> import asyncio
        >>> async def async_callback(value):
        ...     await asyncio.sleep(1)
        ...     print(f"Async updated value: {value}")
        >>> rx_value.rx.watch(async_callback)

        Trigger the async callback:

        >>> rx_value.rx.value = 30
        Async updated value: 30  # Printed after a 1-second delay.
        """
        if precedence < 0:
            raise ValueError("User-defined watch callbacks must declare "
                             "a positive precedence. Negative precedences "
                             "are reserved for internal Watchers.")
        self._watch(fn, onlychanged=onlychanged, queued=queued, precedence=precedence)

    def _watch(self, fn=None, onlychanged=True, queued=False, precedence=0):
        def cb(value):
            from .parameterized import async_executor
            if iscoroutinefunction(fn):
                async_executor(partial(fn, value))
            elif fn is not None:
                fn(value)
        bind(cb, self._reactive, watch=True)


def bind(function, *args, watch: bool = False, **kwargs):
    """
    Bind constant values, parameters, bound functions or reactive expressions to a function.

    This function creates a wrapper around the given ``function``, binding some or
    all of its arguments to constant values, :class:`Parameter` objects, or
    reactive expressions. The resulting function automatically reflects updates
    to any bound parameters or reactive expressions, ensuring that its output
    remains up-to-date.

    Similar to :func:`functools.partial`, arguments can also be bound to constants,
    leaving a simple callable object. When ``watch=True``, the function is
    automatically evaluated whenever any bound parameter or reactive expression changes.

    Parameters
    ----------
    function : callable, generator, async generator, or coroutine
        The function or coroutine to bind constant, dynamic, or reactive arguments to.
        It can be:

        - A standard callable (e.g., a regular function).
        - A generator function (producing iterables).
        - An async generator function (producing asynchronous iterables).
        - A coroutine function (producing awaitables).
    *args : object, Parameter, bound function or reactive expression rx
        Positional arguments to bind to the function. These can be constants,
        `param.Parameter` objects, bound functions or reactive expressions.
    watch : bool, optional
        If `True`, the function is automatically evaluated whenever a bound
        parameter or reactive expression changes. Defaults to `False`.
    **kwargs : object, Parameter, bound function or reactive expression rx
        Keyword arguments to bind to the function. These can also be constants,
        `param.Parameter` objects, bound functions or reactive expressions.

    Returns
    -------
    callable, generator, async generator, or coroutine
        A new function with the bound arguments, annotated with all dependencies.
        The function reflects changes to bound parameters or reactive expressions.

    Examples
    --------
    Bind parameters to a function:

    >>> import param
    >>> class Example(param.Parameterized):
    ...     a = param.Number(1)
    ...     b = param.Number(2)
    >>> example = Example()
    >>> def add(a, b):
    ...     return a + b
    >>> bound_add = param.bind(add, example.param.a, example.param.b)
    >>> bound_add()
    3

    Update a parameter and observe the updated result:

    >>> example.a = 5
    >>> bound_add()
    7

    Automatically evaluate the function when bound arguments change:

    >>> bound_watch = param.bind(print, example.param.a, example.param.b, watch=True)
    >>> example.a = 1  # Triggers automatic evaluation
    1 2
    """
    args, kwargs = (
        tuple(transform_reference(arg) for arg in args),
        {key: transform_reference(arg) for key, arg in kwargs.items()}
    )
    dependencies = {}

    # If the wrapped function has a dependency add it
    fn_dep = transform_reference(function)
    if isinstance(fn_dep, Parameter) or hasattr(fn_dep, '_dinfo'):
        dependencies['__fn'] = fn_dep

    # Extract dependencies from args and kwargs
    for i, p in enumerate(args):
        if hasattr(p, '_dinfo'):
            for j, arg in enumerate(p._dinfo['dependencies']):
                dependencies[f'__arg{i}_arg{j}'] = arg
            for kw, kwarg in p._dinfo['kw'].items():
                dependencies[f'__arg{i}_arg_{kw}'] = kwarg
        elif isinstance(p, Parameter):
            dependencies[f'__arg{i}'] = p
    for kw, v in kwargs.items():
        if hasattr(v, '_dinfo'):
            for j, arg in enumerate(v._dinfo['dependencies']):
                dependencies[f'__kwarg_{kw}_arg{j}'] = arg
            for pkw, kwarg in v._dinfo['kw'].items():
                dependencies[f'__kwarg_{kw}_{pkw}'] = kwarg
        elif isinstance(v, Parameter):
            dependencies[kw] = v

    def combine_arguments(wargs, wkwargs, asynchronous=False):
        combined_args = []
        for arg in args:
            if hasattr(arg, '_dinfo'):
                arg = eval_function_with_deps(arg)
            elif isinstance(arg, Parameter):
                arg = getattr(arg.owner, arg.name)
            combined_args.append(arg)
        combined_args += list(wargs)

        combined_kwargs = {}
        for kw, arg in kwargs.items():
            if hasattr(arg, '_dinfo'):
                arg = eval_function_with_deps(arg)
            elif isinstance(arg, Parameter):
                arg = getattr(arg.owner, arg.name)
            combined_kwargs[kw] = arg
        for kw, arg in wkwargs.items():
            if asynchronous:
                if kw.startswith('__arg'):
                    index = kw[5:]
                    if index.isdigit():
                        combined_args[int(index)] = arg
                elif kw.startswith('__kwarg'):
                    substring = kw[8:]
                    if substring in combined_kwargs:
                        combined_kwargs[substring] = arg
                continue
            elif kw.startswith('__arg') or kw.startswith('__kwarg') or kw.startswith('__fn'):
                continue
            combined_kwargs[kw] = arg
        return combined_args, combined_kwargs

    def eval_fn():
        if callable(function):
            fn = function
        else:
            p = transform_reference(function)
            if isinstance(p, Parameter):
                fn = getattr(p.owner, p.name)
            else:
                fn = eval_function_with_deps(p)
        return fn

    if inspect.isgeneratorfunction(function):
        def wrapped(*wargs, **wkwargs):
            combined_args, combined_kwargs = combine_arguments(
                wargs, wkwargs, asynchronous=True
            )
            evaled = eval_fn()(*combined_args, **combined_kwargs)
            for val in evaled:
                yield val
        wrapper_fn = depends(**dependencies, watch=watch)(wrapped)
        wrapped._dinfo = wrapper_fn._dinfo
    elif inspect.isasyncgenfunction(function):
        async def wrapped(*wargs, **wkwargs):
            combined_args, combined_kwargs = combine_arguments(
                wargs, wkwargs, asynchronous=True
            )
            evaled = eval_fn()(*combined_args, **combined_kwargs)
            async for val in evaled:
                yield val
        wrapper_fn = depends(**dependencies, watch=watch)(wrapped)
        wrapped._dinfo = wrapper_fn._dinfo
    elif iscoroutinefunction(function):
        @depends(**dependencies, watch=watch)
        async def wrapped(*wargs, **wkwargs):
            combined_args, combined_kwargs = combine_arguments(
                wargs, wkwargs, asynchronous=True
            )
            evaled = eval_fn()(*combined_args, **combined_kwargs)
            return await evaled
    else:
        @depends(**dependencies, watch=watch)
        def wrapped(*wargs, **wkwargs):
            combined_args, combined_kwargs = combine_arguments(wargs, wkwargs)
            return eval_fn()(*combined_args, **combined_kwargs)
    wrapped.__bound_function__ = function
    wrapped.rx = reactive_ops(wrapped)
    _reactive_display_objs.add(wrapped)
    for name, accessor in _display_accessors.items():
        setattr(wrapped, name, accessor(wrapped))
    return wrapped

# When we only support python >= 3.11 we should exchange 'rx' with Self type annotation below.
# See https://peps.python.org/pep-0673/

class rx:
    """
    A class for creating reactive expressions by wrapping objects.

    The ``rx`` class allows you to wrap objects and operate on them interactively,
    recording any operations applied. These recorded operations form a pipeline
    that can be replayed dynamically when an operand changes. This makes ``rx``
    particularly useful for building reactive workflows, such as real-time data
    processing or dynamic user interfaces.

    Parameters
    ----------
    obj : any
        The object to wrap, such as a number, string, list, or any supported
        data structure.

    References
    ----------
    For more details, see the user guide:
    https://param.holoviz.org/user_guide/Reactive_Expressions.html

    Examples
    --------
    Instantiate :class:`rx` from an object:

    >>> from param import rx
    >>> reactive_float = rx(3.14)

    Perform operations on the reactive object:

    >>> reactive_result = reactive_float * 2
    >>> reactive_result.value
    6.28

    Update the original value and see the updated result:

    >>> reactive_float.value = 1
    >>> reactive_result.rx.value
    2

    Create a reactive list and compute its length reactively:

    >>> reactive_list = rx([1, 2, 3])
    >>> reactive_length = reactive_list.rx.len()
    >>> reactive_length.rx.value
    3
    """

    _accessors: dict[str, Callable[[rx], Any]] = {}

    _display_options: tuple[str] = ()

    _display_handlers: dict[type, tuple[Any, dict[str, Any]]] = {}

    _method_handlers: dict[str, Callable] = {}

    @classmethod
    def register_accessor(
        cls, name: str, accessor: Callable[[rx], Any],
        predicate: Optional[Callable[[Any], bool]] = None
    ):
        """
        Register an accessor that extends ``rx`` with custom behavior.

        Parameters
        ----------
        name: str
          The name of the accessor will be attribute-accessible under.
        accessor: Callable[[rx], any]
          A callable that will return the accessor namespace object
          given the ``rx`` object it is registered on.
        predicate: Callable[[Any], bool] | None

        """
        cls._accessors[name] = (accessor, predicate)

    @classmethod
    def register_display_handler(cls, obj_type, handler, **kwargs):
        """
        Register a display handler for a specific type of object.

        Makes it possible to define custom display options for
        specific objects.

        Parameters
        ----------
        obj_type: type | callable
          The type to register a custom display handler on.
        handler: Viewable | callable
          A Viewable or callable that is given the object to be displayed
          and the custom keyword arguments.
        kwargs: dict[str, Any]
          Additional display options to register for this type.

        """
        cls._display_handlers[obj_type] = (handler, kwargs)

    @classmethod
    def register_method_handler(cls, method, handler):
        """
        Register a handler that is called when a specific method on
        an object is called.
        """
        cls._method_handlers[method] = handler

    def __new__(cls, obj=None, **kwargs):
        wrapper = None
        obj = transform_reference(obj)
        if kwargs.get('fn'):
            # rx._clone codepath
            fn = kwargs.pop('fn')
            wrapper = kwargs.pop('_wrapper', None)
        elif inspect.isgeneratorfunction(obj) or iscoroutinefunction(obj):
            # Resolves generator and coroutine functions lazily
            wrapper = GenWrapper(object=obj)
            fn = bind(lambda obj: obj, wrapper.param.object)
            obj = Undefined
        elif isinstance(obj, (FunctionType, MethodType)) and hasattr(obj, '_dinfo'):
            # Bound functions and methods are resolved on access
            fn = obj
            obj = None
        elif isinstance(obj, Parameter):
            fn = bind(lambda obj: obj, obj)
            obj = getattr(obj.owner, obj.name)
        else:
            # For all other objects wrap them so they can be updated
            # via .rx.value property
            wrapper = Wrapper(object=obj)
            fn = bind(lambda obj: obj, wrapper.param.object)
        inst = super(rx, cls).__new__(cls)
        inst._fn = fn
        inst._shared_obj = kwargs.get('_shared_obj', None if obj is None else [obj])
        inst._wrapper = wrapper
        return inst

    def __init__(
        self, obj=None, operation=None, fn=None, depth=0, method=None, prev=None,
        _shared_obj=None, _current=None, _wrapper=None, **kwargs
    ):
        # _init is used to prevent to __getattribute__ to execute its
        # specialized code.
        self._init = False
        display_opts = {}
        for _, opts in self._display_handlers.values():
            for k, o in opts.items():
                display_opts[k] = o
        display_opts.update({
            dopt: kwargs.pop(dopt) for dopt in self._display_options + tuple(display_opts)
            if dopt in kwargs
        })
        self._display_opts = display_opts
        self._method = method
        self._operation = operation
        self._depth = depth
        self._dirty = _current is None
        self._dirty_obj = False
        self._current_task = None
        self._error_state = None
        self._current_ = _current
        if isinstance(obj, rx) and not prev:
            self._prev = obj
        else:
            self._prev = prev

        # Define special trigger parameter if operation has to be lazily evaluated
        if operation and (iscoroutinefunction(operation['fn']) or inspect.isgeneratorfunction(operation['fn'])):
            self._trigger = Trigger(internal=True)
            self._current_ = Undefined
        else:
            self._trigger = None
        self._root = self._compute_root()
        self._fn_params = self._compute_fn_params()
        self._internal_params = self._compute_params()
        # Filter params that external objects depend on, ensuring
        # that Trigger parameters do not cause double execution
        self._params = [
            p for p in self._internal_params if (not isinstance(p.owner, Trigger) or p.owner.internal)
            or any (p not in self._internal_params for p in p.owner.parameters)
        ]
        self._setup_invalidations(depth)
        self._kwargs = kwargs
        self._rx = reactive_ops(self)
        self._init = True
        for name, accessor in _display_accessors.items():
            setattr(self, name, accessor(self))
        for name, (accessor, predicate) in rx._accessors.items():
            if predicate is None or predicate(self._current):
                setattr(self, name, accessor(self))

    @property
    def rx(self) -> reactive_ops:
        """
        The reactive operations namespace.

        Provides reactive versions of operations that cannot be made reactive through
        operator overloading. This includes operations such as ``.rx.and_`` and ``.rx.bool``.

        References
        ----------
        For more details, see the user guide:
        https://param.holoviz.org/user_guide/Reactive_Expressions.html#special-methods-on-rx

        Examples
        --------
        Create a reactive expression:

        >>> import param
        >>> rx_expression = param.rx(1)

        Retrieve the current value reactively:

        >>> a_value = rx_expression.rx.value

        Use special methods from the reactive ops namespace for reactive operations:

        >>> condition = rx_expression.rx.and_(True)
        >>> piped = rx_expression.rx.pipe(lambda x: x * 2)
        """
        return self._rx

    @property
    def _obj(self):
        if self._shared_obj is None:
            self._obj = eval_function_with_deps(self._fn)
        elif self._root._dirty_obj:
            root = self._root
            root._shared_obj[0] = eval_function_with_deps(root._fn)
            root._dirty_obj = False
        return self._shared_obj[0]

    @_obj.setter
    def _obj(self, obj):
        if self._shared_obj is None:
            self._shared_obj = [obj]
        else:
            self._shared_obj[0] = obj

    @property
    def _current(self):
        if self._error_state:
            raise self._error_state
        elif self._dirty or self._root._dirty_obj:
            self._resolve()
        return self._current_

    def _compute_root(self):
        if self._prev is None:
            return self
        root = self
        while root._prev is not None:
            root = root._prev
        return root

    def _compute_fn_params(self) -> list[Parameter]:
        if self._fn is None:
            return []

        owner = get_method_owner(self._fn)
        if owner is not None:
            deps = [
                dep.pobj for dep in owner.param.method_dependencies(self._fn.__name__)
            ]
            return deps

        dinfo = getattr(self._fn, '_dinfo', {})
        args = list(dinfo.get('dependencies', []))
        kwargs = list(dinfo.get('kw', {}).values())
        return args + kwargs

    def _compute_params(self) -> list[Parameter]:
        ps = list(self._fn_params)
        if self._trigger:
            ps.append(self._trigger.param.value)

        # Collect parameters on previous objects in chain
        prev = self._prev
        while prev is not None:
            for p in prev._params:
                if p not in ps:
                    ps.append(p)
            prev = prev._prev

        if self._operation is None:
            return ps

        # Accumulate dependencies in args and/or kwargs
        for ref in resolve_ref(self._operation['fn']):
            if ref not in ps:
                ps.append(ref)
        for arg in list(self._operation['args'])+list(self._operation['kwargs'].values()):
            for ref in resolve_ref(arg, recursive=True):
                if ref not in ps:
                    ps.append(ref)

        return ps

    def _setup_invalidations(self, depth: int = 0):
        """
        Since the parameters of the pipeline can change at any time
        we have to invalidate the internal state of the pipeline.
        To handle both invalidations of the inputs of the pipeline
        and the pipeline itself we set up watchers on both.

        1. The first invalidation we have to set up is to re-evaluate
           the function that feeds the pipeline. Only the root node of
           a pipeline has to perform this invalidation because all
           leaf nodes inherit the same shared_obj. This avoids
           evaluating the same function for every branch of the pipeline.
        2. The second invalidation is for the pipeline itself, i.e.
           if any parameter changes we have to notify the pipeline that
           it has to re-evaluate the pipeline. This is done by marking
           the pipeline as `_dirty`. The next time the `_current` value
           is requested the value is resolved by re-executing the
           pipeline.
        """
        if self._fn is not None:
            for _, params in full_groupby(self._fn_params, lambda x: id(x.owner)):
                fps = [p.name for p in params if p in self._root._fn_params]
                if fps:
                    params[0].owner.param._watch(self._invalidate_obj, fps, precedence=-1)
        for _, params in full_groupby(self._internal_params, lambda x: id(x.owner)):
            params[0].owner.param._watch(self._invalidate_current, [p.name for p in params], precedence=-1)

    def _invalidate_current(self, *events):
        if all(event.obj is self._trigger for event in events):
            return
        self._dirty = True
        self._error_state = None

    def _invalidate_obj(self, *events):
        self._root._dirty_obj = True
        self._error_state = None

    async def _resolve_async(self, obj):
        import asyncio
        self._current_task = task = asyncio.current_task()
        if inspect.isasyncgen(obj):
            async for val in obj:
                if self._current_task is not task:
                    break
                self._current_ = val
                self._trigger.param.trigger('value')
        else:
            value = await obj
            if self._current_task is task:
                self._current_ = value
                self._trigger.param.trigger('value')

    def _lazy_resolve(self, obj):
        from .parameterized import async_executor
        if inspect.isgenerator(obj):
            obj = _to_async_gen(obj)
        async_executor(partial(self._resolve_async, obj))

    def _resolve(self):
        if self._error_state:
            raise self._error_state
        elif self._dirty or self._root._dirty_obj:
            try:
                obj = self._obj if self._prev is None else self._prev._resolve()
                if obj is Skip or obj is Undefined:
                    self._current_ = Undefined
                    raise Skip
                operation = self._operation
                if operation:
                    obj = self._eval_operation(obj, operation)
                    if inspect.isasyncgen(obj) or inspect.iscoroutine(obj) or inspect.isgenerator(obj):
                        self._lazy_resolve(obj)
                        obj = Skip
                    if obj is Skip:
                        raise Skip
            except Skip:
                self._dirty = False
                return self._current_
            except Exception as e:
                self._error_state = e
                raise e
            self._current_ = current = obj
        else:
            current = self._current_
        self._dirty = False
        if self._method:
            # E.g. `pi = dfi.A` leads to `pi._method` equal to `'A'`.
            current = getattr(current, self._method, current)
        if hasattr(current, '__call__'):
            self.__call__.__func__.__doc__ = self.__call__.__doc__
        return current

    def _transform_output(self, obj):
        """Apply custom display handlers before their output."""
        applies = False
        for predicate, (handler, opts) in self._display_handlers.items():
            display_opts = {
                k: v for k, v in self._display_opts.items() if k in opts
            }
            display_opts.update(self._kwargs)
            try:
                applies = predicate(obj, **display_opts)
            except TypeError:
                applies = predicate(obj)
            if applies:
                new = handler(obj, **display_opts)
                if new is not obj:
                    return new
        return obj

    @property
    def _callback(self):
        params = self._params
        def evaluate(*args, **kwargs):
            out = self._current
            if self._method:
                out = getattr(out, self._method)
            return self._transform_output(out)
        if params:
            return bind(evaluate, *params)
        return evaluate

    def _clone(self, operation=None, copy=False, **kwargs) -> 'rx':
        operation = operation or self._operation
        depth = self._depth + 1
        if copy:
            kwargs = dict(
                self._kwargs, _current=self._current, method=self._method,
                prev=self._prev, **kwargs
            )
        else:
            kwargs = dict(prev=self, **dict(self._kwargs, **kwargs))
        kwargs = dict(self._display_opts, **kwargs)
        return type(self)(
            self._obj, operation=operation, depth=depth, fn=self._fn,
            _shared_obj=self._shared_obj, _wrapper=self._wrapper,
            **kwargs
        )

    def __dir__(self):
        current = self._current
        if self._method:
            current = getattr(current, self._method)
        extras = {attr for attr in dir(current) if not attr.startswith('_')}
        try:
            return sorted(set(super().__dir__()) | extras)
        except Exception:
            return sorted(set(dir(type(self))) | set(self.__dict__) | extras)

    def _resolve_accessor(self):
        if not self._method:
            # No method is yet set, as in `dfi.A`, so return a copied clone.
            return self._clone(copy=True)
        # This is executed when one runs e.g. `dfi.A > 1`, in which case after
        # dfi.A the _method 'A' is set (in __getattribute__) which allows
        # _resolve_accessor to record the attribute access as an operation.
        operation = {
            'fn': getattr,
            'args': (self._method,),
            'kwargs': {},
            'reverse': False
        }
        self._method = None
        return self._clone(operation)

    def __getattribute__(self, name):
        self_dict = super().__getattribute__('__dict__')
        if not self_dict.get('_init') or name == 'rx' or name.startswith('_'):
            return super().__getattribute__(name)

        current = self_dict['_current_']
        dirty = self_dict['_dirty']
        if dirty:
            self._resolve()
            current = self_dict['_current_']

        method = self_dict['_method']
        if method:
            current = getattr(current, method)
        # Getting all the public attributes available on the current object,
        # e.g. `sum`, `head`, etc.
        extras = [d for d in dir(current) if not d.startswith('_')]
        if (name in extras or current is Undefined) and name not in super().__dir__():
            new = self._resolve_accessor()
            # Setting the method name for a potential use later by e.g. an
            # operator or method, as in `dfi.A > 2`. or `dfi.A.max()`
            new._method = name
            try:
                new.__doc__ = getattr(current, name).__doc__
            except Exception:
                pass
            return new
        return super().__getattribute__(name)

    def __call__(self, *args, **kwargs):
        new = self._clone(copy=True)
        method = new._method or '__call__'
        if method == '__call__' and self._depth == 0 and not hasattr(self._current, '__call__'):
            return self.set_display(*args, **kwargs)

        if method in rx._method_handlers:
            handler = rx._method_handlers[method]
            method = handler(self)
        new._method = None
        kwargs = dict(kwargs)
        operation = {
            'fn': method,
            'args': args,
            'kwargs': kwargs,
            'reverse': False
        }
        return new._clone(operation)

    #----------------------------------------------------------------
    # rx pipeline APIs
    #----------------------------------------------------------------

    def __array_ufunc__(self, ufunc, method, *args, **kwargs):
        new = self._resolve_accessor()
        operation = {
            'fn': getattr(ufunc, method),
            'args': args[1:],
            'kwargs': kwargs,
            'reverse': False
        }
        return new._clone(operation)

    def _apply_operator(self, operator, *args, reverse=False, **kwargs) -> 'rx':
        new = self._resolve_accessor()
        operation = {
            'fn': operator,
            'args': args,
            'kwargs': kwargs,
            'reverse': reverse
        }
        return new._clone(operation)

    # Builtin functions

    def __abs__(self):
        return self._apply_operator(abs)

    def __str__(self):
        return self._apply_operator(str)

    def __round__(self, ndigits=None):
        args = () if ndigits is None else (ndigits,)
        return self._apply_operator(round, *args)

    # Unary operators
    def __ceil__(self):
        return self._apply_operator(math.ceil)
    def __floor__(self):
        return self._apply_operator(math.floor)
    def __invert__(self):
        return self._apply_operator(operator.inv)
    def __neg__(self):
        return self._apply_operator(operator.neg)
    def __pos__(self):
        return self._apply_operator(operator.pos)
    def __trunc__(self):
        return self._apply_operator(math.trunc)

    # Binary operators
    def __add__(self, other):
        return self._apply_operator(operator.add, other)
    def __and__(self, other):
        return self._apply_operator(operator.and_, other)
    def __contains_(self, other):
        return self._apply_operator(operator.contains, other)
    def __divmod__(self, other):
        return self._apply_operator(divmod, other)
    def __eq__(self, other):
        return self._apply_operator(operator.eq, other)
    def __floordiv__(self, other):
        return self._apply_operator(operator.floordiv, other)
    def __ge__(self, other):
        return self._apply_operator(operator.ge, other)
    def __gt__(self, other):
        return self._apply_operator(operator.gt, other)
    def __le__(self, other):
        return self._apply_operator(operator.le, other)
    def __lt__(self, other):
        return self._apply_operator(operator.lt, other)
    def __lshift__(self, other):
        return self._apply_operator(operator.lshift, other)
    def __matmul__(self, other):
        return self._apply_operator(operator.matmul, other)
    def __mod__(self, other):
        return self._apply_operator(operator.mod, other)
    def __mul__(self, other):
        return self._apply_operator(operator.mul, other)
    def __ne__(self, other):
        return self._apply_operator(operator.ne, other)
    def __or__(self, other):
        return self._apply_operator(operator.or_, other)
    def __rshift__(self, other):
        return self._apply_operator(operator.rshift, other)
    def __pow__(self, other):
        return self._apply_operator(operator.pow, other)
    def __sub__(self, other):
        return self._apply_operator(operator.sub, other)
    def __truediv__(self, other):
        return self._apply_operator(operator.truediv, other)
    def __xor__(self, other):
        return self._apply_operator(operator.xor, other)

    # Reverse binary operators
    def __radd__(self, other):
        return self._apply_operator(operator.add, other, reverse=True)
    def __rand__(self, other):
        return self._apply_operator(operator.and_, other, reverse=True)
    def __rdiv__(self, other):
        return self._apply_operator(operator.div, other, reverse=True)
    def __rdivmod__(self, other):
        return self._apply_operator(divmod, other, reverse=True)
    def __rfloordiv__(self, other):
        return self._apply_operator(operator.floordiv, other, reverse=True)
    def __rlshift__(self, other):
        return self._apply_operator(operator.rlshift, other)
    def __rmod__(self, other):
        return self._apply_operator(operator.mod, other, reverse=True)
    def __rmul__(self, other):
        return self._apply_operator(operator.mul, other, reverse=True)
    def __ror__(self, other):
        return self._apply_operator(operator.or_, other, reverse=True)
    def __rpow__(self, other):
        return self._apply_operator(operator.pow, other, reverse=True)
    def __rrshift__(self, other):
        return self._apply_operator(operator.rrshift, other)
    def __rsub__(self, other):
        return self._apply_operator(operator.sub, other, reverse=True)
    def __rtruediv__(self, other):
        return self._apply_operator(operator.truediv, other, reverse=True)
    def __rxor__(self, other):
        return self._apply_operator(operator.xor, other, reverse=True)

    def __getitem__(self, other):
        return self._apply_operator(operator.getitem, other)

    def __iter__(self):
        if isinstance(self._current, Iterator):
            while True:
                try:
                    new = self._apply_operator(next)
                    new.rx.value
                except RuntimeError:
                    break
                yield new
            return
        elif not isinstance(self._current, Iterable):
            raise TypeError(f'cannot unpack non-iterable {type(self._current).__name__} object.')
        items = self._apply_operator(list)
        for i in range(len(self._current)):
            yield items[i]

    def __bool__(self):
        # Implemented otherwise truth value testing (e.g. if rx: ...)
        # defers to __len__ which raises an error.
        return True

    def __len__(self):
        raise TypeError(
            'len(<rx_obj>) is not supported. Use `<rx_obj>.rx.len()` to '
            'obtain the length as a reactive expression, or '
            '`len(<rx_obj>.rx.value)` to obtain the length of the underlying '
            'expression value.'
        )

    def _eval_operation(self, obj, operation):
        fn, args, kwargs = operation['fn'], operation['args'], operation['kwargs']
        resolved_args = []
        for arg in args:
            val = resolve_value(arg)
            if val is Skip or val is Undefined:
                raise Skip
            resolved_args.append(val)
        resolved_kwargs = {}
        for k, arg in kwargs.items():
            val = resolve_value(arg)
            if val is Skip or val is Undefined:
                raise Skip
            resolved_kwargs[k] = val
        if isinstance(fn, str):
            obj = getattr(obj, fn)(*resolved_args, **resolved_kwargs)
        elif operation.get('reverse'):
            obj = fn(resolved_args[0], obj, *resolved_args[1:], **resolved_kwargs)
        else:
            obj = fn(obj, *resolved_args, **resolved_kwargs)
        return obj

    def __setattr__(self, name, value):
        # Setting value instead of rx.value is a common user mistake.
        # They are more but we don't want to restrict __setattr__ too much
        # so only catch value, for now.
        if name == "value":
            raise AttributeError(
                "'rx' has no attribute 'value', try "
                "'<reactive_expr>.rx.value = <val>'."
            )
        super().__setattr__(name, value)


def _rx_transform(obj):
    if not isinstance(obj, rx):
        return obj
    return bind(lambda *_: obj.rx.value, *obj._params)

register_reference_transform(_rx_transform)
