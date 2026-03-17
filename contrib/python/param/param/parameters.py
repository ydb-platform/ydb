"""
Parameters are a kind of class attribute allowing special behavior,
including dynamically generated parameter values, documentation
strings, constant and read-only parameters, and type or range checking
at assignment time.

Potentially useful for any large Python program that needs
user-modifiable object attributes; see the Parameter and Parameterized
classes for more information.  If you do not want to add a dependency
on external code by importing from a separately installed param
package, you can simply save this file as param.py and copy it and
parameterized.py directly into your own package.

This file contains subclasses of Parameter, implementing specific
parameter types (e.g. Number), and also imports the definition of
Parameters and Parameterized classes.
"""

import collections
import copy
import datetime as dt
import glob
import inspect
import numbers
import os.path
import pathlib
import re
import sys
import typing
import warnings

from collections import OrderedDict
from collections.abc import Iterable
from contextlib import contextmanager

from .parameterized import (
    Parameterized, Parameter, ParameterizedFunction, ParamOverrides, String,
    Undefined, get_logger, instance_descriptor, _dt_types,
    _int_types
)
from ._utils import (
    ParamDeprecationWarning as _ParamDeprecationWarning,
    _find_stack_level,
    _validate_error_prefix,
    _deserialize_from_path,
    _named_objs,
    _produce_value,
    _get_min_max_value,
    _is_number,
    concrete_descendents,  # noqa: F401
    descendents as _descendents,
    _abbreviate_paths,
    _to_datetime,
)

#-----------------------------------------------------------------------------
# Utilities
#-----------------------------------------------------------------------------


def param_union(*parameterizeds, warn=True):
    """
    Given a set of :class:`Parameterized` objects, returns a dictionary
    with the union of all param name,value pairs across them.

    Parameters
    ----------
    warn : bool, optional
        Wether to warn if the same parameter have been given multiple values,
        otherwise use the last value, by default True

    Returns
    -------
    dict
        Union of all param name,value pairs

    """
    d = {}
    for o in parameterizeds:
        for k in o.param:
            if k != 'name':
                if k in d and warn:
                    get_logger().warning(f"overwriting parameter {k}")
                d[k] = getattr(o, k)
    return d


def guess_param_types(**kwargs):
    """
    Given a set of keyword literals, promote to the appropriate
    parameter type based on some simple heuristics.
    """
    params = {}
    for k, v in kwargs.items():
        kws = dict(default=v, constant=True)
        if isinstance(v, Parameter):
            params[k] = v
        elif isinstance(v, _dt_types):
            params[k] = Date(**kws)
        elif isinstance(v, bool):
            params[k] = Boolean(**kws)
        elif isinstance(v, int):
            params[k] = Integer(**kws)
        elif isinstance(v, float):
            params[k] = Number(**kws)
        elif isinstance(v, str):
            params[k] = String(**kws)
        elif isinstance(v, dict):
            params[k] = Dict(**kws)
        elif isinstance(v, tuple):
            if all(_is_number(el) for el in v):
                params[k] = NumericTuple(**kws)
            elif len(v) == 2 and all(isinstance(el, _dt_types) for el in v):
                params[k] = DateRange(**kws)
            else:
                params[k] = Tuple(**kws)
        elif isinstance(v, list):
            params[k] = List(**kws)
        else:
            if 'numpy' in sys.modules:
                from numpy import ndarray
                if isinstance(v, ndarray):
                    params[k] = Array(**kws)
                    continue
            if 'pandas' in sys.modules:
                from pandas import (
                    DataFrame as pdDFrame, Series as pdSeries
                )
                if isinstance(v, pdDFrame):
                    params[k] = DataFrame(**kws)
                    continue
                elif isinstance(v, pdSeries):
                    params[k] = Series(**kws)
                    continue
            params[k] = Parameter(**kws)

    return params


def parameterized_class(name, params, bases=Parameterized):
    """
    Dynamically create a parameterized class with the given name and the
    supplied parameters, inheriting from the specified base(s).
    """
    if not isinstance(bases, (list, tuple)):
        bases=[bases]
    return type(name, tuple(bases), params)


def guess_bounds(params, **overrides):
    """
    Given a dictionary of :class:`Parameter` instances, return a corresponding
    set of copies with the bounds appropriately set.


    If given a set of override keywords, use those numeric tuple bounds.
    """
    guessed = {}
    for name, p in params.items():
        new_param = copy.copy(p)
        if isinstance(p, (Integer, Number)):
            if name in overrides:
                minv,maxv = overrides[name]
            else:
                minv, maxv, _ = _get_min_max_value(None, None, value=p.default)
            new_param.bounds = (minv, maxv)
        guessed[name] = new_param
    return guessed


def get_soft_bounds(bounds, softbounds):
    """
    For each soft bound (upper and lower), if there is a defined bound
    (not equal to None) and does not exceed the hard bound, then it is
    returned. Otherwise it defaults to the hard bound. The hard bound
    could still be None.
    """
    if bounds is None:
        hl, hu = (None, None)
    else:
        hl, hu = bounds

    if softbounds is None:
        sl, su = (None, None)
    else:
        sl, su = softbounds

    if sl is None or (hl is not None and sl<hl):
        l = hl
    else:
        l = sl

    if su is None or (hu is not None and su>hu):
        u = hu
    else:
        u = su

    return (l, u)


class Infinity:
    """
    An instance of this class represents an infinite value. Unlike
    Python's float('inf') value, this object can be safely compared
    with gmpy2 numeric types across different gmpy2 versions.

    All operators on Infinity() return Infinity(), apart from the
    comparison and equality operators. Equality works by checking
    whether the two objects are both instances of this class.
    """

    def __eq__  (self,other): return isinstance(other,self.__class__)
    def __ne__  (self,other): return not self==other
    def __lt__  (self,other): return False
    def __le__  (self,other): return False
    def __gt__  (self,other): return True
    def __ge__  (self,other): return True
    def __add__ (self,other): return self
    def __radd__(self,other): return self
    def __ladd__(self,other): return self
    def __sub__ (self,other): return self
    def __iadd__ (self,other): return self
    def __isub__(self,other): return self
    def __repr__(self):       return "Infinity()"
    def __str__ (self):       return repr(self)



class Time(Parameterized):
    """
    A callable object returning a number for the current time.

    Here 'time' is an abstract concept that can be interpreted in any
    useful way.  For instance, in a simulation, it would be the
    current simulation time, while in a turn-taking game it could be
    the number of moves so far.  The key intended usage is to allow
    independent Parameterized objects with Dynamic parameters to
    remain consistent with a global reference.

    The time datatype (time_type) is configurable, but should
    typically be an exact numeric type like an integer or a rational,
    so that small floating-point errors do not accumulate as time is
    incremented repeatedly.

    When used as a context manager using the 'with' statement
    (implemented by the __enter__ and __exit__ special methods), entry
    into a context pushes the state of the Time object, allowing the
    effect of changes to the time value to be explored by setting,
    incrementing or decrementing time as desired. This allows the
    state of time-dependent objects to be modified temporarily as a
    function of time, within the context's block. For instance, you
    could use the context manager to "see into the future" to collect
    data over multiple times, without affecting the global time state
    once exiting the context. Of course, you need to be careful not to
    do anything while in context that would affect the lasting state
    of your other objects, if you want things to return to their
    starting state when exiting the context.

    The starting time value of a new Time object is 0, converted to
    the chosen time type. Here is an illustration of how time can be
    manipulated using a Time object:

    >>> time = Time(until=20, timestep=1)
    >>> 'The initial time is %s' % time()
    'The initial time is 0'
    >>> 'Setting the time to %s' % time(5)
    'Setting the time to 5'
    >>> time += 5
    >>> 'After incrementing by 5, the time is %s' % time()
    'After incrementing by 5, the time is 10'
    >>> with time as t:  # Entering a context
    ...     'Time before iteration: %s' % t()
    ...     'Iteration: %s' % [val for val in t]
    ...     'Time after iteration: %s' % t()
    ...     t += 2
    ...     'The until parameter may be exceeded outside iteration: %s' % t()
    'Time before iteration: 10'
    'Iteration: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]'
    'Time after iteration: 20'
    'The until parameter may be exceeded outside iteration: 22'
    >>> 'After exiting the context the time is back to %s' % time()
    'After exiting the context the time is back to 10'
    """

    _infinitely_iterable = True

    forever = Infinity()

    label= String(default='Time', doc="""
         The label given to the Time object. Can be used to convey
         more specific notions of time as appropriate. For instance,
         the label could be 'Simulation Time' or 'Duration'.""")


    time_type = Parameter(default=int, constant=True, doc="""
        Callable that Time will use to convert user-specified time
        values into the current time; all times will be of the resulting
        numeric type.

        By default, time is of integer type, but you can supply any
        arbitrary-precision type like a fixed-point decimal or a
        rational, to allow fractional times.  Floating-point times are
        also allowed, but are not recommended because they will suffer
        from accumulated rounding errors.  For instance, incrementing
        a floating-point value 0.0 by 0.05, 20 times, will not reach
        1.0 exactly.  Instead, it will be slightly higher than 1.0,
        because 0.05 cannot be represented exactly in a standard
        floating point numeric type. Fixed-point or rational types
        should be able to handle such computations exactly, avoiding
        accumulation issues over long time intervals.

        Some potentially useful exact number classes:

         - int: Suitable if all times can be expressed as integers.

         - Python's decimal.Decimal and fractions.Fraction classes:
           widely available but slow and also awkward to specify times
           (e.g. cannot simply type 0.05, but have to use a special
           constructor or a string).

         - fixedpoint.FixedPoint: Allows a natural representation of
           times in decimal notation, but very slow and needs to be
           installed separately.

         - gmpy2.mpq: Allows a natural representation of times in
           decimal notation, and very fast because it uses the GNU
           Multi-Precision library, but needs to be installed
           separately and depends on a non-Python library.  gmpy2.mpq
           is gmpy2's rational type.
        """)

    timestep = Parameter(default=1.0,doc="""
        Stepsize to be used with the iterator interface.
        Time can be advanced or decremented by any value, not just
        those corresponding to the stepsize, and so this value is only
        a default.""")

    until = Parameter(default=forever,doc="""
         Declaration of an expected end to time values, if any.  When
         using the iterator interface, iteration will end before this
         value is exceeded.""")

    unit = String(default=None, doc="""
        The units of the time dimensions. The default of None is set
        as the global time function may on an arbitrary time base.

        Typical values for the parameter are 'seconds' (the SI unit
        for time) or subdivisions thereof (e.g. 'milliseconds').""")


    def __init__(self, **params):
        super().__init__(**params)
        self._time = self.time_type(0)
        self._exhausted = None
        self._pushed_state = []


    def __eq__(self, other):
        if not isinstance(other, Time):
            return False
        self_params = (self.timestep,self.until)
        other_params = (other.timestep,other.until)
        if self_params != other_params:
            return False
        return True


    def __ne__(self, other):
        return not (self == other)


    def __iter__(self): return self


    def __next__(self):
        timestep = self.time_type(self.timestep)

        if self._exhausted is None:
            self._exhausted = False
        elif (self._time + timestep) <= self.until:
            self._time += timestep
        else:
            self._exhausted = None
            raise StopIteration
        return self._time

    def __call__(self, val=None, time_type=None):
        """
        When called with no arguments, returns the current time value.

        When called with a specified val, sets the time to it.

        When called with a specified time_type, changes the time_type
        and sets the current time to the given val (which *must* be
        specified) converted to that time type.  To ensure that
        the current state remains consistent, this is normally the only
        way to change the time_type of an existing Time instance.
        """
        if time_type and val is None:
            raise Exception("Please specify a value for the new time_type.")
        if time_type:
            type_param = self.param.objects('existing').get('time_type')
            type_param.constant = False
            self.time_type = time_type
            type_param.constant = True
        if val is not None:
            self._time = self.time_type(val)

        return self._time


    def advance(self, val):
        self += val


    def __iadd__(self, other):
        self._time = self._time + self.time_type(other)
        return self


    def __isub__(self, other):
        self._time = self._time - self.time_type(other)
        return self


    def __enter__(self):
        """Enter the context and push the current state."""
        self._pushed_state.append((self._time, self.timestep, self.until))
        self.in_context = True
        return self


    def __exit__(self, exc, *args):
        """
        Exit from the current context, restoring the previous state.
        The StopIteration exception raised in context will force the
        context to exit. Any other exception exc that is raised in the
        block will not be caught.
        """
        (self._time, self.timestep, self.until) = self._pushed_state.pop()
        self.in_context = len(self._pushed_state) != 0
        if exc is StopIteration:
            return True

#-----------------------------------------------------------------------------
# Parameters
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Dynamic/Number
#-----------------------------------------------------------------------------


class Dynamic(Parameter):
    """
    Parameter whose value can be generated dynamically by a callable
    object.

    If a Parameter is declared as Dynamic, it can be set a callable
    object (such as a function or callable class), and getting the
    parameter's value will call that callable.

    Note that at present, the callable object must allow attributes
    to be set on itself.

    If set as ``time_dependent``, setting the ``Dynamic.time_fn`` allows the
    production of dynamic values to be controlled: a new value will be
    produced only if the current value of ``time_fn`` is different from
    what it was the last time the parameter value was requested.

    By default, the Dynamic parameters are not ``time_dependent`` so that
    new values are generated on every call regardless of the time. The
    default ``time_fn`` used when ``time_dependent`` is a single :class:`Time` instance
    that allows general manipulations of time. It may be set to some
    other callable as required so long as a number is returned on each
    call.
    """

    time_fn = Time()
    time_dependent = False

    @typing.overload
    def __init__(
        self, default=None, *,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        """
        Call the superclass's __init__ and set instantiate=True if the
        default is dynamic.
        """
        super().__init__(default=default, **params)

        if callable(self.default):
            self._set_instantiate(True)
            self._initialize_generator(self.default)


    def _initialize_generator(self,gen,obj=None):
        """Add 'last time' and 'last value' attributes to the generator."""
        # Could use a dictionary to hold these things.
        if hasattr(obj,"_Dynamic_time_fn"):
            gen._Dynamic_time_fn = obj._Dynamic_time_fn

        gen._Dynamic_last = None
        # Would have usede None for this, but can't compare a fixedpoint
        # number with None (e.g. 1>None but FixedPoint(1)>None can't be done)
        gen._Dynamic_time = -1

        gen._saved_Dynamic_last = []
        gen._saved_Dynamic_time = []


    def __get__(self,obj,objtype):
        """
        Call the superclass's __get__; if the result is not dynamic
        return that result, otherwise ask that result to produce a
        value and return it.
        """
        gen = super().__get__(obj,objtype)

        if not hasattr(gen,'_Dynamic_last'):
            return gen
        else:
            return self._produce_value(gen)


    @instance_descriptor
    def __set__(self,obj,val):
        """
        Call the superclass's set and keep this parameter's
        instantiate value up to date (dynamic parameters
        must be instantiated).

        If val is dynamic, initialize it as a generator.
        """
        super().__set__(obj,val)

        dynamic = callable(val)
        if dynamic: self._initialize_generator(val,obj)
        if obj is None: self._set_instantiate(dynamic)


    def _produce_value(self,gen,force=False):
        """
        Return a value from gen.

        If there is no time_fn, then a new value will be returned
        (i.e. gen will be asked to produce a new value).

        If force is True, or the value of time_fn() is different from
        what it was was last time _produce_value was called, a new
        value will be produced and returned. Otherwise, the last value
        gen produced will be returned.
        """
        if hasattr(gen,"_Dynamic_time_fn"):
            time_fn = gen._Dynamic_time_fn
        else:
            time_fn = self.time_fn

        if (time_fn is None) or (not self.time_dependent):
            value = _produce_value(gen)
            gen._Dynamic_last = value
        else:

            time = time_fn()

            if force or time!=gen._Dynamic_time:
                value = _produce_value(gen)
                gen._Dynamic_last = value
                gen._Dynamic_time = time
            else:
                value = gen._Dynamic_last

        return value


    def _value_is_dynamic(self,obj,objtype=None):
        """
        Return True if the parameter is actually dynamic (i.e. the
        value is being generated).
        """
        return hasattr(super().__get__(obj,objtype),'_Dynamic_last')


    def _inspect(self,obj,objtype=None):
        """Return the last generated value for this parameter."""
        gen=super().__get__(obj,objtype)

        if hasattr(gen,'_Dynamic_last'):
            return gen._Dynamic_last
        else:
            return gen


    def _force(self,obj,objtype=None):
        """Force a new value to be generated, and return it."""
        gen=super().__get__(obj,objtype)

        if hasattr(gen,'_Dynamic_last'):
            return self._produce_value(gen,force=True)
        else:
            return gen


class Number(Dynamic):
    """
    A numeric :class:`Dynamic` Parameter, with a default value and optional bounds.

    There are two types of bounds: ``bounds`` and
    ``softbounds``.  ``bounds`` are hard bounds: the parameter must
    have a value within the specified range.  The default bounds are
    ``(None, None)``, meaning there are actually no hard bounds. One or
    both bounds can be set by specifying a value
    (e.g. ``bounds=(None, 10)`` means there is no lower bound, and an upper
    bound of 10). Bounds are inclusive by default, but exclusivity
    can be specified for each bound by setting inclusive_bounds
    (e.g. ``inclusive_bounds=(True, False)`` specifies an exclusive upper
    bound).

    Number is also a type of :class:`Dynamic` parameter, so its value
    can be set to a callable to get a dynamically generated
    number.

    When not being dynamically generated, bounds are checked when a
    Number is created or set. Using a default value outside the hard
    bounds, or one that is not numeric, results in an exception. When
    being dynamically generated, bounds are checked when the value
    of a Number is requested. A generated value that is not numeric,
    or is outside the hard bounds, results in an exception.

    As a special case, if ``allow_None=True`` (which is true by default if
    the parameter has a default of ``None`` when declared) then a value
    of ``None`` is also allowed.

    A separate method :meth:`set_in_bounds` is provided that will
    silently crop the given value into the legal range, for use
    in, for instance, a GUI.

    ``softbounds`` are present to indicate the typical range of
    the parameter, but are not enforced. Setting the soft bounds
    allows, for instance, a GUI to know what values to display on
    sliders for the Number.

    Example of creating a Number::

      AB = Number(default=0.5, bounds=(None,10), softbounds=(0,1), doc='Distance from A to B.')

    """

    __slots__ = ['bounds', 'softbounds', 'inclusive_bounds', 'step']

    _slot_defaults = dict(
        Dynamic._slot_defaults, default=0.0, bounds=None, softbounds=None,
        inclusive_bounds=(True,True), step=None,
    )

    @typing.overload
    def __init__(
        self,
        default=0.0, *, bounds=None, softbounds=None, inclusive_bounds=(True,True), step=None,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, bounds=Undefined, softbounds=Undefined,
                 inclusive_bounds=Undefined, step=Undefined, **params):
        """
        Initialize this parameter object and store the bounds.

        Non-dynamic default values are checked against the bounds.
        """
        super().__init__(default=default, **params)
        self.bounds = bounds
        self.inclusive_bounds = inclusive_bounds
        self.softbounds = softbounds
        self.step = step
        self._validate(self.default)

    def __get__(self, obj, objtype):
        """Retrieve the value of the attribute, checking bounds if dynamically generated.

        Parameters
        ----------
        obj: Parameterized | None
            The instance the attribute is accessed on, or `None` for class access.
        objtype: type[Parameterized]
            The class that owns the attribute.

        Returns
        -------
        The value of the attribute, potentially after applying bounds checks.
        """
        result = super().__get__(obj, objtype)

        # Should be able to optimize this commonly used method by
        # avoiding extra lookups (e.g. _value_is_dynamic() is also
        # looking up 'result' - should just pass it in).
        if self._value_is_dynamic(obj, objtype):
            self._validate(result)
        return result

    def set_in_bounds(self,obj,val):
        """
        Set to the given value, but cropped to be within the legal bounds.
        All objects are accepted, and no exceptions will be raised.  See
        crop_to_bounds for details on how cropping is done.
        """
        if not callable(val):
            bounded_val = self.crop_to_bounds(val)
        else:
            bounded_val = val
        super().__set__(obj, bounded_val)

    def crop_to_bounds(self, val):
        """
        Return the given value cropped to be within the hard bounds
        for this parameter.

        If a numeric value is passed in, check it is within the hard
        bounds. If it is larger than the high bound, return the high
        bound. If it's smaller, return the low bound. In either case, the
        returned value could be None.  If a non-numeric value is passed
        in, set to be the default value (which could be None).  In no
        case is an exception raised; all values are accepted.

        As documented in https://github.com/holoviz/param/issues/80,
        currently does not respect exclusive bounds, which would
        strictly require setting to one less for integer values or
        an epsilon less for floats.
        """
        # Values outside the bounds are silently cropped to
        # be inside the bounds.
        if _is_number(val):
            if self.bounds is None:
                return val
            vmin, vmax = self.bounds
            if vmin is not None:
                if val < vmin:
                    return  vmin

            if vmax is not None:
                if val > vmax:
                    return vmax

        elif self.allow_None and val is None:
            return val

        else:
            # non-numeric value sent in: reverts to default value
            return self.default

        return val

    def _validate_bounds(self, val, bounds, inclusive_bounds):
        if bounds is None or (val is None and self.allow_None) or callable(val):
            return
        vmin, vmax = bounds
        incmin, incmax = inclusive_bounds
        if vmax is not None:
            if incmax is True:
                if not val <= vmax:
                    raise ValueError(
                        f"{_validate_error_prefix(self)} must be at most "
                        f"{vmax}, not {val}."
                    )
            else:
                if not val < vmax:
                    raise ValueError(
                        f"{_validate_error_prefix(self)} must be less than "
                        f"{vmax}, not {val}."
                    )

        if vmin is not None:
            if incmin is True:
                if not val >= vmin:
                    raise ValueError(
                        f"{_validate_error_prefix(self)} must be at least "
                        f"{vmin}, not {val}."
                    )
            else:
                if not val > vmin:
                    raise ValueError(
                        f"{_validate_error_prefix(self)} must be greater than "
                        f"{vmin}, not {val}."
                    )

    def _validate_value(self, val, allow_None):
        if (allow_None and val is None) or (callable(val) and not inspect.isgeneratorfunction(val)):
            return

        if not _is_number(val):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes numeric values, "
                f"not {type(val)}."
            )

    def _validate_step(self, val, step):
        if step is not None and not _is_number(step):
            raise ValueError(
                f"{_validate_error_prefix(self, 'step')} can only be "
                f"None or a numeric value, not {type(step)}."
            )

    def _validate(self, val):
        """
        Check that the value is numeric and that it is within the hard
        bounds; if not, an exception is raised.
        """
        self._validate_value(val, self.allow_None)
        self._validate_step(val, self.step)
        self._validate_bounds(val, self.bounds, self.inclusive_bounds)

    def get_soft_bounds(self):
        return get_soft_bounds(self.bounds, self.softbounds)

    def __setstate__(self,state):
        if 'step' not in state:
            state['step'] = None

        super().__setstate__(state)



class Integer(Number):
    """Numeric Parameter required to be an Integer."""

    _slot_defaults = dict(Number._slot_defaults, default=0)

    def _validate_value(self, val, allow_None):
        if callable(val):
            return

        if allow_None and val is None:
            return

        if not isinstance(val, _int_types):
            raise ValueError(
                f"{_validate_error_prefix(self)} must be an integer, "
                f"not {type(val)}."
            )

    def _validate_step(self, val, step):
        if step is not None and not isinstance(step, int):
            raise ValueError(
                f"{_validate_error_prefix(self, 'step')} can only be "
                f"None or an integer value, not {type(step)}."
            )


class Magnitude(Number):
    """Numeric Parameter required to be in the range ``[0.0-1.0]``."""

    _slot_defaults = dict(Number._slot_defaults, default=1.0, bounds=(0.0,1.0))

    @typing.overload
    def __init__(
        self,
        default=1.0, *, bounds=(0.0, 1.0), softbounds=None, inclusive_bounds=(True,True), step=None,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, bounds=Undefined, softbounds=Undefined,
                 inclusive_bounds=Undefined, step=Undefined, **params):
        super().__init__(
            default=default, bounds=bounds, softbounds=softbounds,
            inclusive_bounds=inclusive_bounds, step=step, **params
        )


class Date(Number):
    """Date parameter of datetime or date type."""

    _slot_defaults = dict(Number._slot_defaults, default=None)

    @typing.overload
    def __init__(
        self,
        default=None, *, bounds=None, softbounds=None, inclusive_bounds=(True,True), step=None,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **kwargs):
        super().__init__(default=default, **kwargs)

    def _validate_value(self, val, allow_None):
        """
        Check that the value is numeric and that it is within the hard
        bounds; if not, an exception is raised.
        """
        if self.allow_None and val is None:
            return

        if not isinstance(val, _dt_types) and not (allow_None and val is None):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes datetime and "
                f"date types, not {type(val)}."
            )

    def _validate_step(self, val, step):
        if step is not None and not isinstance(step, _dt_types):
            raise ValueError(
                f"{_validate_error_prefix(self, 'step')} can only be None, "
                f"a datetime or date type, not {type(step)}."
            )

    def _validate_bounds(self, val, bounds, inclusive_bounds):
        val = _to_datetime(val)
        bounds = None if bounds is None else map(_to_datetime, bounds)
        return super()._validate_bounds(val, bounds, inclusive_bounds)

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        if not isinstance(value, (dt.datetime, dt.date)): # i.e np.datetime64
            value = value.astype(dt.datetime)
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")


class CalendarDate(Number):
    """Parameter specifically allowing dates (not datetimes)."""

    _slot_defaults = dict(Number._slot_defaults, default=None)

    @typing.overload
    def __init__(
        self,
        default=None, *, bounds=None, softbounds=None, inclusive_bounds=(True,True), step=None,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **kwargs):
        super().__init__(default=default, **kwargs)

    def _validate_value(self, val, allow_None):
        """
        Check that the value is numeric and that it is within the hard
        bounds; if not, an exception is raised.
        """
        if self.allow_None and val is None:
            return

        if (not isinstance(val, dt.date) or isinstance(val, dt.datetime)) and not (allow_None and val is None):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes date types."
            )

    def _validate_step(self, val, step):
        if step is not None and not isinstance(step, dt.date):
            raise ValueError(
                f"{_validate_error_prefix(self, 'step')} can only be None or "
                f"a date type, not {type(step)}."
            )

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        return value.strftime("%Y-%m-%d")

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        return dt.datetime.strptime(value, "%Y-%m-%d").date()

#-----------------------------------------------------------------------------
# Boolean
#-----------------------------------------------------------------------------

class Boolean(Parameter):
    """Binary or tristate Boolean Parameter."""

    _slot_defaults = dict(Parameter._slot_defaults, default=False)

    @typing.overload
    def __init__(
        self,
        default=False, *,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        super().__init__(default=default, **params)
        self._validate(self.default)

    def _validate_value(self, val, allow_None):
        if allow_None:
            if not isinstance(val, bool) and val is not None:
                raise ValueError(
                    f"{_validate_error_prefix(self)} only takes a "
                    f"boolean value or None, not {val!r}."
                )
        elif not isinstance(val, bool):
            raise ValueError(
                f"{_validate_error_prefix(self)} must be True or False, "
                f"not {val!r}."
            )

    def _validate(self, val):
        self._validate_value(val, self.allow_None)


class Event(Boolean):
    """
    An Event Parameter is one whose value is intimately linked to the
    triggering of events for watchers to consume. Event has a boolean
    value, which when set to ``True`` triggers the associated watchers (as
    any Parameter does) and then is automatically set back to
    ``False``. Conversely, if events are triggered directly via
    :meth:`~parameterized.Parameters.trigger`, the value is transiently set
    to ``True`` (so that it's clear which of
    many parameters being watched may have changed), then restored to
    ``False`` when the triggering completes. An Event parameter is thus like
    a momentary switch or pushbutton with a transient ``True`` value that
    serves only to launch some other action (e.g. via a :func:`depends`
    decorator), rather than encapsulating the action itself as
    :class:`param.Action` does.
    """

    # _autotrigger_value specifies the value used to set the parameter
    # to when the parameter is supplied to the trigger method. This
    # value change is then what triggers the watcher callbacks.
    __slots__ = ['_autotrigger_value', '_mode', '_autotrigger_reset_value']

    @typing.overload
    def __init__(
        self,
        default=False, *,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self,default=False,**params):
        self._autotrigger_value = True
        self._autotrigger_reset_value = False
        self._mode = 'set-reset'
        # Mode can be one of 'set', 'set-reset' or 'reset'

        # 'set' is normal Boolean parameter behavior when set with a value.
        # 'set-reset' temporarily sets the parameter (which triggers
        # watching callbacks) but immediately resets the value back to
        # False.
        # 'reset' applies the reset from True to False without
        # triggering watched callbacks

        # This _mode attribute is one of the few places where a specific
        # parameter has a special behavior that is relied upon by the
        # core functionality implemented in
        # parameterized.py. Specifically, the ``update`` method
        # temporarily sets this attribute in order to disable resetting
        # back to False while triggered callbacks are executing
        super().__init__(default=default,**params)

    def _reset_event(self, obj, val):
        val = False
        if obj is None:
            self.default = val
        else:
            obj._param__private.values[self.name] = val
        self._post_setter(obj, val)

    @instance_descriptor
    def __set__(self, obj, val):
        if self._mode in ['set-reset', 'set']:
            super().__set__(obj, val)
        if self._mode in ['set-reset', 'reset']:
            self._reset_event(obj, val)

#-----------------------------------------------------------------------------
# Tuple
#-----------------------------------------------------------------------------

class __compute_length_of_default:
    def __call__(self, p):
        return len(p.default)

    def __repr__(self):
        return repr(self.sig)

    @property
    def sig(self):
        return None


_compute_length_of_default = __compute_length_of_default()


class Tuple(Parameter):
    """A tuple Parameter (e.g. ``('a', 7.6, [3,5])``) with a fixed tuple length."""

    __slots__ = ['length']

    _slot_defaults = dict(Parameter._slot_defaults, default=(0,0), length=_compute_length_of_default)

    @typing.overload
    def __init__(
        self,
        default=(0,0), *, length=None,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, length=Undefined, **params):
        """
        Initialize a tuple parameter with a fixed length (number of
        elements).  The length is determined by the initial default
        value, if any, and must be supplied explicitly otherwise.  The
        length is not allowed to change after instantiation.
        """
        super().__init__(default=default, **params)
        if length is Undefined and self.default is None:
            raise ValueError(
                f"{_validate_error_prefix(self, 'length')} must be "
                "specified if no default is supplied."
            )
        elif default is not Undefined and default:
            self.length = len(default)
        else:
            self.length = length
        self._validate(self.default)

    def _validate_value(self, val, allow_None):
        if val is None and allow_None:
            return

        if not isinstance(val, tuple):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes a tuple value, "
                f"not {type(val)}."
            )

    def _validate_length(self, val, length):
        if val is None and self.allow_None:
            return

        if not len(val) == length:
            raise ValueError(
                f"{_validate_error_prefix(self, 'length')} is not "
                f"of the correct length ({len(val)} instead of {length})."
            )

    def _validate(self, val):
        self._validate_value(val, self.allow_None)
        self._validate_length(val, self.length)

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        return list(value) # As JSON has no tuple representation

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        return tuple(value) # As JSON has no tuple representation


class NumericTuple(Tuple):
    """A numeric tuple Parameter (e.g. ``(4.5, 7.6, 3)``) with a fixed tuple length."""

    def _validate_value(self, val, allow_None):
        super()._validate_value(val, allow_None)
        if allow_None and val is None:
            return
        for n in val:
            if _is_number(n):
                continue
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes numeric "
                f"values, not {type(n)}."
            )


class XYCoordinates(NumericTuple):
    """A NumericTuple for an X,Y coordinate."""

    _slot_defaults = dict(NumericTuple._slot_defaults, default=(0.0, 0.0))

    @typing.overload
    def __init__(
        self,
        default=(0.0, 0.0), *, length=None,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        super().__init__(default=default, length=2, **params)


class Range(NumericTuple):
    """A numeric range with optional bounds and softbounds."""

    __slots__ = ['bounds', 'inclusive_bounds', 'softbounds', 'step']

    _slot_defaults = dict(
        NumericTuple._slot_defaults, default=None, bounds=None,
        inclusive_bounds=(True,True), softbounds=None, step=None
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, bounds=None, softbounds=None, inclusive_bounds=(True,True), step=None, length=None,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, bounds=Undefined, softbounds=Undefined,
                 inclusive_bounds=Undefined, step=Undefined, **params):
        self.bounds = bounds
        self.inclusive_bounds = inclusive_bounds
        self.softbounds = softbounds
        self.step = step
        super().__init__(default=default,length=2,**params)

    def _validate(self, val):
        super()._validate(val)
        self._validate_bounds(val, self.bounds, self.inclusive_bounds, 'bound')
        self._validate_bounds(val, self.softbounds, self.inclusive_bounds, 'softbound')
        self._validate_step(val, self.step)
        self._validate_order(val, self.step, allow_None=self.allow_None)

    def _validate_step(self, val, step):
        if step is not None:
            if not _is_number(step):
                raise ValueError(
                    f"{_validate_error_prefix(self, 'step')} can only be None "
                    f"or a numeric value, not {type(step)}."
                )
            elif step == 0:
                raise ValueError(
                    f"{_validate_error_prefix(self, 'step')} cannot be 0."
                )

    def _validate_order(self, val, step, allow_None):
        if val is None and allow_None:
            return
        elif val is not None and (val[0] is None or val[1] is None):
            return

        start, end = val
        if step is not None and step > 0 and not start <= end:
            raise ValueError(
                f"{_validate_error_prefix(self)} end {end} is less than its "
                f"start {start} with positive step {step}."
            )
        elif step is not None and step < 0 and not start >= end:
            raise ValueError(
                f"{_validate_error_prefix(self)} start {start} is less than its "
                f"start {end} with negative step {step}."
            )

    def _validate_bound_type(self, value, position, kind):
        if not _is_number(value):
            raise ValueError(
                f"{_validate_error_prefix(self)} {position} {kind} can only be "
                f"None or a numerical value, not {type(value)}."
            )

    def _validate_bounds(self, val, bounds, inclusive_bounds, kind):
        if bounds is not None:
            for pos, v in zip(['lower', 'upper'], bounds):
                if v is None:
                    continue
                self._validate_bound_type(v, pos, kind)
        if kind == 'softbound':
            return

        if bounds is None or (val is None and self.allow_None):
            return
        vmin, vmax = bounds
        incmin, incmax = inclusive_bounds
        for bound, v in zip(['lower', 'upper'], val):
            too_low = (vmin is not None) and (v < vmin if incmin else v <= vmin)
            too_high = (vmax is not None) and (v > vmax if incmax else v >= vmax)
            if too_low or too_high:
                raise ValueError(
                    f"{_validate_error_prefix(self)} {bound} bound must be in "
                    f"range {self.rangestr()}, not {v}."
                )

    def get_soft_bounds(self):
        return get_soft_bounds(self.bounds, self.softbounds)

    def rangestr(self):
        vmin, vmax = self.bounds
        incmin, incmax = self.inclusive_bounds
        incmin = '[' if incmin else '('
        incmax = ']' if incmax else ')'
        return f'{incmin}{vmin}, {vmax}{incmax}'


class DateRange(Range):
    """
    A datetime or date range specified as ``(start, end)``.

    Bounds must be specified as datetime or date types (see ``param._dt_types``).
    """

    def _validate_bound_type(self, value, position, kind):
        if not isinstance(value, _dt_types):
            raise ValueError(
                f"{_validate_error_prefix(self)} {position} {kind} can only be "
                f"None or a date/datetime value, not {type(value)}."
            )

    def _validate_bounds(self, val, bounds, inclusive_bounds, kind):
        val = None if val is None else tuple(map(_to_datetime, val))
        bounds = None if bounds is None else tuple(map(_to_datetime, bounds))
        super()._validate_bounds(val, bounds, inclusive_bounds, kind)

    def _validate_value(self, val, allow_None):
        # Cannot use super()._validate_value as DateRange inherits from
        # NumericTuple which check that the tuple values are numbers and
        # datetime objects aren't numbers.
        if allow_None and val is None:
            return

        if not isinstance(val, tuple):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes a tuple value, "
                f"not {type(val)}."
            )
        for n in val:
            if isinstance(n, _dt_types):
                continue
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes date/datetime "
                f"values, not {type(n)}."
            )

        start, end = val
        if not end >= start:
            raise ValueError(
                f"{_validate_error_prefix(self)} end datetime {val[1]} "
                f"is before start datetime {val[0]}."
            )

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        # List as JSON has no tuple representation
        serialized = []
        for v in value:
            if not isinstance(v, (dt.datetime, dt.date)): # i.e np.datetime64
                v = v.astype(dt.datetime)
            # Separate date and datetime to deserialize to the right type.
            if type(v) is dt.date:
                v = v.strftime("%Y-%m-%d")
            else:
                v = v.strftime("%Y-%m-%dT%H:%M:%S.%f")
            serialized.append(v)
        return serialized

    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        deserialized = []
        for v in value:
            # Date
            if len(v) == 10:
                v = dt.datetime.strptime(v, "%Y-%m-%d").date()
            # Datetime
            else:
                v = dt.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
            deserialized.append(v)
        # As JSON has no tuple representation
        return tuple(deserialized)


class CalendarDateRange(Range):
    """A date range specified as ``(start_date, end_date)``."""

    def _validate_value(self, val, allow_None):
        if allow_None and val is None:
            return

        for n in val:
            if not isinstance(n, dt.date):
                raise ValueError(
                    f"{_validate_error_prefix(self)} only takes date types, "
                    f"not {val}."
                )

        start, end = val
        if not end >= start:
            raise ValueError(
                f"{_validate_error_prefix(self)} end date {val[1]} is before "
                f"start date {val[0]}."
            )

    def _validate_bound_type(self, value, position, kind):
        if not isinstance(value, dt.date):
            raise ValueError(
                f"{_validate_error_prefix(self)} {position} {kind} can only be "
                f"None or a date value, not {type(value)}."
            )

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        # As JSON has no tuple representation
        return [v.strftime("%Y-%m-%d") for v in value]

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        # As JSON has no tuple representation
        return tuple([dt.datetime.strptime(v, "%Y-%m-%d").date() for v in value])

#-----------------------------------------------------------------------------
# Callable
#-----------------------------------------------------------------------------

class Callable(Parameter):
    """
    Parameter holding a value that is a callable object, such as a function.

    A keyword argument ``instantiate=True`` should be provided when a
    function object is used that might have state.  On the other hand,
    regular standalone functions cannot be deepcopied as of Python
    2.4, so instantiate must be False for those values.
    """

    @typing.overload
    def __init__(
        self,
        default=None, *,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        super().__init__(default=default, **params)
        self._validate(self.default)

    def _validate_value(self, val, allow_None):
        if (allow_None and val is None) or callable(val):
            return
        raise ValueError(
            f"{_validate_error_prefix(self)} only takes a callable object, "
            f"not objects of {type(val)}."
        )

    def _validate(self, val):
        self._validate_value(val, self.allow_None)


class Action(Callable):
    """
    A user-provided function that can be invoked like a class or object method using ().
    In a GUI, this might be mapped to a button, but it can be invoked directly as well.
    """
# Currently same implementation as Callable, but kept separate to allow different handling in GUIs

#-----------------------------------------------------------------------------
# Composite
#-----------------------------------------------------------------------------

class Composite(Parameter):
    """
    A Parameter that is a composite of a set of other attributes of the class.

    The constructor argument ``attribs`` takes a list of attribute
    names, which may or may not be Parameters.  Getting the parameter
    returns a list of the values of the constituents of the composite,
    in the order specified.  Likewise, setting the parameter takes a
    sequence of values and sets the value of the constituent
    attributes.

    This Parameter type has not been tested with watchers and
    dependencies, and may not support them properly.
    """

    __slots__ = ['attribs', 'objtype']

    @typing.overload
    def __init__(
        self,
        *, attribs=None,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, *, attribs=Undefined, **kw):
        if attribs is Undefined:
            attribs = []
        super().__init__(default=Undefined, **kw)
        self.attribs = attribs

    def __get__(self, obj, objtype):
        """Return the values of all the attribs, as a list."""
        if obj is None:
            return [getattr(objtype, a) for a in self.attribs]
        else:
            return [getattr(obj, a) for a in self.attribs]

    def _validate_attribs(self, val, attribs):
        if len(val) == len(attribs):
            return
        raise ValueError(
            f"{_validate_error_prefix(self)} got the wrong number "
            f"of values (needed {len(attribs)}, but got {len(val)})."
        )

    def _validate(self, val):
        self._validate_attribs(val, self.attribs)

    def _post_setter(self, obj, val):
        if obj is None:
            for a, v in zip(self.attribs, val):
                setattr(self.objtype, a, v)
        else:
            for a, v in zip(self.attribs, val):
                setattr(obj, a, v)

#-----------------------------------------------------------------------------
# Selector
#-----------------------------------------------------------------------------

class SelectorBase(Parameter):
    """
    Parameter whose value must be chosen from a list of possibilities.

    Subclasses must implement get_range().
    """

    def get_range(self):
        raise NotImplementedError("get_range() must be implemented in subclasses.")


class ListProxy(list):
    """
    Container that supports both list-style and dictionary-style
    updates. Useful for replacing code that originally accepted lists
    but needs to support dictionary access (typically for naming
    items).
    """

    def __init__(self, iterable, parameter=None):
        super().__init__(iterable)
        self._parameter = parameter

    def _warn(self, method):
        clsname = type(self._parameter).__name__
        get_logger().warning(
            '{clsname}.objects{method} is deprecated if objects attribute '
            'was declared as a dictionary. Use `{clsname}.objects[label] '
            '= value` instead.'.format(clsname=clsname, method=method)
        )

    @contextmanager
    def _trigger(self, trigger=True):
        trigger = 'objects' in self._parameter.watchers and trigger
        old = dict(self._parameter.names) or list(self._parameter._objects)
        yield
        if trigger:
            value = self._parameter.names or self._parameter._objects
            self._parameter._trigger_event('objects', old, value)

    def __getitem__(self, index):
        if self._parameter.names:
            return self._parameter.names[index]
        return super().__getitem__(index)

    def __setitem__(self, index, object, trigger=True):
        if isinstance(index, (int, slice)):
            if self._parameter.names:
                self._warn('[index] = object')
            with self._trigger():
                super().__setitem__(index, object)
                self._parameter._objects[index] = object
            return
        if self and not self._parameter.names:
            self._parameter.names = _named_objs(self)
        with self._trigger(trigger):
            if index in self._parameter.names:
                old = self._parameter.names[index]
                idx = self.index(old)
                super().__setitem__(idx, object)
                self._parameter._objects[idx] = object
            else:
                super().append(object)
                self._parameter._objects.append(object)
            self._parameter.names[index] = object

    def __eq__(self, other):
        eq = super().__eq__(other)
        if self._parameter.names and eq is NotImplemented:
            return dict(zip(self._parameter.names, self)) == other
        return eq

    def __ne__(self, other):
        return not self.__eq__(other)

    def append(self, object):
        if self._parameter.names:
            self._warn('.append')
        with self._trigger():
            super().append(object)
            self._parameter._objects.append(object)

    def copy(self):
        if self._parameter.names:
            return self._parameter.names.copy()
        return list(self)

    def clear(self):
        with self._trigger():
            super().clear()
            self._parameter._objects.clear()
            self._parameter.names.clear()

    def extend(self, objects):
        if self._parameter.names:
            self._warn('.append')
        with self._trigger():
            super().extend(objects)
            self._parameter._objects.extend(objects)

    def get(self, key, default=None):
        if self._parameter.names:
            return self._parameter.names.get(key, default)
        return _named_objs(self).get(key, default)

    def insert(self, index, object):
        if self._parameter.names:
            self._warn('.insert')
        with self._trigger():
            super().insert(index, object)
            self._parameter._objects.insert(index, object)

    def items(self):
        if self._parameter.names:
            return self._parameter.names.items()
        return _named_objs(self).items()

    def keys(self):
        if self._parameter.names:
            return self._parameter.names.keys()
        return _named_objs(self).keys()

    def pop(self, *args):
        index = args[0] if args else -1
        if isinstance(index, int):
            with self._trigger():
                super().pop(index)
                object = self._parameter._objects.pop(index)
                if self._parameter.names:
                    self._parameter.names = {
                        k: v for k, v in self._parameter.names.items()
                        if v is object
                    }
            return
        if self and not self._parameter.names:
            raise ValueError(
                'Cannot pop an object from {clsname}.objects if '
                'objects was not declared as a dictionary.'
            )
        with self._trigger():
            object = self._parameter.names.pop(*args)
            super().remove(object)
            self._parameter._objects.remove(object)
        return object

    def remove(self, object):
        with self._trigger():
            super().remove(object)
            self._parameter._objects.remove(object)
            if self._parameter.names:
                copy = self._parameter.names.copy()
                self._parameter.names.clear()
                self._parameter.names.update({
                    k: v for k, v in copy.items() if v is not object
                })

    def update(self, objects, **items):
        if not self._parameter.names:
            self._parameter.names = _named_objs(self)
        objects = objects.items() if isinstance(objects, dict) else objects
        with self._trigger():
            for i, o in enumerate(objects):
                if not isinstance(o, collections.abc.Sequence):
                    raise TypeError(
                        f'cannot convert dictionary update sequence element #{i} to a sequence'
                    )
                o = tuple(o)
                n = len(o)
                if n != 2:
                    raise ValueError(
                        f'dictionary update sequence element #{i} has length {n}; 2 is required'
                    )
                k, v = o
                self.__setitem__(k, v, trigger=False)
            for k, v in items.items():
                self.__setitem__(k, v, trigger=False)

    def values(self):
        if self._parameter.names:
            return self._parameter.names.values()
        return _named_objs(self).values()


class __compute_selector_default:
    """
    Using a function instead of setting default to [] in _slot_defaults, as
    if it were modified in place later, which would happen with check_on_set set to False,
    then the object in _slot_defaults would itself be updated and the next Selector
    instance created wouldn't have [] as the default but a populated list.
    """

    def __call__(self, p):
        return []

    def __repr__(self):
        return repr(self.sig)

    @property
    def sig(self):
        return []

_compute_selector_default = __compute_selector_default()


class __compute_selector_checking_default:
    def __call__(self, p):
        return len(p.objects) != 0

    def __repr__(self):
        return repr(self.sig)

    @property
    def sig(self):
        return None

_compute_selector_checking_default = __compute_selector_checking_default()


class _SignatureSelector(Parameter):
    # Needs docstring; why is this a separate mixin?
    _slot_defaults = dict(
        SelectorBase._slot_defaults, _objects=_compute_selector_default,
        compute_default_fn=None, check_on_set=_compute_selector_checking_default,
        allow_None=None, instantiate=False, default=None,
    )

    @classmethod
    def _modified_slots_defaults(cls):
        defaults = super()._modified_slots_defaults()
        defaults['objects'] = defaults.pop('_objects')
        return defaults


class Selector(SelectorBase, _SignatureSelector):
    """
    Parameter whose value must be one object from a list of possible objects.

    By default, if no default is specified, picks the first object from
    the provided set of objects, as long as the objects are in an
    ordered data collection.

    ``check_on_set`` restricts the value to be among the current list of
    objects. By default, if objects are initially supplied,
    ``check_on_set`` is ``True``, whereas if no objects are initially
    supplied, ``check_on_set`` is ``False``. This can be overridden by
    explicitly specifying check_on_set initially.

    If ``check_on_set`` is ``True`` (either because objects are supplied
    initially, or because it is explicitly specified), the default
    (initial) value must be among the list of objects (unless the
    default value is ``None``).

    The list of objects can be supplied as a list (appropriate for
    selecting among a set of strings, or among a set of objects with a
    ``name`` parameter), or as a (preferably ordered) dictionary from
    names to objects.  If a dictionary is supplied, the objects
    will need to be hashable so that their names can be looked
    up from the object value.

    ``empty_default`` is an internal argument that does not have a slot.
    """

    __slots__ = ['_objects', 'compute_default_fn', 'check_on_set', 'names']

    @typing.overload
    def __init__(
        self,
        *, objects=[], default=None, instantiate=False, compute_default_fn=None,
        check_on_set=None, allow_None=None, empty_default=False,
        doc=None, label=None, precedence=None, constant=False, readonly=False,
        pickle_default_value=True, per_instance=True, allow_refs=False, nested_refs=False,
        default_factory=None, metadata=None,
    ):
        ...

    # Selector is usually used to allow selection from a list of
    # existing objects, therefore instantiate is False by default.
    def __init__(self, *, objects=Undefined, default=Undefined, instantiate=Undefined,
                 compute_default_fn=Undefined, check_on_set=Undefined,
                 allow_None=Undefined, empty_default=False, **params):

        if compute_default_fn is not Undefined:
            warnings.warn(
                'compute_default_fn has been deprecated and will be removed in a future version.',
                _ParamDeprecationWarning,
                stacklevel=_find_stack_level(),
            )

        autodefault = Undefined
        if objects is not Undefined and objects:
            if isinstance(objects, dict):
                autodefault = list(objects.values())[0]
            elif isinstance(objects, list):
                autodefault = objects[0]

        default = autodefault if (not empty_default and default is Undefined) else default

        self.objects = objects
        self.compute_default_fn = compute_default_fn
        self.check_on_set = check_on_set

        super().__init__(
            default=default, instantiate=instantiate, **params)
        # Required as Parameter sets allow_None=True if default is None
        if allow_None is Undefined:
            self.allow_None = self._slot_defaults['allow_None']
        else:
            self.allow_None = allow_None
        if self.default is not None:
            self._validate_value(self.default)
        self._update_state()

    def _update_state(self):
        if self.check_on_set is False and self.default is not None:
            self._ensure_value_is_in_objects(self.default)

    @property
    def objects(self):
        return ListProxy(self._objects, self)

    @objects.setter
    def objects(self, objects):
        if isinstance(objects, collections.abc.Mapping):
            self.names = objects
            self._objects = list(objects.values())
        else:
            self.names = {}
            self._objects = objects

    # Note that if the list of objects is changed, the current value for
    # this parameter in existing POs could be outside of the new range.

    def compute_default(self):
        """
        If this parameter's compute_default_fn is callable, call it
        and store the result in self.default.

        Also removes None from the list of objects (if the default is
        no longer None).

        .. deprecated:: 2.3.0
        """
        warnings.warn(
            'compute_default() has been deprecated and will be removed in a future version.',
            _ParamDeprecationWarning,
            stacklevel=_find_stack_level(),
        )

        if self.default is None and callable(self.compute_default_fn):
            self.default = self.compute_default_fn()
            self._ensure_value_is_in_objects(self.default)

    def _validate(self, val):
        if not self.check_on_set:
            self._ensure_value_is_in_objects(val)
            return

        self._validate_value(val)

    def _validate_value(self, val):
        if self.check_on_set and not (self.allow_None and val is None) and val not in self.objects:
            items = []
            limiter = ']'
            length = 0
            for item in self.objects:
                string = str(item)
                length += len(string)
                if length < 200:
                    items.append(string)
                else:
                    limiter = ', ...]'
                    break
            items = '[' + ', '.join(items) + limiter
            raise ValueError(
                f"{_validate_error_prefix(self)} does not accept {val!r}; "
                f"valid options include: {items!r}"
            )

    def _ensure_value_is_in_objects(self, val):
        """
        Make sure that the provided value is present on the objects list.
        Subclasses can override if they support multiple items on a list,
        to check each item instead.
        """
        if val not in self.objects:
            self._objects.append(val)

    def get_range(self):
        """
        Return the possible objects to which this parameter could be set.

        (Returns the dictionary {object.name: object}.)
        """
        return _named_objs(self._objects, self.names)


class ObjectSelector(Selector):
    """
    Deprecated. Same as Selector, but with a different constructor for
    historical reasons.
    """

    @typing.overload
    def __init__(
        self,
        default=None, *, objects=[], instantiate=False, compute_default_fn=None,
        check_on_set=None, allow_None=None, empty_default=False,
        doc=None, label=None, precedence=None, constant=False, readonly=False,
        pickle_default_value=True, per_instance=True, allow_refs=False, nested_refs=False,
        default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, objects=Undefined, **kwargs):
        super().__init__(objects=objects, default=default,
                         empty_default=True, **kwargs)


class FileSelector(Selector):
    """Given a path glob, allows one file to be selected from those matching."""

    __slots__ = ['path']

    _slot_defaults = dict(
        Selector._slot_defaults, path="",
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, path="", objects=[], instantiate=False, compute_default_fn=None,
        check_on_set=None, allow_None=None, empty_default=False,
        doc=None, label=None, precedence=None, constant=False, readonly=False,
        pickle_default_value=True, per_instance=True, allow_refs=False, nested_refs=False,
        default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, path=Undefined, **kwargs):
        self.default = default
        self.path = path
        self.update(path=path)
        if default is not Undefined:
            self.default = default
        super().__init__(default=self.default, objects=self._objects, **kwargs)

    def _on_set(self, attribute, old, new):
        super()._on_set(attribute, new, old)
        if attribute == 'path':
            self.update(path=new)

    def update(self, path=Undefined):
        if path is Undefined:
            path = self.path
        if path == "":
            self.objects = []
        else:
            # Convert using os.fspath and pathlib.Path to handle ensure
            # the path separators are consistent (on Windows in particular)
            pathpattern = os.fspath(pathlib.Path(path))
            self.objects = sorted(glob.glob(pathpattern))
        if self.default in self.objects:
            return
        self.default = self.objects[0] if self.objects else None

    def get_range(self):
        return _abbreviate_paths(self.path,super().get_range())


class ListSelector(Selector):
    """
    Variant of :class:`Selector` where the value can be multiple objects from
    a list of possible objects.
    """

    @typing.overload
    def __init__(
        self,
        default=None, *, objects=[], instantiate=False, compute_default_fn=None,
        check_on_set=None, allow_None=None, empty_default=False,
        doc=None, label=None, precedence=None, constant=False, readonly=False,
        pickle_default_value=True, per_instance=True, allow_refs=False, nested_refs=False,
        default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, objects=Undefined, **kwargs):
        super().__init__(
            objects=objects, default=default, empty_default=True, **kwargs)

    def compute_default(self):
        warnings.warn(
            'compute_default() has been deprecated and will be removed in a future version.',
            _ParamDeprecationWarning,
            stacklevel=_find_stack_level(),
        )
        if self.default is None and callable(self.compute_default_fn):
            self.default = self.compute_default_fn()
            for o in self.default:
                if o not in self.objects:
                    self.objects.append(o)

    def _validate(self, val):
        if (val is None and self.allow_None):
            return
        self._validate_type(val)

        if self.check_on_set:
            self._validate_value(val)
        else:
            for v in val:
                self._ensure_value_is_in_objects(v)

    def _validate_type(self, val):
        if not isinstance(val, list):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes list types, "
                f"not {val!r}."
            )

    def _validate_value(self, val):
        self._validate_type(val)
        if val is not None:
            for o in val:
                super()._validate_value(o)

    def _update_state(self):
        if self.check_on_set is False and self.default is not None:
            for o in self.default:
                self._ensure_value_is_in_objects(o)


class MultiFileSelector(ListSelector):
    """Given a path glob, allows multiple files to be selected from the list of matches."""

    __slots__ = ['path']

    _slot_defaults = dict(
        Selector._slot_defaults, path="",
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, path="", objects=[], compute_default_fn=None,
        check_on_set=None, allow_None=None, empty_default=False,
        doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True,
        per_instance=True, allow_refs=False, nested_refs=False,
        default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, path=Undefined, **kwargs):
        self.default = default
        self.path = path
        self.update(path=path)
        super().__init__(default=default, objects=self._objects, **kwargs)

    def _on_set(self, attribute, old, new):
        super()._on_set(attribute, new, old)
        if attribute == 'path':
            self.update(path=new)

    def update(self, path=Undefined):
        if path is Undefined:
            path = self.path
        self.objects = sorted(glob.glob(path))
        if self.default and all([o in self.objects for o in self.default]):
            return
        elif not self.default:
            return
        self.default = self.objects

    def get_range(self):
        return _abbreviate_paths(self.path,super().get_range())


class ClassSelector(SelectorBase):
    """
    Parameter allowing selection of either a subclass or an instance of a class
    or tuple of classes.

    By default, requires an instance, but if ``is_instance=False``, accepts a
    class instead. Both class and instance values respect the ``instantiate``
    slot, though it matters only for ``is_instance=True``.
    """

    __slots__ = ['class_', 'is_instance']

    _slot_defaults = dict(SelectorBase._slot_defaults, instantiate=True, is_instance=True)

    @typing.overload
    def __init__(
        self,
        *, class_, default=None, instantiate=True, is_instance=True,
        allow_None=False, doc=None, label=None, precedence=None,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, *, class_, default=Undefined, instantiate=Undefined, is_instance=Undefined, **params):
        self.class_ = class_
        self.is_instance = is_instance
        super().__init__(default=default,instantiate=instantiate,**params)
        self._validate(self.default)

    def _validate(self, val):
        super()._validate(val)
        self._validate_class_(val, self.class_, self.is_instance)

    def _validate_class_(self, val, class_, is_instance):
        if (val is None and self.allow_None):
            return
        if (is_instance and isinstance(val, class_)) or (not is_instance and issubclass(val, class_)):
            return

        if isinstance(class_, Iterable):
            class_name = ('({})'.format(', '.join(cl.__name__ for cl in class_)))
        else:
            class_name = class_.__name__

        raise ValueError(
            f"{_validate_error_prefix(self)} value must be "
            f"{'an instance' if is_instance else 'a subclass'} of {class_name}, not {val!r}."
        )

    def get_range(self):
        """
        Return the possible types for this parameter's value.

        (I.e. return ``{name: <class>}`` for all classes that are
        :func:`param.parameterized.descendents` of ``self.class_``.)

        Only classes from modules that have been imported are added
        (see :func:`param.parameterized.descendents`).
        """
        classes = self.class_ if isinstance(self.class_, tuple) else (self.class_,)
        all_classes = {}
        for cls in classes:
            desc = _descendents(cls, concrete=True)
            # This will clobber separate classes with identical names.
            # Known historical issue, see https://github.com/holoviz/param/pull/1035
            all_classes.update({c.__name__: c for c in desc})
        d = OrderedDict((name, class_) for name,class_ in all_classes.items())
        if self.allow_None:
            d['None'] = None
        return d


class Dict(ClassSelector):
    """Parameter whose value is a dictionary."""

    @typing.overload
    def __init__(
        self,
        default=None, *, is_instance=True,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=True,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        super().__init__(default=default, class_=dict, **params)


class Array(ClassSelector):
    """Parameter whose value is a numpy array."""

    @typing.overload
    def __init__(
        self,
        default=None, *, is_instance=True,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=True,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, **params):
        from numpy import ndarray
        super().__init__(default=default, class_=ndarray, **params)

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        return value.tolist()

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        import numpy
        if isinstance(value, str):
            return _deserialize_from_path(
                {'.npy': numpy.load, '.txt': lambda x: numpy.loadtxt(str(x))},
                value, 'Array'
            )
        else:
            return numpy.asarray(value)


class DataFrame(ClassSelector):
    """
    Parameter whose value is a pandas ``DataFrame``.

    The structure of the DataFrame can be constrained by the rows and
    columns arguments:

    ``rows``: If specified, may be a number or an integer bounds tuple to
    constrain the allowable number of rows.

    ``columns``: If specified, may be a number, an integer bounds tuple, a
    list or a set. If the argument is numeric, constrains the number of
    columns using the same semantics as used for rows. If either a list
    or set of strings, the column names will be validated. If a set is
    used, the supplied DataFrame must contain the specified columns and
    if a list is given, the supplied DataFrame must contain exactly the
    same columns and in the same order and no other columns.
    """

    __slots__ = ['rows', 'columns', 'ordered']

    _slot_defaults = dict(
        ClassSelector._slot_defaults, rows=None, columns=None, ordered=None
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, rows=None, columns=None, ordered=None, is_instance=True,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=True,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, rows=Undefined, columns=Undefined, ordered=Undefined, **params):
        from pandas import DataFrame as pdDFrame
        self.rows = rows
        self.columns = columns
        self.ordered = ordered
        super().__init__(default=default, class_=pdDFrame, **params)
        self._validate(self.default)

    def _length_bounds_check(self, bounds, length, name):
        message = f'{name} length {length} does not match declared bounds of {bounds}'
        if not isinstance(bounds, tuple):
            if (bounds != length):
                raise ValueError(f"{_validate_error_prefix(self)}: {message}")
            else:
                return
        (lower, upper) = bounds
        failure = ((lower is not None and (length < lower))
                   or (upper is not None and length > upper))
        if failure:
            raise ValueError(f"{_validate_error_prefix(self)}: {message}")

    def _validate(self, val):
        super()._validate(val)

        if isinstance(self.columns, set) and self.ordered is True:
            raise ValueError(
                f'{_validate_error_prefix(self)}: columns cannot be ordered '
                f'when specified as a set'
            )

        if self.allow_None and val is None:
            return

        if self.columns is None:
            pass
        elif (isinstance(self.columns, tuple) and len(self.columns)==2
              and all(isinstance(v, (type(None), numbers.Number)) for v in self.columns)): # Numeric bounds tuple
            self._length_bounds_check(self.columns, len(val.columns), 'columns')
        elif isinstance(self.columns, (list, set)):
            self.ordered = isinstance(self.columns, list) if self.ordered is None else self.ordered
            difference = set(self.columns) - {str(el) for el in val.columns}
            if difference:
                raise ValueError(
                    f"{_validate_error_prefix(self)}: provided columns "
                    f"{list(val.columns)} does not contain required "
                    f"columns {sorted(self.columns)}"
                )
        else:
            self._length_bounds_check(self.columns, len(val.columns), 'column')

        if self.ordered:
            if list(val.columns) != list(self.columns):
                raise ValueError(
                    f"{_validate_error_prefix(self)}: provided columns "
                    f"{list(val.columns)} must exactly match {self.columns}"
                )
        if self.rows is not None:
            self._length_bounds_check(self.rows, len(val), 'row')

    @classmethod
    def serialize(cls, value):
        if value is None:
            return None
        return value.to_dict('records')

    @classmethod
    def deserialize(cls, value):
        if value == 'null' or value is None:
            return None
        import pandas
        if isinstance(value, str):
            return _deserialize_from_path(
                {
                    '.csv': pandas.read_csv,
                    '.dta': pandas.read_stata,
                    '.feather': pandas.read_feather,
                    '.h5': pandas.read_hdf,
                    '.hdf5': pandas.read_hdf,
                    '.json': pandas.read_json,
                    '.ods': pandas.read_excel,
                    '.parquet': pandas.read_parquet,
                    '.pkl': pandas.read_pickle,
                    '.tsv': lambda x: pandas.read_csv(x, sep='\t'),
                    '.xlsm': pandas.read_excel,
                    '.xlsx': pandas.read_excel,
                }, value, 'DataFrame')
        else:
            return pandas.DataFrame(value)


class Series(ClassSelector):
    """
    Parameter whose value is a pandas ``Series``.

    The structure of the Series can be constrained by the rows argument
    which may be a number or an integer bounds tuple to constrain the
    allowable number of rows.
    """

    __slots__ = ['rows']

    _slot_defaults = dict(
        ClassSelector._slot_defaults, rows=None, allow_None=False
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, rows=None, allow_None=False, is_instance=True,
        doc=None, label=None, precedence=None, instantiate=True,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, rows=Undefined, allow_None=Undefined, **params):
        from pandas import Series as pdSeries
        self.rows = rows
        super().__init__(default=default, class_=pdSeries, allow_None=allow_None,
                         **params)
        self._validate(self.default)

    def _length_bounds_check(self, bounds, length, name):
        message = f'{name} length {length} does not match declared bounds of {bounds}'
        if not isinstance(bounds, tuple):
            if (bounds != length):
                raise ValueError(f"{_validate_error_prefix(self)}: {message}")
            else:
                return
        (lower, upper) = bounds
        failure = ((lower is not None and (length < lower))
                   or (upper is not None and length > upper))
        if failure:
            raise ValueError(f"{_validate_error_prefix(self)}: {message}")

    def _validate(self, val):
        super()._validate(val)

        if self.allow_None and val is None:
            return

        if self.rows is not None:
            self._length_bounds_check(self.rows, len(val), 'row')

#-----------------------------------------------------------------------------
# List
#-----------------------------------------------------------------------------

class List(Parameter):
    """
    Parameter whose value is a list of objects, usually of a specified type.

    The bounds allow a minimum and/or maximum length of
    list to be enforced.  If the ``item_type`` is non-None, all
    items in the list are checked to be instances of that type if
    ``is_instance`` is ``True`` (default) or subclasses of that type when False.
    """

    __slots__ = ['bounds', 'item_type', 'is_instance']

    _slot_defaults = dict(
        Parameter._slot_defaults, item_type=None, bounds=(0, None),
        instantiate=True, default=[], is_instance=True,
    )

    @typing.overload
    def __init__(
        self,
        default=[], *, item_type=None, instantiate=True, bounds=(0, None),
        is_instance=True, allow_None=False, doc=None, label=None, precedence=None,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, item_type=Undefined,
                 instantiate=Undefined, bounds=Undefined, is_instance=Undefined, **params):
        if item_type is not Undefined:
            self.item_type = item_type
        else:
            self.item_type = item_type
        self.is_instance = is_instance
        self.bounds = bounds
        Parameter.__init__(self, default=default, instantiate=instantiate,
                           **params)
        self._validate(self.default)

    def _validate(self, val):
        """
        Check that the value is numeric and that it is within the hard
        bounds; if not, an exception is raised.
        """
        self._validate_value(val, self.allow_None)
        self._validate_bounds(val, self.bounds)
        self._validate_item_type(val, self.item_type, self.is_instance)

    def _validate_bounds(self, val, bounds):
        """Check that the list is of the right length and has the right contents."""
        if bounds is None or (val is None and self.allow_None):
            return
        min_length, max_length = bounds
        l = len(val)
        if min_length is not None and max_length is not None:
            if not (min_length <= l <= max_length):
                raise ValueError(
                    f"{_validate_error_prefix(self)} length must be between "
                    f"{min_length} and {max_length} (inclusive), not {l}."
                )
        elif min_length is not None:
            if not min_length <= l:
                raise ValueError(
                    f"{_validate_error_prefix(self)} length must be at "
                    f"least {min_length}, not {l}."
                )
        elif max_length is not None:
            if not l <= max_length:
                raise ValueError(
                    f"{_validate_error_prefix(self)} length must be at "
                    f"most {max_length}, not {l}."
                )

    def _validate_value(self, val, allow_None):
        if allow_None and val is None:
            return
        if not isinstance(val, list):
            raise ValueError(
                f"{_validate_error_prefix(self)} must be a list, not an "
                f"object of {type(val)}."
            )

    def _validate_item_type(self, val, item_type, is_instance):
        if item_type is None or (self.allow_None and val is None):
            return
        err_kind = None
        for v in val:
            if is_instance and not isinstance(v, item_type):
                err_kind = "instances"
                obj_display = lambda v: type(v)
            elif not is_instance and (type(v) is not type or not issubclass(v, item_type)):
                err_kind = "subclasses"
                obj_display = lambda v: v
            if err_kind:
                raise TypeError(
                    f"{_validate_error_prefix(self)} items must be {err_kind} "
                    f"of {item_type!r}, not {obj_display(v)}."
                )


class HookList(List):
    """
    Parameter whose value is a list of callable objects.

    This type of :class:`List` Parameter is typically used to provide a place
    for users to register a set of commands to be called at a
    specified place in some sequence of processing steps.
    """

    __slots__ = ['bounds']

    def _validate_value(self, val, allow_None):
        super()._validate_value(val, allow_None)
        if allow_None and val is None:
            return
        for v in val:
            if callable(v):
                continue
            raise ValueError(
                f"{_validate_error_prefix(self)} items must be callable, "
                f"not {v!r}."
            )

#-----------------------------------------------------------------------------
# Path
#-----------------------------------------------------------------------------

# For portable code:
#   - specify paths in unix (rather than Windows) style;
#   - use resolve_path(path_to_file=True) for paths to existing files to be read,
#   - use resolve_path(path_to_file=False) for paths to existing folders to be read.

class resolve_path(ParameterizedFunction):
    """
    Find the path to an existing file, searching the paths specified
    in the search_paths parameter if the filename is not absolute, and
    converting a UNIX-style path to the current OS's format if
    necessary.

    To turn a supplied relative path into an absolute one, the path is
    appended to paths in the search_paths parameter, in order, until
    the file is found.

    An IOError is raised if the file is not found.

    Similar to Python's os.path.abspath(), except more search paths
    than just os.getcwd() can be used, and the file must exist.
    """

    search_paths = List(default=[os.getcwd()], pickle_default_value=None, doc="""
        Prepended to a non-relative path, in order, until a file is
        found.""")

    path_to_file = Boolean(default=True, pickle_default_value=None,
                           allow_None=True, doc="""
        String specifying whether the path refers to a 'File' or a
        'Folder'. If None, the path may point to *either* a 'File' *or*
        a 'Folder'.""")

    def __call__(self, path, **params):
        p = ParamOverrides(self, params)
        path = os.path.normpath(path)
        ftype = "File" if p.path_to_file is True \
            else "Folder" if p.path_to_file is False else "Path"

        if not p.search_paths:
            p.search_paths = [os.getcwd()]

        if os.path.isabs(path):
            if ((p.path_to_file is None  and os.path.exists(path)) or
                (p.path_to_file is True  and os.path.isfile(path)) or
                (p.path_to_file is False and os.path.isdir( path))):
                return path
            raise OSError(f"{ftype} '{path}' not found.")

        else:
            paths_tried = []
            for prefix in p.search_paths:
                try_path = os.path.join(os.path.normpath(prefix), path)

                if ((p.path_to_file is None  and os.path.exists(try_path)) or
                    (p.path_to_file is True  and os.path.isfile(try_path)) or
                    (p.path_to_file is False and os.path.isdir( try_path))):
                    return try_path

                paths_tried.append(try_path)

            raise OSError(ftype + " " + os.path.split(path)[1] + " was not found in the following place(s): " + str(paths_tried) + ".")


class Path(Parameter):
    """
    Parameter that can be set to a string specifying the path of a file or folder.

    The string should be specified in UNIX style, but it will be
    returned in the format of the user's operating system. Please use
    the :class:`Filename` or :class:`Foldername` Parameters if you require discrimination
    between the two possibilities.

    The specified path can be absolute, or relative to either:

    * any of the paths specified in the ``search_paths`` attribute (if
      ``search_paths`` is not ``None``);
    * any of the paths searched by :func:`resolve_path` (if ``search_paths``
      is ``None``).

    Parameters
    ----------
    search_paths : list, default=[os.getcwd()]
        List of paths to search the path from
    check_exists: boolean, default=True
        If True (default) the path must exist on instantiation and set,
        otherwise the path can optionally exist.

    """

    __slots__ = ['search_paths', 'check_exists']

    _slot_defaults = dict(
        Parameter._slot_defaults, check_exists=True,
    )

    @typing.overload
    def __init__(
        self,
        default=None, *, search_paths=None, check_exists=True,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, search_paths=Undefined, check_exists=Undefined, **params):
        if search_paths is Undefined:
            search_paths = []

        self.search_paths = search_paths
        if check_exists is not Undefined and not isinstance(check_exists, bool):
            raise ValueError("'check_exists' attribute value must be a boolean")
        self.check_exists = check_exists
        super().__init__(default,**params)
        self._validate(self.default)

    def _resolve(self, path):
        return resolve_path(path, path_to_file=None, search_paths=self.search_paths)

    def _validate(self, val):
        if val is None:
            if not self.allow_None:
                raise ValueError(f'{_validate_error_prefix(self)} does not accept None')
        else:
            if not isinstance(val, (str, pathlib.Path)):
                raise ValueError(f'{_validate_error_prefix(self)} only take str or pathlib.Path types')
            try:
                self._resolve(val)
            except OSError as e:
                if self.check_exists:
                    raise OSError(e.args[0]) from None

    def __get__(self, obj, objtype):
        """Return an absolute, normalized path (see resolve_path)."""
        raw_path = super().__get__(obj,objtype)
        if raw_path is None:
            path = None
        else:
            try:
                path = self._resolve(raw_path)
            except OSError:
                if self.check_exists:
                    raise
                else:
                    path = raw_path
        return path

    def __getstate__(self):
        # don't want to pickle the search_paths
        state = super().__getstate__()

        if 'search_paths' in state:
            state['search_paths'] = []

        return state



class Filename(Path):
    """
    Parameter that can be set to a string specifying the path of a file.

    The string should be specified in UNIX style, but it will be
    returned in the format of the user's operating system.

    The specified path can be absolute, or relative to either:

    * any of the paths specified in the ``search_paths`` attribute (if
      ``search_paths`` is not ``None``);
    * any of the paths searched by :func:`resolve_path` (if ``search_paths``
      is ``None``).
    """

    def _resolve(self, path):
        return resolve_path(path, path_to_file=True, search_paths=self.search_paths)


class Foldername(Path):
    """
    Parameter that can be set to a string specifying the path of a folder.

    The string should be specified in UNIX style, but it will be
    returned in the format of the user's operating system.

    The specified path can be absolute, or relative to either:

    * any of the paths specified in the ``search_paths`` attribute (if
      ``search_paths`` is not ``None``);
    * any of the paths searched by resolve_dir_path() (if ``search_paths``
      is ``None``).
    """

    def _resolve(self, path):
        return resolve_path(path, path_to_file=False, search_paths=self.search_paths)

#-----------------------------------------------------------------------------
# Color
#-----------------------------------------------------------------------------

class Color(Parameter):
    """
    Color parameter defined as a hex RGB string with an optional ``#``
    prefix or (optionally) as a CSS3 color name.
    """

    # CSS3 color specification https://www.w3.org/TR/css-color-3/#svg-color
    _named_colors = [ 'aliceblue', 'antiquewhite', 'aqua',
        'aquamarine', 'azure', 'beige', 'bisque', 'black',
        'blanchedalmond', 'blue', 'blueviolet', 'brown', 'burlywood',
        'cadetblue', 'chartreuse', 'chocolate', 'coral',
        'cornflowerblue', 'cornsilk', 'crimson', 'cyan', 'darkblue',
        'darkcyan', 'darkgoldenrod', 'darkgray', 'darkgrey',
        'darkgreen', 'darkkhaki', 'darkmagenta', 'darkolivegreen',
        'darkorange', 'darkorchid', 'darkred', 'darksalmon',
        'darkseagreen', 'darkslateblue', 'darkslategray',
        'darkslategrey', 'darkturquoise', 'darkviolet', 'deeppink',
        'deepskyblue', 'dimgray', 'dimgrey', 'dodgerblue',
        'firebrick', 'floralwhite', 'forestgreen', 'fuchsia',
        'gainsboro', 'ghostwhite', 'gold', 'goldenrod', 'gray',
        'grey', 'green', 'greenyellow', 'honeydew', 'hotpink',
        'indianred', 'indigo', 'ivory', 'khaki', 'lavender',
        'lavenderblush', 'lawngreen', 'lemonchiffon', 'lightblue',
        'lightcoral', 'lightcyan', 'lightgoldenrodyellow',
        'lightgray', 'lightgrey', 'lightgreen', 'lightpink',
        'lightsalmon', 'lightseagreen', 'lightskyblue',
        'lightslategray', 'lightslategrey', 'lightsteelblue',
        'lightyellow', 'lime', 'limegreen', 'linen', 'magenta',
        'maroon', 'mediumaquamarine', 'mediumblue', 'mediumorchid',
        'mediumpurple', 'mediumseagreen', 'mediumslateblue',
        'mediumspringgreen', 'mediumturquoise', 'mediumvioletred',
        'midnightblue', 'mintcream', 'mistyrose', 'moccasin',
        'navajowhite', 'navy', 'oldlace', 'olive', 'olivedrab',
        'orange', 'orangered', 'orchid', 'palegoldenrod', 'palegreen',
        'paleturquoise', 'palevioletred', 'papayawhip', 'peachpuff',
        'peru', 'pink', 'plum', 'powderblue', 'purple', 'red',
        'rosybrown', 'royalblue', 'saddlebrown', 'salmon',
        'sandybrown', 'seagreen', 'seashell', 'sienna', 'silver',
        'skyblue', 'slateblue', 'slategray', 'slategrey', 'snow',
        'springgreen', 'steelblue', 'tan', 'teal', 'thistle',
        'tomato', 'turquoise', 'violet', 'wheat', 'white',
        'whitesmoke', 'yellow', 'yellowgreen']

    __slots__ = ['allow_named']

    _slot_defaults = dict(Parameter._slot_defaults, allow_named=True)

    @typing.overload
    def __init__(
        self,
        default=None, *, allow_named=True,
        allow_None=False, doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, allow_named=Undefined, **kwargs):
        super().__init__(default=default, **kwargs)
        self.allow_named = allow_named
        self._validate(self.default)

    def _validate(self, val):
        self._validate_value(val, self.allow_None)
        self._validate_allow_named(val, self.allow_named)

    def _validate_value(self, val, allow_None):
        if (allow_None and val is None):
            return
        if not isinstance(val, str):
            raise ValueError(
                f"{_validate_error_prefix(self)} expects a string value, "
                f"not an object of {type(val)}."
            )

    def _validate_allow_named(self, val, allow_named):
        if (val is None and self.allow_None):
            return
        is_hex = re.match('^#?(([0-9a-fA-F]{2}){3}|([0-9a-fA-F]){3})$', val)
        if self.allow_named:
            if not is_hex and val.lower() not in self._named_colors:
                raise ValueError(
                    f"{_validate_error_prefix(self)} only takes RGB hex codes "
                    f"or named colors, received '{val}'."
                )
        elif not is_hex:
            raise ValueError(
                f"{_validate_error_prefix(self)} only accepts valid RGB hex "
                f"codes, received {val!r}."
            )

#-----------------------------------------------------------------------------
# Bytes
#-----------------------------------------------------------------------------

class Bytes(Parameter):
    """
    A Bytes Parameter, with a default value and optional regular
    expression (regex) matching.

    Similar to the :class:`String` parameter, but instead of type string
    this parameter only allows objects of type bytes (e.g. ``b'bytes'``).
    """

    __slots__ = ['regex']

    _slot_defaults = dict(
        Parameter._slot_defaults, default=b"", regex=None, allow_None=False,
    )


    @typing.overload
    def __init__(
        self,
        default=b"", *, regex=None, allow_None=False,
        doc=None, label=None, precedence=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, regex=Undefined, allow_None=Undefined, **kwargs):
        super().__init__(default=default, **kwargs)
        self.regex = regex
        self._validate(self.default)

    def _validate_regex(self, val, regex):
        if (val is None and self.allow_None):
            return
        if regex is not None and re.match(regex, val) is None:
            raise ValueError(
                f"{_validate_error_prefix(self)} value {val!r} "
                f"does not match regex {regex!r}."
            )

    def _validate_value(self, val, allow_None):
        if allow_None and val is None:
            return
        if not isinstance(val, bytes):
            raise ValueError(
                f"{_validate_error_prefix(self)} only takes a byte string value, "
                f"not value of {type(val)}."
            )

    def _validate(self, val):
        self._validate_value(val, self.allow_None)
        self._validate_regex(val, self.regex)
