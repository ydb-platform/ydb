"""
Generic support for objects with full-featured Parameters and
messaging.

This file comes from the Param library (https://github.com/holoviz/param)
but can be taken out of the param module and used on its own if desired,
either alone (providing basic Parameter support) or with param's
__init__.py (providing specialized Parameter types).
"""

import abc
import copy
import datetime as dt
import inspect
import numbers
import operator
import os
import re
import sys
import types
import typing
import warnings
from inspect import getfullargspec

from collections import defaultdict, namedtuple, OrderedDict
from collections.abc import Callable, Generator, Iterable
from functools import partial, wraps, reduce
from itertools import chain
from operator import itemgetter, attrgetter
from types import FunctionType, MethodType
# When python 3.9 support is dropped replace Union with |
from typing import Any, Union, Literal, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import logging

from contextlib import contextmanager
CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
DEBUG = 10
VERBOSE = INFO - 1

from . import serializer
from ._utils import (
    DEFAULT_SIGNATURE,
    ParamDeprecationWarning as _ParamDeprecationWarning,
    ParamFutureWarning as _ParamFutureWarning,
    ParamPendingDeprecationWarning as _ParamPendingDeprecationWarning,
    Skip,
    _find_stack_level,
    _in_ipython,
    _is_auto_name,
    _is_mutable_container,
    _recursive_repr,
    _to_async_gen,
    _validate_error_prefix,
    accept_arguments,
    iscoroutinefunction,
    descendents,  # noqa: F401
    gen_types,
)

# Ideally setting param_pager would be in __init__.py but param_pager is
# needed on import to create the Parameterized class, so it'd need to precede
# importing parameterized.py in __init__.py which would be a little weird.
if _in_ipython():
    # In case the optional ipython module is unavailable
    try:
        from .ipython import ParamPager, ipython_async_executor as async_executor
        param_pager = ParamPager(metaclass=True)  # Generates param description
    except ModuleNotFoundError:
        from ._utils import async_executor
else:
    from ._utils import async_executor
    param_pager = None


@gen_types
def _dt_types():
    yield dt.datetime
    yield dt.date
    if np := sys.modules.get("numpy"):
        yield np.datetime64

@gen_types
def _int_types():
    yield int
    if np := sys.modules.get("numpy"):
        yield np.integer


logger = None

def get_logger(name: Optional[str] = None)->"logging.Logger":
    """
    Retrieve a logger instance for use with the ``param`` library.

    This function returns a logger configured for the ``param`` library. If no
    logger has been explicitly set, it initializes a default root logger for
    ``param`` with an ``INFO`` log level and a basic console handler. Additional
    loggers can be retrieved by specifying a `name`, which appends to the
    root logger's name.

    Parameters
    ----------
    name : str | None, optional
        The name of the logger to retrieve. If ``None`` (default), the root logger
        for ``param`` is returned. If specified, a child logger with the name
        ``param.<name>`` is returned.

    Returns
    -------
    logging.Logger
        A logger instance configured for the ``param`` library.

    Examples
    --------
    Retrieve the root logger for ``param``:

    >>> import param
    >>> logger = param.get_logger()
    >>> logger.info("This is an info message.")
    INFO:param: This is an info message.

    Retrieve a named child logger:

    >>> logger = param.parameterized.get_logger("custom_logger")
    >>> logger.warning("This is a warning from custom_logger.")
    WARNING:param.custom_logger: This is a warning from custom_logger.
    """
    import logging
    if logger is None:
        logging.addLevelName(VERBOSE, "VERBOSE")
        root_logger = logging.getLogger('param')
        if not root_logger.handlers:
            root_logger.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt='%(levelname)s:%(name)s: %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
    else:
        root_logger = logger
    if name is None:
        return root_logger
    else:
        return logging.getLogger(root_logger.name + '.' + name)


# Indicates whether warnings should be raised as errors, stopping
# processing.
warnings_as_exceptions = False

docstring_signature = True        # Add signature to class docstrings
docstring_describe_params = True  # Add parameter description to class
                                  # docstrings (requires ipython module)
object_count = 0
warning_count = 0

# Hook to apply to depends and bind arguments to turn them into valid parameters
_reference_transforms = []

def register_reference_transform(transform):
    """
    Append a transform to extract potential parameter dependencies
    from an object.

    Parameters
    ----------
    transform: Callable[Any, Any]

    """
    return _reference_transforms.append(transform)

def transform_reference(arg):
    """
    Apply transforms to turn objects which should be treated like
    a parameter reference into a valid reference that can be resolved
    by Param. This is useful for adding handling for depending on objects
    that are not simple Parameters or functions with dependency
    definitions.
    """
    for transform in _reference_transforms:
        if isinstance(arg, Parameter) or hasattr(arg, '_dinfo'):
            break
        arg = transform(arg)
    return arg

def eval_function_with_deps(function):
    """
    Evaluate a function after resolving its dependencies.

    Calls and returns a function after resolving any dependencies
    stored on the _dinfo attribute and passing the resolved values
    as arguments.
    """
    args, kwargs = (), {}
    if hasattr(function, '_dinfo'):
        arg_deps = function._dinfo['dependencies']
        kw_deps = function._dinfo.get('kw', {})
        if kw_deps or any(isinstance(d, Parameter) for d in arg_deps):
            args = (getattr(dep.owner, dep.name) for dep in arg_deps)
            kwargs = {n: getattr(dep.owner, dep.name) for n, dep in kw_deps.items()}
    return function(*args, **kwargs)

def resolve_value(value, recursive=True):
    """Resolve the current value of a dynamic reference."""
    if not recursive:
        pass
    elif isinstance(value, (list, tuple)):
        return type(value)(resolve_value(v) for v in value)
    elif isinstance(value, dict):
        return type(value)((resolve_value(k), resolve_value(v)) for k, v in value.items())
    elif isinstance(value, slice):
        return slice(
            resolve_value(value.start),
            resolve_value(value.stop),
            resolve_value(value.step)
        )
    value = transform_reference(value)
    is_gen = inspect.isgeneratorfunction(value)
    if hasattr(value, '_dinfo') or iscoroutinefunction(value) or is_gen:
        value = eval_function_with_deps(value)
        if is_gen:
            value = _to_async_gen(value)
    elif isinstance(value, Parameter):
        value = getattr(value.owner, value.name)
    return value

def resolve_ref(reference, recursive=False):
    """Resolve all parameters a dynamic reference depends on."""
    if recursive:
        if isinstance(reference, (list, tuple, set)):
            return [r for v in reference for r in resolve_ref(v, recursive)]
        elif isinstance(reference, dict):
            return [r for kv in reference.items() for o in kv for r in resolve_ref(o, recursive)]
        elif isinstance(reference, slice):
            return [r for v in (reference.start, reference.stop, reference.step) for r in resolve_ref(v, recursive)]
    reference = transform_reference(reference)
    if hasattr(reference, '_dinfo'):
        dinfo = getattr(reference, '_dinfo', {})
        args = list(dinfo.get('dependencies', []))
        kwargs = list(dinfo.get('kw', {}).values())
        refs = []
        for arg in (args + kwargs):
            if isinstance(arg, str):
                owner = get_method_owner(reference)
                if arg in owner.param:
                    arg = owner.param[arg]
                elif '.' in arg:
                    path = arg.split('.')
                    arg = owner
                    for attr in path[:-1]:
                        arg = getattr(arg, attr)
                    arg = arg.param[path[-1]]
                else:
                    arg = getattr(owner, arg)
            refs.extend(resolve_ref(arg))
        return refs
    elif isinstance(reference, Parameter):
        return [reference]
    return []


class _Undefined:
    """
    Dummy value to signal completely undefined values rather than
    simple None values.
    """

    def __bool__(self):
        # Haven't defined whether Undefined is falsy or truthy,
        # so to avoid subtle bugs raise an error when it
        # is used in a comparison without `is`.
        raise RuntimeError('Use `is` to compare Undefined')

    def __repr__(self):
        return '<Undefined>'


Undefined = _Undefined()


@contextmanager
def logging_level(level: str) -> Generator[None, None, None]:
    """
    Context manager to temporarily modify Param's logging level.

    This function allows you to temporarily change the logging level of the
    Param library. Once the context exits, the original logging level is restored.

    Parameters
    ----------
    level : str
        The desired logging level as a string. Must be one of:
        ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``, or ``VERBOSE``.

    Yields
    ------
    None
        A context where the logging level is temporarily modified.

    Examples
    --------
    Temporarily set the logging level to ``DEBUG``:

    >>> import param
    >>> with param.logging_level('DEBUG'):
    ...     param.get_logger().debug("This is a debug message.")
    DEBUG:param: This is a debug message.

    After the context exits, the logging level is restored.
    """
    level = level.upper()
    levels = [DEBUG, INFO, WARNING, ERROR, CRITICAL, VERBOSE]
    level_names = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'VERBOSE']

    if level not in level_names:
        raise Exception(f"Level {level!r} not in {levels!r}")

    param_logger = get_logger()
    logging_level = param_logger.getEffectiveLevel()
    param_logger.setLevel(levels[level_names.index(level)])
    try:
        yield None
    finally:
        param_logger.setLevel(logging_level)


@contextmanager
def _batch_call_watchers(parameterized, enable=True, run=True):
    """Manage batching of watcher calls with optional queueing and execution.

    This internal version of `batch_call_watchers` allows control over whether
    events are queued and whether accumulated watchers are executed upon exit.

    Parameters
    ----------
    parameterized : object
        The object whose watchers are being managed.
    enable : bool, optional
        If `True`, enable batching of events. Defaults to `True`.
    run : bool, optional
        If `True`, execute accumulated watchers on exit.
        If `False`, they remain queued. Defaults to `True`.

    Yields
    ------
    None:
        This is a context manager that temporarily modifies watcher behavior.
    """
    BATCH_WATCH = parameterized.param._BATCH_WATCH
    parameterized.param._BATCH_WATCH = enable or parameterized.param._BATCH_WATCH
    try:
        yield
    finally:
        parameterized.param._BATCH_WATCH = BATCH_WATCH
        if run and not BATCH_WATCH:
            parameterized.param._batch_call_watchers()


@contextmanager
def batch_call_watchers(parameterized):
    """
    Context manager to batch events to provide to :class:`Watchers` on a
    parameterized object.

    This context manager queues any events triggered by setting a parameter value
    on the supplied :class:`Parameterized` object, saving them up to dispatch
    them all at once when the context manager exits.
    """
    BATCH_WATCH = parameterized.param._BATCH_WATCH
    parameterized.param._BATCH_WATCH = True
    try:
        yield
    finally:
        parameterized.param._BATCH_WATCH = BATCH_WATCH
        if not BATCH_WATCH:
            parameterized.param._batch_call_watchers()


@contextmanager
def _syncing(parameterized, parameters):
    old = parameterized._param__private.syncing
    parameterized._param__private.syncing = set(old) | set(parameters)
    try:
        yield
    finally:
        parameterized._param__private.syncing = old


@contextmanager
def edit_constant(parameterized: 'Parameterized') -> Generator[None, None, None]:
    """
    Context manager to temporarily set parameters on a Parameterized object
    to ``constant=False`` to allow editing them.

    The ``edit_constant`` context manager allows temporarily disabling the ``constant``
    property of all parameters on the given :class:`Parameterized` object, enabling them
    to be modified. Once the context exits, the original ``constant`` states are restored.

    Parameters
    ----------
    parameterized : Parameterized
        The :class:`Parameterized` object whose parameters will have their ``constant``
        property temporarily disabled.

    Yields
    ------
    None
        A context where all parameters of the :class:`Parameterized` object can be modified.

    Examples
    --------
    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     constant_param = param.Number(default=10, constant=True)
    >>> p = MyClass()

    Use ``edit_constant`` to modify the constant parameter:

    >>> with param.edit_constant(p):
    ...     p.constant_param = 20
    >>> p.constant_param
    20
    """
    kls_params = parameterized.param.objects(instance=False)
    inst_params = parameterized._param__private.params
    init_inst_params = list(inst_params)
    updated = []
    for pname, pobj in (kls_params | inst_params).items():
        if pobj.constant:
            pobj.constant = False
            updated.append(pname)
    try:
        yield
    finally:
        for pname in updated:
            # Some operations trigger a parameter instantiation (copy),
            # we ensure both the class and instance parameters are reset.
            if pname in kls_params and pname not in init_inst_params:
                type(parameterized).param[pname].constant=True
            if pname in inst_params:
                parameterized.param[pname].constant = True


@contextmanager
def discard_events(parameterized: 'Parameterized') -> Generator[None, None, None]:
    """
    Context manager that discards any events within its scope triggered on the
    supplied Parameterized object.

    The ``discard_events`` context manager ensures that any events triggered
    on the supplied :class:`Parameterized` object during its scope are discarded.
    This allows for silent changes to dependent parameters, making it useful
    for initialization or setup phases where changes should not propagate
    to watchers or dependencies.

    Be cautious when using this context manager, as it bypasses the normal
    dependency mechanism. Manual changes made within this context may
    leave the object in an inconsistent state if dependencies are meant
    to ensure parameter consistency.

    Parameters
    ----------
    parameterized : Parameterized
        The :class:`Parameterized` object whose events will be suppressed.

    Yields
    ------
    None
        A context where events on the supplied :class:`Parameterized` object are discarded.

    References
    ----------
    For more details, see the Param User Guide:
    https://param.holoviz.org/user_guide/Dependencies_and_Watchers.html#discard-events

    Examples
    --------
    Instantiate Parameterized and print its value(s) when changed:

    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     a = param.Number(default=1)
    ...
    ...     @param.depends('a', watch=True)
    ...     def on_a(self):
    ...         print(self.a)
    >>> p = MyClass()
    >>> p.a = 2
    2

    Use ``discard_events`` to suppress events:

    >>> with param.parameterized.discard_events(p):
    ...     p.a = 3
    # Nothing is printed
    """
    batch_watch = parameterized.param._BATCH_WATCH
    parameterized.param._BATCH_WATCH = True
    watchers, events = (list(parameterized.param._state_watchers),
                        list(parameterized.param._events))
    try:
        yield
    finally:
        parameterized.param._BATCH_WATCH = batch_watch
        parameterized.param._state_watchers = watchers
        parameterized.param._events = events


def classlist(class_):
    """
    Return a list of the class hierarchy above (and including) the given class.

    Same as ``inspect.getmro(class_)[::-1]``
    """
    return inspect.getmro(class_)[::-1]


def get_all_slots(class_):
    """
    Return a list of slot names for slots defined in `class_` and its
    superclasses.
    """
    # A subclass's __slots__ attribute does not contain slots defined
    # in its superclass (the superclass' __slots__ end up as
    # attributes of the subclass).
    all_slots = []
    parent_param_classes = [c for c in classlist(class_)[1::]]
    for c in parent_param_classes:
        if hasattr(c,'__slots__'):
            all_slots+=c.__slots__
    return all_slots


def get_occupied_slots(instance):
    """
    Return a list of slots for which values have been set.

    (While a slot might be defined, if a value for that slot hasn't
    been set, then it's an AttributeError to request the slot's
    value.)
    """
    return [slot for slot in get_all_slots(type(instance))
            if hasattr(instance,slot)]


class bothmethod:
    """
    'optional @classmethod'.

    A decorator that allows a method to receive either the class
    object (if called on the class) or the instance object
    (if called on the instance) as its first argument.
    """

    def __init__(self, method):
        self.method = method

    def __get__(self, instance, owner):
        if instance is None:
            # Class call
            return self.method.__get__(owner)
        else:
            # Instance call
            return self.method.__get__(instance, owner)


def _getattrr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)
    return reduce(_getattr, [obj] + attr.split('.'))


def no_instance_params(cls):
    """Disables instance parameters on the class."""
    cls._param__private.disable_instance_params = True
    return cls


def _instantiate_param_obj(paramobj, owner=None):
    """Return a Parameter object suitable for instantiation given the class's Parameter object."""
    # Shallow-copy Parameter object without the watchers
    p = copy.copy(paramobj)
    p.owner = owner

    # Reset watchers since class parameter watcher should not execute
    # on instance parameters
    p.watchers = {}

    # shallow-copy any mutable slot values other than the actual default
    for s in p.__class__._all_slots_:
        v = getattr(p, s)
        if _is_mutable_container(v) and s != "default":
            setattr(p, s, copy.copy(v))
    return p


def _instantiated_parameter(parameterized, param):
    """
    Given a Parameterized object and one of its class Parameter objects,
    return the appropriate Parameter object for this instance, instantiating
    it if need be.
    """
    if (getattr(parameterized._param__private, 'initialized', False) and param.per_instance and
        not getattr(type(parameterized)._param__private, 'disable_instance_params', False)):
        key = param.name

        if key not in parameterized._param__private.params:
            parameterized._param__private.params[key] = _instantiate_param_obj(param, parameterized)

        param = parameterized._param__private.params[key]

    return param


def instance_descriptor(f):
    # If parameter has an instance Parameter, delegate setting
    def _f(self, obj, val):
        # obj is None when the metaclass is setting
        if obj is not None:
            instance_param = obj._param__private.params.get(self.name)
            if instance_param is None:
                instance_param = _instantiated_parameter(obj, self)
            if instance_param is not None and self is not instance_param:
                instance_param.__set__(obj, val)
                return
        return f(self, obj, val)
    return _f


def get_method_owner(method):
    """Get the instance that owns the supplied method."""
    if not inspect.ismethod(method):
        return None
    if isinstance(method, partial):
        method = method.func
    return method.__self__


@accept_arguments
def output(func, *output, **kw):
    """
    Annotate a method to declare its outputs with specific types.

    The ``output`` decorator allows annotating a method in a :class:`Parameterized` class
    to declare the types of the values it returns. This provides metadata for the
    method's outputs, which can be queried using the :meth:`~Parameters.outputs`
    method. Outputs can be declared as unnamed, named, or typed, and the decorator
    supports multiple outputs.

    Parameters
    ----------
    func : callable
        The method being annotated.
    *output : tuple or Parameter or type, optional
        Positional arguments to declare outputs. Can include:

        - :class:`Parameter` instances or Python object types (e.g., :class:`int`, :class:`str`).
        - Tuples of the form ``(name, type)`` to declare named outputs.
        - Multiple such tuples for declaring multiple outputs.
    **kw : dict, optional
        Keyword arguments mapping output names to types. Types can be:

        - :class:`Parameter` instances.
        - Python object types, which will be converted to :class:`ClassSelector`.

    Returns
    -------
    callable
        The decorated method with annotated outputs.

    Raises
    ------
    ValueError
        If an invalid type is provided for an output or duplicate names are
        used for multiple outputs.

    Notes
    -----
    - Unnamed outputs default to the method name.
    - Python types are converted to :class:`ClassSelector` instances.
    - If no arguments are provided, the output is assumed to be an object
      without a specific type.

    Examples
    --------
    Declare a method with an unspecified output type:

    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     @param.output()
    ...     def my_method(self):
    ...         return 42

    Query the outputs:

    >>> MyClass().param.outputs()
    {'my_method': (<param.parameterized.Parameter at 0x7f12f8d334c0>,
      <bound method MyClass.my_method of MyClass(name='MyClass00004')>,
      None)}

    Declare a method with a specified type:

    >>> class MyClass(param.Parameterized):
    ...     @param.output(param.Number())
    ...     def my_method(self):
    ...         return 42.0

    Use a custom output name and type:

    >>> class MyClass(param.Parameterized):
    ...     @param.output(custom_name=param.Number())
    ...     def my_method(self):
    ...         return 42.0

    Declare multiple outputs using keyword arguments:

    >>> class MyClass(param.Parameterized):
    ...     @param.output(number=param.Number(), string=param.String())
    ...     def my_method(self):
    ...         return 42.0, "hello"

    Declare multiple outputs using tuples:

    >>> class MyClass(param.Parameterized):
    ...     @param.output(('number', param.Number()), ('string', param.String()))
    ...     def my_method(self):
    ...         return 42.0, "hello"

    Declare a method returning a Python object type:

    >>> class MyClass(param.Parameterized):
    ...     @param.output(int)
    ...     def my_method(self):
    ...         return 42
    """
    if output:
        outputs = []
        for i, out in enumerate(output):
            i = i if len(output) > 1 else None
            if isinstance(out, tuple) and len(out) == 2 and isinstance(out[0], str):
                outputs.append(out+(i,))
            elif isinstance(out, str):
                outputs.append((out, Parameter(), i))
            else:
                outputs.append((None, out, i))
    elif kw:
        # (requires keywords to be kept ordered, which was not true in previous versions)
        outputs = [(name, otype, i if len(kw) > 1 else None)
                   for i, (name, otype) in enumerate(kw.items())]
    else:
        outputs = [(None, Parameter(), None)]

    names, processed = [], []
    for name, otype, i in outputs:
        if isinstance(otype, type):
            if issubclass(otype, Parameter):
                otype = otype()
            else:
                from .import ClassSelector
                otype = ClassSelector(class_=otype)
        elif isinstance(otype, tuple) and all(isinstance(t, type) for t in otype):
            from .import ClassSelector
            otype = ClassSelector(class_=otype)
        if not isinstance(otype, Parameter):
            raise ValueError('output type must be declared with a Parameter class, '
                             'instance or a Python object type.')
        processed.append((name, otype, i))
        names.append(name)

    if len(set(names)) != len(names):
        raise ValueError('When declaring multiple outputs each value '
                         'must be unique.')

    _dinfo = getattr(func, '_dinfo', {})
    _dinfo.update({'outputs': processed})

    @wraps(func)
    def _output(*args,**kw):
        return func(*args,**kw)

    _output._dinfo = _dinfo

    return _output


def _parse_dependency_spec(spec):
    """
    Parse param.depends string specifications into three components.

    The 3 components are:

    1. The dotted path to the sub-object
    2. The attribute being depended on, i.e. either a parameter or method
    3. The parameter attribute being depended on
    """
    assert spec.count(":")<=1
    spec = spec.strip()
    m = re.match("(?P<path>[^:]*):?(?P<what>.*)", spec)
    what = m.group('what')
    path = "."+m.group('path')
    m = re.match(r"(?P<obj>.*)(\.)(?P<attr>.*)", path)
    obj = m.group('obj')
    attr = m.group("attr")
    return obj or None, attr, what or 'value'


def _params_depended_on(minfo, dynamic=True, intermediate=True):
    """
    Resolve dependencies declared on a Parameterized method.

    Dynamic dependencies, i.e. dependencies on sub-objects which may
    or may not yet be available, are only resolved if dynamic=True.
    By default intermediate dependencies, i.e. dependencies on the
    path to a sub-object are returned. For example for a dependency
    on 'a.b.c' dependencies on 'a' and 'b' are returned as long as
    intermediate=True.

    Returns lists of concrete dependencies on available parameters
    and dynamic dependencies specifications which have to resolved
    if the referenced sub-objects are defined.
    """
    deps, dynamic_deps = [], []
    dinfo = getattr(minfo.method, "_dinfo", {})
    for d in dinfo.get('dependencies', list(minfo.cls.param)):
        ddeps, ddynamic_deps = (minfo.cls if minfo.inst is None else minfo.inst).param._spec_to_obj(d, dynamic, intermediate)
        dynamic_deps += ddynamic_deps
        for dep in ddeps:
            if isinstance(dep, PInfo):
                deps.append(dep)
            else:
                method_deps, method_dynamic_deps = _params_depended_on(dep, dynamic, intermediate)
                deps += method_deps
                dynamic_deps += method_dynamic_deps
    return deps, dynamic_deps


def _resolve_mcs_deps(obj, resolved, dynamic, intermediate=True):
    """
    Resolve constant and dynamic parameter dependencies previously
    obtained using the _params_depended_on function. Existing resolved
    dependencies are updated with a supplied parameter instance while
    dynamic dependencies are resolved if possible.
    """
    dependencies = []
    for dep in resolved:
        if not issubclass(type(obj), dep.cls):
            dependencies.append(dep)
            continue
        inst = obj if dep.inst is None else dep.inst
        dep = PInfo(inst=inst, cls=dep.cls, name=dep.name,
                    pobj=inst.param[dep.name], what=dep.what)
        dependencies.append(dep)
    for dep in dynamic:
        subresolved, _ = obj.param._spec_to_obj(dep.spec, intermediate=intermediate)
        for subdep in subresolved:
            if isinstance(subdep, PInfo):
                dependencies.append(subdep)
            else:
                dependencies += _params_depended_on(subdep, intermediate=intermediate)[0]
    return dependencies


def _skip_event(*events, **kwargs):
    """
    Check whether a subobject event should be skipped.

    Returns True if all the values on the new subobject
    match the values on the previous subobject.
    """
    what = kwargs.get('what', 'value')
    changed = kwargs.get('changed')
    if changed is None:
        return False
    for e in events:
        for p in changed:
            if what == 'value':
                old = Undefined if e.old is None else _getattrr(e.old, p, None)
                new = Undefined if e.new is None else _getattrr(e.new, p, None)
            else:
                old = Undefined if e.old is None else _getattrr(e.old.param[p], what, None)
                new = Undefined if e.new is None else _getattrr(e.new.param[p], what, None)
            if not Comparator.is_equal(old, new):
                return False
    return True


def extract_dependencies(function):
    """Extract references from a method or function that declares the references."""
    subparameters = list(function._dinfo['dependencies'])+list(function._dinfo['kw'].values())
    params = []
    for p in subparameters:
        if isinstance(p, str):
            owner = get_method_owner(function)
            *subps, p = p.split('.')
            for subp in subps:
                owner = getattr(owner, subp, None)
                if owner is None:
                    raise ValueError('Cannot depend on undefined sub-parameter {p!r}.')
            if p in owner.param:
                pobj = owner.param[p]
                if pobj not in params:
                    params.append(pobj)
            else:
                for sp in extract_dependencies(getattr(owner, p)):
                    if sp not in params:
                        params.append(sp)
        elif p not in params:
            params.append(p)
    return params


# Two callers at the module top level to support pickling.
async def _async_caller(*events, what='value', changed=None, callback=None, function=None):
    if callback:
        callback(*events)
    if not _skip_event or not _skip_event(*events, what=what, changed=changed):
        await function()


def _sync_caller(*events, what='value', changed=None, callback=None, function=None):
    if callback:
        callback(*events)
    if not _skip_event(*events, what=what, changed=changed):
        return function()


def _m_caller(self, method_name, what='value', changed=None, callback=None):
    """
    Wrap a method call adding support for scheduling a callback
    before it is executed and skipping events if a subobject has
    changed but its values have not.
    """
    function = getattr(self, method_name)
    _caller = _async_caller if iscoroutinefunction(function) else _sync_caller
    caller = partial(_caller, what=what, changed=changed, callback=callback, function=function)
    caller._watcher_name = method_name
    return caller


def _add_doc(obj, docstring):
    """Add a docstring to a namedtuple."""
    obj.__doc__ = docstring


PInfo = namedtuple("PInfo", "inst cls name pobj what")
_add_doc(PInfo,
    """
    Object describing something being watched about a Parameter.

    `inst`: Parameterized instance owning the Parameter, or None

    `cls`: Parameterized class owning the Parameter

    `name`: Name of the Parameter being watched

    `pobj`: Parameter object being watched

    `what`: What is being watched on the Parameter (either 'value' or a slot name)
    """)

MInfo = namedtuple("MInfo", "inst cls name method")
_add_doc(MInfo,
    """
    Object describing a Parameterized method being watched.

    `inst`: Parameterized instance owning the method, or None

    `cls`: Parameterized class owning the method

    `name`: Name of the method being watched

    `method`: bound method of the object being watched
    """)

DInfo = namedtuple("DInfo", "spec")
_add_doc(DInfo,
    """
    Object describing dynamic dependencies.
    `spec`: Dependency specification to resolve
    """)

Event = namedtuple("Event", "what name obj cls old new type")
_add_doc(Event,
    """
    Object representing an event that triggers a Watcher.

    `what`: What is being watched on the Parameter (either value or a slot name)

    `name`: Name of the Parameter that was set or triggered

    `obj`: Parameterized instance owning the watched Parameter, or None

    `cls`: Parameterized class owning the watched Parameter

    `old`: Previous value of the item being watched

    `new`: New value of the item being watched

    `type`: `triggered` if this event was triggered explicitly), `changed` if
    the item was set and watching for `onlychanged`, `set` if the item was set,
    or  None if type not yet known
    """)

_Watcher = namedtuple("Watcher", "inst cls fn mode onlychanged parameter_names what queued precedence")

class Watcher(_Watcher):
    """
    Object declaring a callback function to invoke when an Event is
    triggered on a watched item.

    `inst`: Parameterized instance owning the watched Parameter, or
    None

    `cls`: Parameterized class owning the watched Parameter

    `fn`: Callback function to invoke when triggered by a watched
    Parameter

    `mode`: 'args' for param.watch (call `fn` with PInfo object
    positional args), or 'kwargs' for param.watch_values (call `fn`
    with <param_name>:<new_value> keywords)

    `onlychanged`: If True, only trigger for actual changes, not
    setting to the current value

    `parameter_names`: List of Parameters to watch, by name

    `what`: What to watch on the Parameters (either 'value' or a slot
    name)

    `queued` : Immediately invoke callbacks triggered during processing
    of an Event (if False), or queue them up for processing
    later, after this event has been handled (if True)

    `precedence` : A numeric value which determines the precedence of
    the watcher.  Lower precedence values are executed
    with higher priority.
    """

    def __new__(cls_, *args, **kwargs):
        """Create a new instance of the class, setting a default precedence value.

        This method allows creating a `Watcher` instance without explicitly
        specifying a `precedence` value. If `precedence` is not provided, it
        defaults to `0`.

        Parameters
        ----------
        *args : Any
            Positional arguments to initialize the instance.
        **kwargs : Any
            Keyword arguments to initialize the instance.

        Returns
        -------
        An instance of the Watcher with the specified or default values.
        """
        values = dict(zip(cls_._fields, args))
        values.update(kwargs)
        if 'precedence' not in values:
            values['precedence'] = 0
        return super().__new__(cls_, **values)

    def __str__(self):
        cls = type(self)
        attrs = ', '.join([f'{f}={getattr(self, f)!r}' for f in cls._fields])
        return f"{cls.__name__}({attrs})"



class ParameterMetaclass(type):
    """Metaclass allowing control over creation of Parameter classes."""

    def __new__(mcs, classname, bases, classdict):

        # store the class's docstring in __classdoc
        if '__doc__' in classdict:
            classdict['__classdoc']=classdict['__doc__']

        # when asking for help on Parameter *object*, return the doc slot
        classdict['__doc__'] = property(attrgetter('doc'))

        # Compute all slots in order, using a dict later turned into a list
        # as it's the fastest way to get an ordered set in Python
        all_slots = {}
        for bcls in set(chain(*(base.__mro__[::-1] for base in bases))):
            all_slots.update(dict.fromkeys(getattr(bcls, '__slots__', [])))

        # To get the benefit of slots, subclasses must themselves define
        # __slots__, whether or not they define attributes not present in
        # the base Parameter class.  That's because a subclass will have
        # a __dict__ unless it also defines __slots__.
        if '__slots__' not in classdict:
            classdict['__slots__'] = []
        else:
            all_slots.update(dict.fromkeys(classdict['__slots__']))

        classdict['_all_slots_'] = list(all_slots)

        # No special handling for a __dict__ slot; should there be?
        return type.__new__(mcs, classname, bases, classdict)

    def __getattribute__(mcs,name):
        if name=='__doc__':
            # when asking for help on Parameter *class*, return the
            # stored class docstring
            return type.__getattribute__(mcs,'__classdoc')
        else:
            return type.__getattribute__(mcs,name)


_UDPATE_PARAMETER_SIGNATURE = _in_ipython() or (os.getenv("PARAM_PARAMETER_SIGNATURE", "false").lower() in ("1" , "true"))


class _ParameterBase(metaclass=ParameterMetaclass):
    """
    Base Parameter class used to dynamically update the signature of all
    the Parameters.
    """

    @classmethod
    def _modified_slots_defaults(cls):
        defaults = cls._slot_defaults.copy()
        defaults['label'] = defaults.pop('_label')
        return defaults

    @classmethod
    def __init_subclass__(cls):
        super().__init_subclass__()
        if not _UDPATE_PARAMETER_SIGNATURE:
            return
        # _update_signature has been tested against the Parameters available
        # in Param, we don't want to break the Parameters created elsewhere
        # so wrapping this in a loose try/except.
        try:
            cls._update_signature()
        except Exception:
            # The super signature has been changed so we need to get the one
            # from the class constructor directly.
            cls.__signature__ = inspect.signature(cls.__init__)

    @classmethod
    def _update_signature(cls):
        defaults = cls._modified_slots_defaults()
        new_parameters = {}

        for i, kls in enumerate(cls.mro()):
            if kls.__name__.startswith('_'):
                continue
            sig = inspect.signature(kls.__init__)
            for pname, parameter in sig.parameters.items():
                if pname == 'self':
                    continue
                if i >= 1 and parameter.default == inspect.Signature.empty:
                    continue
                if parameter.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL):
                    continue
                if getattr(parameter, 'default', None) is Undefined:
                    if pname not in defaults:
                        raise LookupError(
                            f'Argument {pname!r} of Parameter {cls.__name__!r} has no '
                            'entry in _slot_defaults.'
                        )
                    default = defaults[pname]
                    if callable(default) and hasattr(default, 'sig'):
                        default = default.sig
                    new_parameter = parameter.replace(default=default)
                else:
                    new_parameter = parameter
                if i >= 1:
                    new_parameter = new_parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
                new_parameters.setdefault(pname, new_parameter)

        def _sorter(p):
            if p.default == inspect.Signature.empty:
                return 0
            else:
                return 1

        new_parameters = sorted(new_parameters.values(), key=_sorter)
        new_sig = sig.replace(parameters=new_parameters)
        cls.__signature__ = new_sig


class Parameter(_ParameterBase):
    """
    Base :class:`Parameter` type to hold any type of Python object.

    Parameters are a special kind of class attribute implemented as descriptor.
    Setting a Parameterized class attribute to a `Parameter` instance enables
    enhanced functionality, including type and range validation at assignment,
    support for constant and read-only parameters, documentation strings and
    dynamic parameter values.

    Parameters can only be used as class attributes of :class:`Parameterized` classes.
    Using them in standalone contexts or with non-:class:`Parameterized` classes will
    not provide the described behavior.

    Notes
    -----
    Parameters provide lots of features.

    **Dynamic Behavior**:

    - Parameters provide support for dynamic values, type validation,
      and range checking.
    - Parameters can be declared as constant or read-only.

    **Automatic Initialization**:

    - Parameters can be set during object construction using keyword
      arguments. For example: ``myfoo = Foo(alpha=0.5); print(myfoo.alpha)``.

    - If custom constructors are implemented, they can still pass
      keyword arguments to the superclass to allow Parameter initialization.

    **Inheritance**:

    - :class:`Parameterized` classes automatically inherit parameters from
      their superclasses. Attributes can be selectively overridden.

    **Subclassing**:

    - The :class:`Parameter` class can be subclassed to create custom behavior,
      such as validating specific ranges or generating values dynamically.

    **GUI Integration**:

    - Parameters provide sufficient metadata for auto-generating property
      sheets in graphical user interfaces, enabling user-friendly
      parameter editing.

    Examples
    --------
    Define a :class:`Parameterized` class with parameters:

    >>> import param
    >>> class Foo(param.Parameterized):
    ...     alpha = param.Parameter(default=0.1, doc="The starting value.")
    ...     beta = param.Parameter(default=0.5, doc="The standard deviation.", constant=True)

    When no initial value is provided the default is used:

    >>> Foo().alpha
    0.1

    When an initial value is provided it is used:

    >>> foo = Foo(alpha=0.5)
    >>> foo.alpha
    0.5

    Constant parameters cannot be modified:

    >>> foo.beta = 0.1  # Cannot be changed since it's constant
    ...
    TypeError: Constant parameter 'beta' cannot be modified
    """

    # Because they implement __get__ and __set__, Parameters are known
    # as 'descriptors' in Python; see "Implementing Descriptors" and
    # "Invoking Descriptors" in the 'Customizing attribute access'
    # section of the Python reference manual:
    # http://docs.python.org/ref/attribute-access.html
    #
    # Overview of Parameters for programmers
    # ======================================
    #
    # Consider the following code:
    #
    #
    # class A(Parameterized):
    #     p = Parameter(default=1)
    #
    # a1 = A()
    # a2 = A()
    #
    #
    # * a1 and a2 share one Parameter object (A.__dict__['p']).
    #
    # * The default (class) value of p is stored in this Parameter
    #   object (A.__dict__['p'].default).
    #
    # * If the value of p is set on a1 (e.g. a1.p=2), a1's value of p
    #   is stored in a1 itself (a1._param__private.values['p'])
    #
    # * When a1.p is requested, a1._param__private.values['p'] is
    #   returned. When a2.p is requested, 'p' is not found in
    #   a1._param__private.values, so A.__dict__['p'].default (i.e. A.p) is
    #   returned instead.
    #
    #
    # Be careful when referring to the 'name' of a Parameter:
    #
    # * A Parameterized class has a name for the attribute which is
    #   being represented by the Parameter ('p' in the example above);
    #   in the code, this is called the 'name'.
    #
    # * When a Parameterized instance has its own local value for a
    #   parameter, it is stored as 'p._param__private.values[X]' where X is the
    #   name of the Parameter


    # So that the extra features of Parameters do not require a lot of
    # overhead, Parameters are implemented using __slots__ (see
    # http://www.python.org/doc/2.4/ref/slots.html).  Instead of having
    # a full Python dictionary associated with each Parameter instance,
    # Parameter instances have an enumerated list (named __slots__) of
    # attributes, and reserve just enough space to store these
    # attributes.  Using __slots__ requires special support for
    # operations to copy and restore Parameters (e.g. for Python
    # persistent storage pickling); see __getstate__ and __setstate__.
    __slots__ = ['name', 'default', 'default_factory', 'doc',
                 'precedence', 'instantiate', 'constant', 'readonly',
                 'pickle_default_value', 'allow_None', 'per_instance',
                 'watchers', 'owner', 'allow_refs', 'nested_refs', '_label',
                 'metadata',]

    # Note: When initially created, a Parameter does not know which
    # Parameterized class owns it, nor does it know its names
    # (attribute name, internal name). Once the owning Parameterized
    # class is created, owner, and name are
    # set.

    _serializers = {'json': serializer.JSONSerialization}

    _slot_defaults = dict(
        default=None, precedence=None, doc=None, _label=None, instantiate=False,
        constant=False, readonly=False, pickle_default_value=True, allow_None=False,
        per_instance=True, allow_refs=False, nested_refs=False, default_factory=None,
        metadata=None,
    )

    # Parameters can be updated during Parameterized class creation when they
    # are defined multiple times in a class hierarchy. We have to record which
    # Parameter slots require the default value to be re-validated. Any slots
    # in this list do not have to trigger such re-validation.
    _non_validated_slots = ['_label', 'doc', 'name', 'precedence',
                            'constant', 'pickle_default_value',
                            'watchers', 'owner', 'metadata']

    @typing.overload
    def __init__(
        self,
        default=None, *,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__( # pylint: disable-msg=R0913
        self,
        default=Undefined,
        *,
        doc=Undefined,
        label=Undefined,
        precedence=Undefined,
        instantiate=Undefined,
        constant=Undefined,
        readonly=Undefined,
        pickle_default_value=Undefined,
        allow_None=Undefined,
        per_instance=Undefined,
        allow_refs=Undefined,
        nested_refs=Undefined,
        default_factory=Undefined,
        metadata=Undefined,
    ):
        """
        Initialize a new :class:`Parameter` object with the specified attributes.

        Parameters
        ----------
        default : Any, optional
            The owning class's value for the attribute represented by this
            parameter, which can be overridden in an instance.
            Default is ``None``.
        default_factory : callable, optional
            A callable used to generate the attribute value. It can be provided
            directly (in which case it is invoked with no arguments) or wrapped
            in a :class:`DefaultFactory` for more advanced behavior (in which
            case it receives ``cls``, ``self``, and ``parameter`` as arguments).
            When specified, ``default_factory`` takes precedence over ``default``.
            The factory is invoked once the :class:`Parameterized` instance
            is initialized.

            .. versionadded:: 2.3.0
        doc : str | None, optional
            A documentation string describing the purpose of the parameter.
            Default is ``None``.
        label : str | None, optional
            An optional text label used when displaying this parameter, such as
            in a listing. If not specified, the parameter's attribute name in
            the owning :class:`Parameterized` object is used.
        precedence : float | None, optional
            A numeric value, usually in the range 0.0 to 1.0, that determines
            the order of the parameter in a listing or user interface. A negative
            precedence indicates a parameter that should be hidden in such
            listings. Default is ``None``.
        instantiate : bool, optional
            Whether the default value of this parameter will be deepcopied when
            a :class:`Parameterized` object is instantiated (``True``), or if the
            single default value will be shared by all Parameterized instances
            (``False``, the default).
            For an immutable ``Parameter`` value, it is best to leave ``instantiate``
            at the default of ``False``, so that a user can choose to change the
            value at the ``Parameterized`` instance level (affecting only that
            instance) or at the :class:`Parameterized` class or superclass level
            (affecting all existing and future instances of that class or
            superclass). For a mutable ``Parameter`` value, the default of ``False``
            is also appropriate if you want all instances to share the same
            value state, e.g. if they are each simply referring to a single
            global object like a singleton. If instead each ``Parameterized``
            should have its own independently mutable value, instantiate should
            be set to ``True``, but note that there is then no simple way to
            change the value of this parameter at the class or superclass
            level, because each instance, once created, will then have an
            independently deepcopied value. Default is ``False``.
        constant : bool, optional
            If ``True``, the parameter value can only be set at the class level
            or in a :class:`Parameterized` constructor call. The value is otherwise
            constant on the ``Parameterized`` instance, once it has been
            constructed. Default is ``False``.
        readonly : bool, optional
            If ``True``, the parameter value cannot be modified at the class or
            instance level. Default is ``False``.
        pickle_default_value : bool, optional
            Whether the default value should be pickled. Set to ``False`` in rare
            cases, such as system-specific file paths.

            .. deprecated:: 2.3.0
        allow_None : bool, optional
            If ``True``, allows ``None`` as a valid parameter value. If the default
            value is ``None``, this is automatically set to ``True``. Default is
            ``False``.
        per_instance : bool, optional
            Whether to create a separate ``Parameter`` object for each instance of
            the ``Parameterized`` class (``True``), or share the same ``Parameter``
            object across all instances (``False``). See also ``instantiate``,
            which is conceptually similar but affects the parameter value
            rather than the parameter object. Default is ``True``.
        allow_refs : bool, optional
            If ``True``, allows linking parameter references to this parameter,
            meaning the value will automatically reflect the current value of
            the reference that is passed in. Default is ``False``.
        nested_refs : bool, optional
            If ``True`` and ``allow_refs=True``, inspects nested objects (e.g.,
            dictionaries, lists, slices, tuples) for references and resolves
            them automatically. Default is ``False``.
        metadata : Mapping, optional
            An arbitrary mapping, to be used to store additional metadata
            than cannot be declared with the other attributes. Useful for
            third-party libraries to extend Param. Default is ``None``.

            .. versionadded:: 2.3.0

        Examples
        --------
        Define a parameter with a default value:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     my_param = param.Parameter(default=10, doc="An example parameter.")
        >>> instance = MyClass()
        >>> instance.my_param
        10

        Use a constant parameter:

        >>> class ConstantExample(param.Parameterized):
        ...     my_param = param.Parameter(default=5, constant=True)
        >>> instance = ConstantExample()
        >>> instance.my_param = 10  # Raises an error
        ...
        TypeError: Constant parameter 'my_param' cannot be modified.
        """
        if default_factory is not Undefined and not callable(default_factory):
            raise TypeError(
                "default_factory must be a callable, "
                f"not {type(default_factory)!r}."
            )
        self.name = None
        self.owner = None
        self.allow_refs = allow_refs
        self.nested_refs = nested_refs
        self.precedence = precedence
        self.default = default
        self.default_factory = default_factory
        self.doc = doc
        if constant is True or readonly is True:  # readonly => constant
            self.constant = True
        else:
            self.constant = constant
        self.readonly = readonly
        self._label = label
        self._set_instantiate(instantiate)
        if pickle_default_value is False:
            warnings.warn(
                'pickle_default_value has been deprecated.',
                category=_ParamDeprecationWarning,
                stacklevel=_find_stack_level(),
            )
        self.pickle_default_value = pickle_default_value
        self._set_allow_None(allow_None)
        self.metadata = metadata
        self.watchers = {}
        self.per_instance = per_instance

    @classmethod
    def serialize(cls, value):
        """Given the parameter value, return a Python value suitable for serialization."""
        return value

    @classmethod
    def deserialize(cls, value):
        """Given a serializable Python value, return a value that the parameter can be set to."""
        return value

    def schema(
            self,
            safe: bool = False,
            subset: Optional[Iterable[str]] = None,
            mode: str = 'json',
        ) -> dict[str, Any]:
        """
        Generate a schema for the parameters of the :class:`Parameterized` object.

        This method returns a schema representation of the object's parameters,
        including metadata such as types, default values, and documentation.
        The schema format is determined by the specified serialization mode.

        Parameters
        ----------
        safe : bool, optional
            If ``True``, only includes parameters marked as safe for serialization.
            Default is ``False``.
        subset : iterable of str or None, optional
            A list of parameter names to include in the schema. If ``None``, all
            parameters are included. Default is ``None``.
        mode : str, optional
            The serialization format to use. Must be one of the available
            serialization formats registered in ``_serializers``. Default is ``'json'``.

        Returns
        -------
        dict[str, Any]
            A schema dictionary representing the parameters of the object and their
            associated metadata.

        Examples
        --------
        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Number(default=1, bounds=(0, 10), doc="A numeric parameter.")
        ...     b = param.String(default="hello", doc="A string parameter.")
        >>> instance = MyClass()

        Get the schema in JSON format:

        >>> instance.param.a.schema()
        {'type': 'number', 'minimum': 0, 'maximum': 10}
        """
        if mode not in  self._serializers:
            raise KeyError(f'Mode {mode!r} not in available serialization formats {list(self._serializers.keys())!r}')
        return self._serializers[mode].param_schema(self.__class__.__name__, self,
                                                    safe=safe, subset=subset)

    @property
    def rx(self):
        """
        The reactive operations namespace.

        Provides reactive versions of operations that cannot be made reactive through
        operator overloading. This includes operations such as ``.rx.and_`` and ``.rx.bool``.

        Calling this namespace (``.rx()``) creates and returns a reactive expression, enabling
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
        from .reactive import reactive_ops
        return reactive_ops(self)

    @property
    def label(self) -> str:
        """
        Get the label for this parameter.

        The ``label`` property returns a human-readable text label associated with
        the parameter.

        Returns
        -------
        str
            The label for the parameter, either automatically generated from the
            name or the custom label if explicitly set.

        Examples
        --------
        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     param_1 = param.Parameter()
        ...     param_2 = param.Parameter(label="My Label")
        >>> instance = MyClass()

        Access the automatically generated label:

        >>> instance.param.param_1.label
        'Param 1'  # Based on `label_formatter` applied to the name

        Access the manually specified label:

        >>> instance.param.param_2.label
        'My Label'
        """
        if self.name and self._label is None:
            return label_formatter(self.name)
        else:
            return self._label

    @label.setter
    def label(self, val):
        self._label = val

    def _set_allow_None(self, allow_None):
        # allow_None is set following these rules (last takes precedence):
        # 1. to False by default
        # 2. to the value provided in the constructor, if any
        # 3. to True if default is None
        if self.default is None:
            self.allow_None = True
        elif allow_None is not Undefined:
            self.allow_None = allow_None
        else:
            self.allow_None = self._slot_defaults['allow_None']

    def _set_instantiate(self,instantiate):
        """Constant parameters must be instantiated."""
        # instantiate doesn't actually matter for read-only
        # parameters, since they can't be set even on a class.  But
        # having this code avoids needless instantiation.
        if self.readonly:
            self.instantiate = False
        elif instantiate is not Undefined:
            self.instantiate = instantiate
        else:
            # Default value
            self.instantiate = self._slot_defaults['instantiate']

    def __setattr__(self, attribute, value):
        if attribute == 'name':
            name = getattr(self, 'name', None)
            if name is not None and value != name:
                raise AttributeError("Parameter name cannot be modified after "
                                     "it has been bound to a Parameterized.")

        is_slot = attribute in self.__class__._all_slots_
        has_watcher = attribute != "default" and attribute in getattr(self, 'watchers', [])
        if not (is_slot or has_watcher):
            # Return early if attribute is not a slot
            return super().__setattr__(attribute, value)

        # Otherwise get the old value so we can call watcher/on_set
        old = getattr(self, attribute, NotImplemented)
        if is_slot:
            try:
                self._on_set(attribute, old, value)
            except AttributeError:
                pass

        super().__setattr__(attribute, value)
        if has_watcher and old is not NotImplemented:
            self._trigger_event(attribute, old, value)

    def _trigger_event(self, attribute, old, new):
        event = Event(what=attribute, name=self.name, obj=None, cls=self.owner,
                      old=old, new=new, type=None)
        for watcher in self.watchers[attribute]:
            self.owner.param._call_watcher(watcher, event)
        if not self.owner.param._BATCH_WATCH:
            self.owner.param._batch_call_watchers()

    def __getattribute__(self, key):
        """
        Allow slot values to be Undefined in an "unbound" parameter, i.e. one
        that is not (yet) owned by a Parameterized object, in which case their
        value will be retrieved from the _slot_defaults dictionary.
        """
        v = object.__getattribute__(self, key)
        # Safely checks for name (avoiding recursion) to decide if this object is unbound
        if v is Undefined and key != "name" and getattr(self, "name", None) is None:
            try:
                v = self._slot_defaults[key]
            except KeyError as e:
                raise KeyError(
                    f'Slot {key!r} on unbound parameter {self.__class__.__name__!r} '
                    'has no default value defined in `_slot_defaults`'
                ) from e
            if callable(v):
                v = v(self)
        return v

    def _on_set(self, attribute, old, value):
        """
        Can be overridden on subclasses to handle changes when parameter
        attribute is set.
        """

    def _update_state(self):
        """
        Can be overridden on subclasses to update a Parameter state, i.e. slot
        values, after the slot values have been set in the inheritance procedure.
        """

    def __get__(self, obj, objtype): # pylint: disable-msg=W0613
        """
        Return the value for this Parameter.

        If called for a Parameterized class, produce that
        class's value (i.e. this Parameter object's 'default'
        attribute).

        If called for a Parameterized instance, produce that
        instance's value, if one has been set - otherwise produce the
        class's value (default).
        """
        if obj is None: # e.g. when __get__ called for a Parameterized class
            result = self.default
        else:
            # Attribute error when .values does not exist (_ClassPrivate)
            # and KeyError when there's no cached value for this parameter.
            try:
                result = obj._param__private.values[self.name]
            except (AttributeError, KeyError):
                result = self.default
        return result

    @instance_descriptor
    def __set__(self, obj, val):
        """
        Set the value for this Parameter.

        If called for a Parameterized class, set that class's
        value (i.e. set this Parameter object's 'default' attribute).

        If called for a Parameterized instance, set the value of
        this Parameter on that instance (i.e. in the instance's
        `values` dictionary located in the private namespace `_param__private`,
        under the parameter's name).

        If the Parameter's constant attribute is True, only allows
        the value to be set for a Parameterized class or on
        uninitialized Parameterized instances.

        If the Parameter's readonly attribute is True, only allows the
        value to be specified in the Parameter declaration inside the
        Parameterized source code. A read-only parameter also
        cannot be set on a Parameterized class.

        Note that until we support some form of read-only
        object, it is still possible to change the attributes of the
        object stored in a constant or read-only Parameter (e.g. one
        item in a list).
        """
        name = self.name
        if obj is not None and self.allow_refs and obj._param__private.initialized:
            syncing = name in obj._param__private.syncing
            ref, deps, val, is_async = obj.param._resolve_ref(self, val)
            refs = obj._param__private.refs
            if ref is not None:
                self.owner.param._update_ref(name, ref)
            elif name in refs and not syncing and not obj._param__private.parameters_state['TRIGGER']:
                del refs[name]
                if name in obj._param__private.async_refs:
                    obj._param__private.async_refs.pop(name).cancel()
            if is_async or val is Undefined:
                return

        self._validate(val)

        _old = NotImplemented
        # obj can be None if __set__ is called for a Parameterized class
        if self.constant or self.readonly:
            if self.readonly:
                raise TypeError("Read-only parameter '%s' cannot be modified" % name)
            elif obj is None:
                _old = self.default
                self.default = val
            elif not obj._param__private.initialized:
                _old = obj._param__private.values.get(self.name, self.default)
                obj._param__private.values[self.name] = val
            else:
                _old = obj._param__private.values.get(self.name, self.default)
                if val is not _old:
                    raise TypeError("Constant parameter '%s' cannot be modified" % name)
        else:
            if obj is None:
                _old = self.default
                self.default = val
            else:
                # When setting a Parameter before calling super.
                if not isinstance(obj._param__private, _InstancePrivate):
                    warnings.warn(
                        f"Setting the Parameter {self.name!r} to {val!r} before "
                        f"the Parameterized class {type(obj).__name__!r} is fully "
                        "instantiated is deprecated and will raise an error in "
                        "a future version. Ensure the value is set after calling "
                        "`super().__init__(**params)` in the constructor.",
                        category=_ParamPendingDeprecationWarning,
                        stacklevel=_find_stack_level(),
                    )
                    obj._param__private = _InstancePrivate(
                        explicit_no_refs=type(obj)._param__private.explicit_no_refs
                    )
                _old = obj._param__private.values.get(name, self.default)
                obj._param__private.values[name] = val
        self._post_setter(obj, val)

        if obj is not None:
            if not hasattr(obj, '_param__private') or not getattr(obj._param__private, 'initialized', False):
                return
            obj.param._update_deps(name)

        if obj is None:
            watchers = self.watchers.get("value")
        elif name in obj._param__private.watchers:
            watchers = obj._param__private.watchers[name].get('value')
            if watchers is None:
                watchers = self.watchers.get("value")
        else:
            watchers = None

        obj = self.owner if obj is None else obj

        if obj is None or not watchers:
            return

        event = Event(what='value', name=name, obj=obj, cls=self.owner,
                      old=_old, new=val, type=None)

        # Copy watchers here since they may be modified inplace during iteration
        for watcher in sorted(watchers, key=lambda w: w.precedence):
            obj.param._call_watcher(watcher, event)
        if not obj.param._BATCH_WATCH:
            obj.param._batch_call_watchers()

    def _validate_value(self, value, allow_None):
        """Validate the parameter value against constraints.

        Parameters
        ----------
        value : Any
            The value to be validated.
        allow_None : bool
            Whether `None` is allowed as a valid value.

        Raises
        ------
        ValueError
            If the value does not meet the parameter's constraints.
        """

    def _validate(self, val):
        """Validate the parameter value and its attributes.

        This method ensures that the given value adheres to the parameter's
        constraints and attributes. Subclasses can extend this method to
        include additional validation logic.

        Parameters
        ----------
        val: Any
            The value to be validated.
        """
        self._validate_value(val, self.allow_None)

    def _post_setter(self, obj, val):
        """Handle actions to be performed after setting a parameter value.

        This method is called after the parameter value has been validated
        and assigned. Subclasses can override this method to implement
        additional behavior post-assignment.

        Parameters
        ----------
        obj : Parameterized
            The object on which the parameter is being set.
        val : Any
            The value that has been assigned to the parameter.
        """

    def __delete__(self,obj):
        raise TypeError("Cannot delete '%s': Parameters deletion not allowed." % self.name)

    def _set_names(self, attrib_name):
        if None not in (self.owner, self.name) and attrib_name != self.name:
            raise AttributeError('The {} parameter {!r} has already been '
                                 'assigned a name by the {} class, '
                                 'could not assign new name {!r}. Parameters '
                                 'may not be shared by multiple classes; '
                                 'ensure that you create a new parameter '
                                 'instance for each new class.'.format(type(self).__name__, self.name,
                                    self.owner.name, attrib_name))
        self.name = attrib_name

    def __getstate__(self):
        """
        All Parameters have slots, not a dict, so we have to support
        pickle and deepcopy ourselves.
        """
        return {slot: getattr(self, slot) for slot in self.__class__._all_slots_}

    def __setstate__(self,state):
        # set values of __slots__ (instead of in non-existent __dict__)
        for k, v in state.items():
            setattr(self, k, v)


# Define one particular type of Parameter that is used in this file
class String(Parameter):
    r"""
    A String Parameter with optional regular expression (regex) validation.

    The ``String`` class extends the :class:`Parameter` class to specifically handle
    string values and provides additional support for validating values
    against a regular expression.

    Parameters
    ----------
    default : str, optional
        The default value of the parameter. Default is an empty string (``""``).
    regex : str or None, optional
        A regular expression used to validate the string value. If ``None``, no
        regex validation is applied. Default is ``None``.

    Examples
    --------
    Define a ``String`` parameter with regex validation:

    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     user_name = param.String(default="John Doe", regex=r"^[A-Za-z ]+$", doc="Name of a person.")
    >>> instance = MyClass()

    Access the default value:

    >>> instance.user_name
    'John Doe'

    Set a valid value:

    >>> instance.user_name = "Jane Smith"
    >>> instance.user_name
    'Jane Smith'

    Attempt to set an invalid value (non-alphabetic characters):

    >>> instance.user_name = "Jane123"
    ...
    ValueError: String parameter 'MyClass.user_name' value 'Jane123' does not match regex '^[A-Za-z ]+$'.
    """

    __slots__ = ['regex']

    _slot_defaults = dict(Parameter._slot_defaults, default="", regex=None)

    @typing.overload
    def __init__(
        self,
        default="", *, regex=None,
        doc=None, label=None, precedence=None, instantiate=False, constant=False,
        readonly=False, pickle_default_value=True, allow_None=False, per_instance=True,
        allow_refs=False, nested_refs=False, default_factory=None, metadata=None,
    ):
        ...

    def __init__(self, default=Undefined, *, regex=Undefined, **kwargs):
        super().__init__(default=default, **kwargs)
        self.regex = regex
        self._validate(self.default)

    def _validate_regex(self, val, regex):
        if (val is None and self.allow_None):
            return
        if regex is not None and re.match(regex, val) is None:
            raise ValueError(
                f'{_validate_error_prefix(self)} value {val!r} does not '
                f'match regex {regex!r}.'
            )

    def _validate_value(self, val, allow_None):
        if allow_None and val is None:
            return
        if not isinstance(val, str):
            raise ValueError(
                f'{_validate_error_prefix(self)} only takes a string value, '
                f'not value of {type(val)}.'
            )

    def _validate(self, val):
        self._validate_value(val, self.allow_None)
        self._validate_regex(val, self.regex)


class shared_parameters:
    """
    Context manager to share parameter instances when creating
    multiple Parameterized objects of the same type. Parameter default
    values are instantiated once and cached to be reused when another
    Parameterized object of the same type is instantiated.
    Can be useful to easily modify large collections of Parameterized
    objects at once and can provide a significant speedup.

    References
    ----------
    See https://param.holoviz.org/user_guide/Parameters.html#instantiating-with-shared-parameters
    """

    _share = False
    _shared_cache = {}

    def __enter__(self):
        shared_parameters._share = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        shared_parameters._share = False
        shared_parameters._shared_cache = {}


def as_uninitialized(fn):
    """
    Decorate call fn with the parameterized_instance's
    initialization flag set to False, then revert the flag.

    (Used to decorate Parameterized methods that must alter
    a constant Parameter.)
    """
    @wraps(fn)
    def override_initialization(self_,*args,**kw):
        parameterized_instance = self_.self
        original_initialized = parameterized_instance._param__private.initialized
        parameterized_instance._param__private.initialized = False
        ret = fn(self_, *args, **kw)
        parameterized_instance._param__private.initialized = original_initialized
        return ret
    return override_initialization


class Comparator:
    """
    Comparator defines methods for determining whether two objects
    should be considered equal. It works by registering custom
    comparison functions, which may either be registed by type or with
    a predicate function. If no matching comparison can be found for
    the two objects the comparison will return False.

    If registered by type the Comparator will check whether both
    objects are of that type and apply the comparison. If the equality
    function is instead registered with a function it will call the
    function with each object individually to check if the comparison
    applies. This is useful for defining comparisons for objects
    without explicitly importing them.

    To use the Comparator simply call the is_equal function.
    """

    equalities = {
        numbers.Number: operator.eq,
        str: operator.eq,
        bytes: operator.eq,
        type(None): operator.eq,
        lambda o: hasattr(o, '_infinitely_iterable'): operator.eq,  # Time
    }
    gen_equalities = {
        _dt_types: operator.eq
    }

    @classmethod
    def is_equal(cls, obj1, obj2):
        equals = cls.equalities.copy()
        for gen, op in cls.gen_equalities.items():
            for t in gen():
                equals[t] = op

        for eq_type, eq in equals.items():
            try:
                are_instances = isinstance(obj1, eq_type) and isinstance(obj2, eq_type)
            except TypeError:
                pass
            else:
                if are_instances:
                    return eq(obj1, obj2)
            if isinstance(eq_type, FunctionType) and eq_type(obj1) and eq_type(obj2):
                return eq(obj1, obj2)
        if isinstance(obj2, (list, set, tuple)):
            return cls.compare_iterator(obj1, obj2)
        elif isinstance(obj2, dict):
            return cls.compare_mapping(obj1, obj2)
        return False

    @classmethod
    def compare_iterator(cls, obj1, obj2):
        if type(obj1) is not type(obj2) or len(obj1) != len(obj2):
            return False
        for o1, o2 in zip(obj1, obj2):
            if not cls.is_equal(o1, o2):
                return False
        return True

    @classmethod
    def compare_mapping(cls, obj1, obj2):
        if type(obj1) is not type(obj2) or len(obj1) != len(obj2): return False
        for k in obj1:
            if k in obj2:
                if not cls.is_equal(obj1[k], obj2[k]):
                    return False
            else:
                return False
        return True


class _ParametersRestorer:
    """Context-manager to handle the reset of parameter values after an update."""

    def __init__(self, *, parameters, restore, refs=None):
        self._parameters = parameters
        self._restore = restore
        self._refs = {} if refs is None else refs

    def __enter__(self):
        return self._restore

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            self._parameters._update(dict(self._restore, **self._refs))
        finally:
            self._restore = {}


class Parameters:
    """
    Object that holds the ``.param`` namespace and implementation of
    :class:`Parameterized` methods as well as any state that is not in
    ``__slots__`` or the Parameters themselves.

    Exists at both the metaclass level (instantiated by the metaclass)
    and at the instance level. Can contain state specific to either the
    class or the instance as necessary.

    References
    ----------
    https://param.holoviz.org/user_guide/Parameters.html#parameterized-namespace
    """

    def __init__(self_, cls: type['Parameterized'], self: Union['Parameterized', None]=None):
        """
        `cls` is the Parameterized class which is always set.
        self is the instance if set.
        """
        self_.cls = cls
        self_.self = self

    @property
    def _BATCH_WATCH(self_):
        return self_.self_or_cls._param__private.parameters_state['BATCH_WATCH']

    @_BATCH_WATCH.setter
    def _BATCH_WATCH(self_, value):
        self_.self_or_cls._param__private.parameters_state['BATCH_WATCH'] = value

    @property
    def _TRIGGER(self_):
        return self_.self_or_cls._param__private.parameters_state['TRIGGER']

    @_TRIGGER.setter
    def _TRIGGER(self_, value):
        self_.self_or_cls._param__private.parameters_state['TRIGGER'] = value

    @property
    def _events(self_):
        return self_.self_or_cls._param__private.parameters_state['events']

    @_events.setter
    def _events(self_, value):
        self_.self_or_cls._param__private.parameters_state['events'] = value

    @property
    def _state_watchers(self_):
        return self_.self_or_cls._param__private.parameters_state['watchers']

    @_state_watchers.setter
    def _state_watchers(self_, value):
        self_.self_or_cls._param__private.parameters_state['watchers'] = value

    @property
    def watchers(self_):
        """Dictionary of instance watchers."""
        if self_.self is None:
            raise TypeError('Accessing `.param.watchers` is only supported on a Parameterized instance, not class.')
        return self_.self._param__private.watchers

    @watchers.setter
    def watchers(self_, value):
        if self_.self is None:
            raise TypeError('Setting `.param.watchers` is only supported on a Parameterized instance, not class.')
        self_.self._param__private.watchers = value

    @property
    def self_or_cls(self_) -> Union['Parameterized', type['Parameterized']]:
        return self_.cls if self_.self is None else self_.self

    def __setstate__(self, state):
        # Set old parameters state on Parameterized.parameters_state
        self_, cls = state.get('self'), state.get('cls')
        self_or_cls = self_ if self_ is not None else cls
        for k in self_or_cls._param__private.parameters_state:
            key = '_'+k
            if key in state:
                self_or_cls._param__private.parameters_state[k] = state.pop(key)
        for k, v in state.items():
            setattr(self, k, v)

    def __getitem__(self_, key: str) -> Parameter:
        """
        Retrieve a :class:`Parameter` by its key.

        This method allows access to a class or instance :class:`Parameter` using its name.

        Parameters
        ----------
        key : str
            The name of the :class:`Parameter` to retrieve.

        Returns
        -------
        Parameter
            The :class:`Parameter` associated with the given key. If accessed on an instance,
            the method returns the instantiated (copied) parameter.
        """
        inst = self_.self
        if inst is None:
            return self_._cls_parameters[key]
        p = self_.objects(instance=False)[key]
        return _instantiated_parameter(inst, p)

    def __dir__(self_):
        """Return the list of standard attributes and parameters.

        Returns
        -------
        list[str]:
            A combined list of standard attributes and parameter names.
        """
        return super().__dir__() + list(self_._cls_parameters)

    def __iter__(self_):
        """Iterate over the parameters on this object."""
        yield from self_._cls_parameters

    def __contains__(self_, param):
        return param in self_._cls_parameters

    def __getattr__(self_, attr):
        """Handle attribute access for :class:`Parameter` objects.

        This method extends standard attribute access to support Parameters
        defined in the object. If the requested attribute corresponds to a
        Parameter, it retrieves the Parameter value.

        Parameters
        ----------
        attr : str
            The name of the attribute to access.

        Returns
        -------
        The value of the Parameter if it exists.

        Raises
        ------
        AttributeError
            If the class is not initialized or the attribute does not exist in
            the parameter objects.
        """
        cls = self_.__dict__.get('cls')
        if cls is None: # Class not initialized
            raise AttributeError

        if attr in self_._cls_parameters:
            return self_.__getitem__(attr)
        elif self_.self is None:
            raise AttributeError(f"type object '{self_.cls.__name__}.param' has no attribute {attr!r}")
        else:
            raise AttributeError(f"'{self_.cls.__name__}.param' object has no attribute {attr!r}")

    @as_uninitialized
    def _set_name(self_, name):
        self_.self.name = name

    @as_uninitialized
    def _generate_name(self_):
        self_._set_name('%s%05d' % (self_.cls.__name__, object_count))

    @as_uninitialized
    def _setup_params(self_, **params):
        """
        Initialize default and keyword parameter values.

        First, ensures that values for all Parameters with 'instantiate=True'
        (typically used for mutable Parameters) are copied directly into each object,
        to ensure that there is an independent copy of the value (to avoid surprising
        aliasing errors). Second, ensures that Parameters with 'constant=True' are
        referenced on the instance, to make sure that setting a constant
        Parameter on the class doesn't affect already created instances. Then
        sets each of the keyword arguments, raising when any of them are not
        defined as parameters.
        """
        self = self_.self
        ## Deepcopy all 'instantiate=True' parameters
        params_to_deepcopy = {}
        params_to_ref = {}
        objects = self_._cls_parameters
        for pname, p in objects.items():
            if pname == 'name' or p.default_factory:
                continue
            if p.instantiate:
                params_to_deepcopy[pname] = p
            elif p.constant:
                params_to_ref[pname] = p

        for p in params_to_deepcopy.values():
            self_._instantiate_param(p)
        for p in params_to_ref.values():
            self_._instantiate_param(p, deepcopy=False)

        ## keyword arg setting
        deps, refs = {}, {}
        for name, val in params.items():
            desc = self_.cls.get_param_descriptor(name)[0] # pylint: disable-msg=E1101
            if not desc:
                raise TypeError(
                    f"{self.__class__.__name__}.__init__() got an unexpected "
                    f"keyword argument {name!r}"
                )

            pobj = objects.get(name)
            if pobj is None or not pobj.allow_refs:
                # Until Parameter.allow_refs=True by default we have to
                # speculatively evaluate a values to check whether they
                # contain a reference and warn the user that the
                # behavior may change in future.
                if name not in self_.cls._param__private.explicit_no_refs:
                    try:
                        ref, _, resolved, _ = self_._resolve_ref(pobj, val)
                    except Exception:
                        ref = None
                    if ref:
                        warnings.warn(
                            f"Parameter {name!r} on {pobj.owner} is being given a valid parameter "
                            f"reference {val} but is implicitly allow_refs=False. "
                            "In future allow_refs will be enabled by default and "
                            f"the reference {val} will be resolved to its underlying "
                            f"value {resolved}. Please explicitly set allow_ref on the "
                            "Parameter definition to declare whether references "
                            "should be resolved or not.",
                            category=_ParamFutureWarning,
                            stacklevel=_find_stack_level(),
                        )
                setattr(self, name, val)
                continue

            # Resolve references
            ref, ref_deps, resolved, is_async = self_._resolve_ref(pobj, val)
            if ref is not None:
                refs[name] = ref
                deps[name] = ref_deps
            if not is_async and not (resolved is Undefined or resolved is Skip):
                setattr(self, name, resolved)
        return refs, deps

    def _setup_refs(self_, refs):
        groups = defaultdict(list)
        for pname, subrefs in refs.items():
            for p in subrefs:

                if isinstance(p, Parameter):
                    groups[p.owner].append((pname, p.name))
                else:
                    for sp in extract_dependencies(p):
                        groups[sp.owner].append((pname, sp.name))
        for owner, pnames in groups.items():
            refnames, pnames = zip(*pnames)
            self_.self._param__private.ref_watchers.append((
                refnames,
                owner.param._watch(self_._sync_refs, list(set(pnames)), precedence=-1)
            ))

    def _update_ref(self_, name, ref):
        param_private = self_.self._param__private
        if name in param_private.async_refs:
            param_private.async_refs.pop(name).cancel()
        for _, watcher in param_private.ref_watchers:
            dep_obj = watcher.cls if watcher.inst is None else watcher.inst
            dep_obj.param.unwatch(watcher)
        self_.self._param__private.ref_watchers = []
        refs = dict(self_.self._param__private.refs, **{name: ref})
        deps = {name: resolve_ref(ref, self_[name].nested_refs) for name, ref in refs.items()}
        self_._setup_refs(deps)
        self_.self._param__private.refs = refs

    def _sync_refs(self_, *events):
        updates = {}
        for pname, ref in self_.self._param__private.refs.items():
            # Skip updating value if dependency has not changed
            recursive = self_[pname].nested_refs
            deps = resolve_ref(ref, recursive)
            is_gen = inspect.isgeneratorfunction(ref)
            is_async = iscoroutinefunction(ref) or is_gen
            if not any((dep.owner is e.obj and dep.name == e.name) for dep in deps for e in events) and not is_async:
                continue

            try:
                new_val = resolve_value(ref, recursive)
            except Skip:
                new_val = Undefined
            if new_val is Skip or new_val is Undefined:
                continue
            elif is_async:
                async_executor(partial(self_._async_ref, pname, new_val))
                continue

            updates[pname] = new_val

        with edit_constant(self_.self):
            with _syncing(self_.self, updates):
                self_.update(updates)

    def _resolve_ref(self_, pobj, value):
        is_gen = inspect.isgeneratorfunction(value)
        is_async = iscoroutinefunction(value) or is_gen
        deps = resolve_ref(value, recursive=pobj.nested_refs)
        if not (deps or is_async or is_gen):
            return None, None, value, False
        ref = value
        try:
            value = resolve_value(value, recursive=pobj.nested_refs)
        except Skip:
            value = Undefined
        if is_async:
            async_executor(partial(self_._async_ref, pobj.name, value))
            value = None
        return ref, deps, value, is_async

    async def _async_ref(self_, pname, awaitable):
        if not self_.self._param__private.initialized:
            async_executor(partial(self_._async_ref, pname, awaitable))
            return

        import asyncio
        current_task = asyncio.current_task()
        running_task = self_.self._param__private.async_refs.get(pname)
        if running_task is None:
            self_.self._param__private.async_refs[pname] = current_task
        elif current_task is not running_task:
            self_.self._param__private.async_refs[pname].cancel()
        try:
            if isinstance(awaitable, types.AsyncGeneratorType):
                async for new_obj in awaitable:
                    with _syncing(self_.self, (pname,)):
                        self_.update({pname: new_obj})
            else:
                with _syncing(self_.self, (pname,)):
                    try:
                        self_.update({pname: await awaitable})
                    except Skip:
                        pass
        finally:
            # Ensure we clean up but only if the task matches the currrent task
            if self_.self._param__private.async_refs.get(pname) is current_task:
                del self_.self._param__private.async_refs[pname]

    @classmethod
    def _changed(cls, event):
        """
        Predicate that determines whether a Event object has actually
        changed such that old != new.
        """
        return not Comparator.is_equal(event.old, event.new)

    def _instantiate_param(self_, param_obj, dict_=None, key=None, deepcopy=True):
        # deepcopy or store a reference to reference param_obj.default into
        # self._param__private.values (or dict_ if supplied) under the
        # parameter's name (or key if supplied)
        instantiator = copy.deepcopy if deepcopy else lambda o: o
        self = self_.self
        dict_ = dict_ or self._param__private.values
        key = key or param_obj.name
        if shared_parameters._share:
            param_key = (str(type(self)), param_obj.name)
            if param_key in shared_parameters._shared_cache:
                new_object = shared_parameters._shared_cache[param_key]
            else:
                new_object = instantiator(param_obj.default)
                shared_parameters._shared_cache[param_key] = new_object
        else:
            new_object = instantiator(param_obj.default)

        dict_[key] = new_object

        if isinstance(new_object, Parameterized) and deepcopy:
            global object_count
            object_count += 1
            # Writes over name given to the original object;
            # could instead have kept the same name
            new_object.param._generate_name()

    def _update_deps(self_, attribute=None, init=False):
        obj = self_.self
        init_methods = []
        for method, queued, on_init, constant, dynamic in type(obj).param._depends['watch']:
            # On initialization set up constant watchers; otherwise
            # clean up previous dynamic watchers for the updated attribute
            dynamic = [d for d in dynamic if attribute is None or d.spec.split(".")[0] == attribute]
            if init:
                constant_grouped = defaultdict(list)
                for dep in _resolve_mcs_deps(obj, constant, []):
                    constant_grouped[(id(dep.inst), id(dep.cls), dep.what)].append((None, dep))
                for group in constant_grouped.values():
                    self_._watch_group(obj, method, queued, group)
                m = getattr(self_.self, method)
                if on_init and m not in init_methods:
                    init_methods.append(m)
            elif dynamic:
                for w in obj._param__private.dynamic_watchers.pop(method, []):
                    (w.cls if w.inst is None else w.inst).param.unwatch(w)
            else:
                continue

            # Resolve dynamic dependencies one-by-one to be able to trace their watchers
            grouped = defaultdict(list)
            for ddep in dynamic:
                for dep in _resolve_mcs_deps(obj, [], [ddep]):
                    grouped[(id(dep.inst), id(dep.cls), dep.what)].append((ddep, dep))

            for group in grouped.values():
                watcher = self_._watch_group(obj, method, queued, group, attribute)
                obj._param__private.dynamic_watchers[method].append(watcher)
        for m in init_methods:
            if iscoroutinefunction(m):
                async_executor(m)
            else:
                m()

    def _resolve_dynamic_deps(self, obj, dynamic_dep, param_dep, attribute):
        """
        If a subobject whose parameters are being depended on changes
        we should only trigger events if the actual parameter values
        of the new object differ from those on the old subobject,
        therefore we accumulate parameters to compare on a subobject
        change event.

        Additionally we need to make sure to notify the parent object
        if a subobject changes so the dependencies can be
        reinitialized so we return a callback which updates the
        dependencies.
        """
        subobj = obj
        subobjs = [obj]
        for subpath in dynamic_dep.spec.split('.')[:-1]:
            subobj = getattr(subobj, subpath.split(':')[0], None)
            subobjs.append(subobj)

        dep_obj = param_dep.cls if param_dep.inst is None else param_dep.inst
        if dep_obj not in subobjs[:-1]:
            return None, None, param_dep.what

        depth = subobjs.index(dep_obj)
        callback = None
        if depth > 0:
            def callback(*events):
                """
                If a subobject changes, we need to notify the main
                object to update the dependencies.
                """
                obj.param._update_deps(attribute)

        p = '.'.join(dynamic_dep.spec.split(':')[0].split('.')[depth+1:])
        if p == 'param':
            subparams = [sp for sp in list(subobjs[-1].param)]
        else:
            subparams = [p]

        if ':' in dynamic_dep.spec:
            what = dynamic_dep.spec.split(':')[-1]
        else:
            what = param_dep.what

        return subparams, callback, what

    def _watch_group(self_, obj, name, queued, group, attribute=None):
        """
        Set up a watcher for a group of dependencies.

        Ensures that
        if the dependency was dynamically generated we check whether
        a subobject change event actually causes a value change and
        that we update the existing watchers, i.e. clean up watchers
        on the old subobject and create watchers on the new subobject.
        """
        dynamic_dep, param_dep = group[0]
        dep_obj = param_dep.cls if param_dep.inst is None else param_dep.inst
        params = []
        for _, g in group:
            if g.name not in params:
                params.append(g.name)

        if dynamic_dep is None:
            subparams, callback, what = None, None, param_dep.what
        else:
            subparams, callback, what = self_._resolve_dynamic_deps(
                obj, dynamic_dep, param_dep, attribute)

        mcaller = _m_caller(obj, name, what, subparams, callback)
        return dep_obj.param._watch(
            mcaller, params, param_dep.what, queued=queued, precedence=-1)

    @_recursive_repr()
    def _repr_html_(self_, open=True):
        return _parameterized_repr_html(self_.self_or_cls, open)

    # Classmethods

    def add_parameter(self_, param_name: str, param_obj: Parameter):
        """
        Add a new :class:`Parameter` object to this class.

        This method allows dynamically adding a Parameter to the class, resulting
        in behavior equivalent to declaring the Parameter in the class's source code.

        Parameters
        ----------
        param_name : str
            The name of the parameter to add.
        param_obj : Parameter
            The Parameter object to add.

        Examples
        --------
        Create a :class:`Parameterized` class:

        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number()
        ...     b = param.String()
        >>> p = P()

        Add a new parameter to the class ``P`` via the class namespace ``P.param``:

        >>> P.param.add_parameter('c', param.Tuple(default=(1, 2, 3)))
        >>> print(p.c)
        (1, 2, 3)

        Add a new parameter to the class ``P`` via the instance namespace ``p.param``:

        >>> p.param.add_parameter('d', param.Tuple(default=(3, 2, 1)))
        >>> p.d
        (3, 2, 1)

        """
        # Could have just done setattr(cls,param_name,param_obj),
        # which is supported by the metaclass's __setattr__ , but
        # would need to handle the params() cache as well
        # (which is tricky but important for startup speed).
        cls = self_.cls
        type.__setattr__(cls, param_name, param_obj)
        ParameterizedMetaclass._initialize_parameter(cls, param_name, param_obj)
        # delete cached params()
        cls._param__private.params.clear()

    # Bothmethods

    def update(self_, arg=Undefined, /, **kwargs):
        """
        Update multiple parameters of this object or class before triggering events.

        Allows setting the parameters of the object or class using a
        dictionary, an iterable, or keyword arguments in the form of
        ``param=value``. The specified parameters will be updated to
        the given values.

        This method can also be used as a context manager to temporarily set
        and then reset parameter values.

        Parameters
        ----------
        **kwargs : dict or iterable or keyword arguments
            The parameters to update, provided as a dictionary, iterable, or
            keyword arguments in ``param=value`` format.

        References
        ----------
        https://param.holoviz.org/user_guide/Parameters.html#other-parameterized-methods

        Examples
        --------
        Create a :class:`Parameterized` class:

        >>> import param
        >>> class P(param.Parameterized):
        ...    a = param.String()
        ...    b = param.String()

        Define the instance:

        >>> p = P(a="0. Hello", b="0. World")

        Use :meth:`update` to update the parameters:

        >>> p.param.update(a="1. Hello", b="2. World")
        >>> p.a, p.b
        ('1. Hello', '1. World')

        Update the parameters temporarily:

        >>> with p.param.update(a="2. Hello", b="2. World"):
        ...     print(p.a, p.b)
        2. Hello 2. World

        >>> p.a, p.b
        ('1. Hello', '1. World')

        Lets check that the events are triggered **after** all parameters have
        been updated:

        >>> @param.depends(p.param.a, watch=True)
        ... def print_a_b(a):
        ...     print(p.a, p.b)
        >>> my_param.param.update(a="3. Hello",b="3. World")
        3. Hello 3. World
        """
        refs = {}
        if self_.self is not None:
            private = self_.self._param__private
            params = list(kwargs if arg is Undefined else dict(arg, **kwargs))
            for pname in params:
                if pname in refs:
                    continue
                elif pname in private.refs:
                    refs[pname] = private.refs[pname]
                elif pname in private.async_refs:
                    refs[pname] = private.async_refs[pname]
        restore = dict(self_._update(arg, **kwargs))
        return _ParametersRestorer(parameters=self_, restore=restore, refs=refs)

    def _update(self_, arg=Undefined, /, **kwargs):
        BATCH_WATCH = self_._BATCH_WATCH
        self_._BATCH_WATCH = True
        self_or_cls = self_.self_or_cls
        if arg is not Undefined:
            kwargs = dict(arg, **kwargs)

        trigger_params = [
            k for k in kwargs
            if k in self_ and hasattr(self_[k], '_autotrigger_value')
        ]

        for tp in trigger_params:
            self_[tp]._mode = 'set'

        values = self_.values()
        restore = {k: values[k] for k, v in kwargs.items() if k in values}

        for (k, v) in kwargs.items():
            if k not in self_:
                self_._BATCH_WATCH = False
                raise ValueError(f"{k!r} is not a parameter of {self_.cls.__name__}")
            try:
                setattr(self_or_cls, k, v)
            except Exception:
                self_._BATCH_WATCH = False
                raise

        self_._BATCH_WATCH = BATCH_WATCH
        if not BATCH_WATCH:
            self_._batch_call_watchers()

        for tp in trigger_params:
            p = self_[tp]
            p._mode = 'reset'
            setattr(self_or_cls, tp, p._autotrigger_reset_value)
            p._mode = 'set-reset'
        return restore

    @property
    def _cls_parameters(self_):
        """
        Class parameters are cached because they are accessed often,
        and parameters are rarely added (and cannot be deleted).
        """
        cls = self_.cls
        pdict = cls._param__private.params
        if pdict:
            return pdict

        paramdict = {}
        for class_ in classlist(cls):
            for name, val in class_.__dict__.items():
                if isinstance(val, Parameter):
                    paramdict[name] = val

        # We only want the cache to be visible to the cls on which
        # params() is called, so we mangle the name ourselves at
        # runtime (if we were to mangle it now, it would be
        # _Parameterized.__params for all classes).
        # cls._param__private.params[f'_{cls.__name__}__params'] = paramdict
        cls._param__private.params = paramdict
        return paramdict

    def objects(self_, instance: Literal[True, False, 'existing']=True) -> dict[str, Parameter]:
        """
        Return the Parameters of this instance or class.

        This method provides access to :class:`Parameter` objects defined on
        a :class:`Parameterized` class or instance, depending on the
        value of the ``instance`` argument.

        Parameters
        ----------
        instance : bool or ``{'existing'}``, default=True
            - ``True``: Return instance-specific parameters, creating them if
              necessary. This requires the instance to be fully initialized.
            - ``False``: Return class-level parameters without creating
              instance-specific copies.
            - ``'existing'``: Returns a mix of instance parameters that already
              exist and class parameters, avoiding creation of new
              instance-specific parameters.

        Returns
        -------
        dict[str, Parameter]
            A dictionary mapping parameter names to their corresponding
            :class:`Parameter` objects.

        Examples
        --------
        Accessing *class-level* Parameters:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     param1 = param.Number(default=1)
        >>> MyClass.param.objects(instance=False)
        {'name': <param.parameterized.String at 0x...>}

        Accessing *instance-level* Parameters:

        >>> obj = MyClass()
        >>> obj.param.objects()
        {'name': <param.parameterized.String at 0x...>}
        """
        if self_.self is not None and not self_.self._param__private.initialized and instance is True:
            raise RuntimeError(
                'Looking up instance Parameter objects (`.param.objects()`) until '
                'the Parameterized instance has been fully initialized is not allowed. '
                'Ensure you have called `super().__init__(**params)` in your Parameterized '
                'constructor before trying to access instance Parameter objects, or '
                'looking up the class Parameter objects with `.param.objects(instance=False)` '
                'may be enough for your use case.',
            )

        pdict = self_._cls_parameters
        if instance and self_.self is not None:
            if instance == 'existing':
                if getattr(self_.self._param__private, 'initialized', False) and self_.self._param__private.params:
                    return dict(pdict, **self_.self._param__private.params)
                return pdict
            else:
                return {k: self_.self.param[k] for k in pdict}
        return pdict

    def trigger(self_, *param_names: str) -> None:
        """
        Trigger watchers for the given set of parameter names.

        This method invokes all watchers associated with the given parameter names,
        regardless of whether the parameter values have actually changed.

        Parameters
        ----------
        *param_names : str
            Names of the parameters to trigger. Each name must correspond to a
            parameter defined on this :class:`Parameterized` object.

        Notes
        -----
        As a special case, the value will actually be changed for a Parameter
        of type :class:`param.Event`, setting it to ``True`` so that it is
        clear which :class:`param.Event` parameter has been triggered.

        Examples
        --------
        This method is useful to trigger watchers of parameters whose value
        is a mutable container:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     values = param.List([1, 2])
        >>> obj = MyClass()
        >>> def callback(event):
        ...     print(f"Triggered {event.name} / {event.new}")
        >>> obj.param.watch(callback, 'values')
        >>> obj.values.append(3)
        >>> obj.param.trigger('values')
        Triggered values / [1, 2, 3]
        """
        if self_.self is not None and not self_.self._param__private.initialized:
            raise RuntimeError(
                'Triggering watchers on a partially initialized Parameterized instance '
                'is not allowed. Ensure you have called super().__init__(**params) in '
                'the Parameterized instance constructor before trying to set up a watcher.',
            )

        trigger_params = [p for p in self_
                          if hasattr(self_[p], '_autotrigger_value')]
        triggers = {p:self_[p]._autotrigger_value
                    for p in trigger_params if p in param_names}

        events = self_._events
        watchers = self_._state_watchers
        self_._events  = []
        self_._state_watchers = []
        param_values = self_.values()
        params = {name: param_values[name] for name in param_names}
        self_._TRIGGER = True
        self_.update(dict(params, **triggers))
        self_._TRIGGER = False
        self_._events += events
        self_._state_watchers += watchers

    def _update_event_type(self_, watcher, event, triggered):
        """Return an updated Event object with the type field set appropriately."""
        if triggered:
            event_type = 'triggered'
        else:
            event_type = 'changed' if watcher.onlychanged else 'set'
        return Event(what=event.what, name=event.name, obj=event.obj, cls=event.cls,
                     old=event.old, new=event.new, type=event_type)

    def _execute_watcher(self, watcher, events):
        if watcher.mode == 'args':
            args, kwargs = events, {}
        else:
            args, kwargs = (), {event.name: event.new for event in events}

        if iscoroutinefunction(watcher.fn):
            if async_executor is None:
                raise RuntimeError("Could not execute %s coroutine function. "
                                   "Please register a asynchronous executor on "
                                   "param.parameterized.async_executor, which "
                                   "schedules the function on an event loop." %
                                   watcher.fn)
            async_executor(partial(watcher.fn, *args, **kwargs))
        else:
            try:
                watcher.fn(*args, **kwargs)
            except Skip:
                pass

    def _call_watcher(self_, watcher, event):
        """Invoke the given watcher appropriately given an Event object."""
        if self_._TRIGGER:
            pass
        elif watcher.onlychanged and (not self_._changed(event)):
            return

        if self_._BATCH_WATCH:
            self_._events.append(event)
            if not any(watcher is w for w in self_._state_watchers):
                self_._state_watchers.append(watcher)
        else:
            event = self_._update_event_type(watcher, event, self_._TRIGGER)
            with _batch_call_watchers(self_.self_or_cls, enable=watcher.queued, run=False):
                self_._execute_watcher(watcher, (event,))

    def _batch_call_watchers(self_):
        """
        Batch call a set of watchers based on the parameter value
        settings in kwargs using the queued Event and watcher objects.
        """
        while self_._events:
            event_dict = OrderedDict([((event.name, event.what), event)
                                      for event in self_._events])
            watchers = self_._state_watchers[:]
            self_._events = []
            self_._state_watchers = []

            for watcher in sorted(watchers, key=lambda w: w.precedence):
                events = [self_._update_event_type(watcher, event_dict[(name, watcher.what)],
                                                   self_._TRIGGER)
                          for name in watcher.parameter_names
                          if (name, watcher.what) in event_dict]
                with _batch_call_watchers(self_.self_or_cls, enable=watcher.queued, run=False):
                    self_._execute_watcher(watcher, events)
    # Please update the docstring with better description and examples
    # I've (MarcSkovMadsen) not been able to understand this. Its probably because I lack context.
    # Its not mentioned in the documentation.
    # The pytests do not make sense to me.
    def set_dynamic_time_fn(self_,time_fn,sublistattr=None):
        """
        Set ``time_fn`` for all :class:`param.Dynamic` Parameters of this class or
        instance object that are currently being dynamically
        generated.

        Additionally, sets ``_Dynamic_time_fn=time_fn`` on this class or
        instance object, so that any future changes to Dynamic
        Parmeters can inherit ``time_fn`` (e.g. if a :class:`param.Number` is changed
        from a float to a number generator, the number generator will
        inherit ``time_fn``).

        If specified, sublistattr is the name of an attribute of this
        class or instance that contains an iterable collection of
        subobjects on which ``set_dynamic_time_fn`` should be called.  If
        the attribute sublistattr is present on any of the subobjects,
        ``set_dynamic_time_fn()`` will be called for those, too.
        """
        self_or_cls = self_.self_or_cls
        self_or_cls._Dynamic_time_fn = time_fn

        if isinstance(self_or_cls,type):
            a = (None,self_or_cls)
        else:
            a = (self_or_cls,)

        for n,p in self_or_cls.param.objects('existing').items():
            if hasattr(p, '_value_is_dynamic'):
                if p._value_is_dynamic(*a):
                    g = self_or_cls.param.get_value_generator(n)
                    g._Dynamic_time_fn = time_fn

        if sublistattr:
            try:
                sublist = getattr(self_or_cls,sublistattr)
            except AttributeError:
                sublist = []

            for obj in sublist:
                obj.param.set_dynamic_time_fn(time_fn,sublistattr)

    def serialize_parameters(self_, subset: Union[Iterable[str], None]=None, mode='json'):
        """
        Return the serialized parameters of the :class:`Parameterized` object.

        Parameters
        ----------
        subset : iterable of str, optional
            An iterable of parameter names to serialize. If ``None``, all
            parameters will be serialized. Default is ``None``.
        mode : str, optional
            The serialization format. By default, only ``'json'`` is supported.
            Default is ``'json'``.

        Returns
        -------
        Any
            The serialized value.

        Raises
        ------
        ValueError
            If the specified serialization mode is not supported.

        References
        ----------
        https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#serializing-with-json

        Examples
        --------
        Create a Parameterized instance and serialize its parameters:

        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number()
        ...     b = param.String()
        >>> p = P(a=1, b="hello")

        Serialize parameters:

        >>> serialized_data = p.param.serialize_parameters()
        >>> serialized_data
        '{"name": "P00002", "a": 1, "b": "hello"}'
        """
        if mode not in Parameter._serializers:
            raise ValueError(f'Mode {mode!r} not in available serialization formats {list(Parameter._serializers.keys())!r}')
        self_or_cls = self_.self_or_cls
        serializer = Parameter._serializers[mode]
        return serializer.serialize_parameters(self_or_cls, subset=subset)

    def serialize_value(self_, pname: str, mode: str='json'):
        """
        Serialize the value of a specific parameter.

        This method serializes the value of a given parameter on a Parameterized
        object using the specified serialization mode.

        Parameters
        ----------
        pname : str
            The name of the parameter whose value is to be serialized.
        mode : str, optional
            The serialization format to use. By default, only ``'json'`` is supported.
            Default is ``'json'``.

        Returns
        -------
        Any
            The serialized value of the specified parameter.

        Raises
        ------
        ValueError
            If the specified serialization mode is not supported.

        References
        ----------
        https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#serializing-with-json

        Examples
        --------
        Serialize the value of a specific parameter:

        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number()
        ...     b = param.String()
        >>> p = P(a=1, b="hello")

        Serialize the value of parameter 'a':

        >>> serialized_value = p.param.serialize_value('a')
        >>> serialized_value
        '1'
        """
        if mode not in Parameter._serializers:
            raise ValueError(f'Mode {mode!r} not in available serialization formats {list(Parameter._serializers.keys())!r}')
        self_or_cls = self_.self_or_cls
        serializer = Parameter._serializers[mode]
        return serializer.serialize_parameter_value(self_or_cls, pname)

    def deserialize_parameters(self_, serialization, subset: Union[Iterable[str], None]=None, mode: str='json') -> dict:
        """
        Deserialize the given serialized data. This data can be used to
        create a :class:`Parameterized` object or update the parameters
        of an existing :class:`Parameterized` object.

        Parameters
        ----------
        serialization : str
            The serialized parameter data.
        subset : iterable of str, optional
            An iterable of parameter names to deserialize. If ``None``, all
            parameters will be deserialized. Default is ``None``.
        mode : str, optional
            The serialization format. By default, only ``'json'`` is supported.
            Default is ``'json'``.

        Returns
        -------
        dict
            A dictionary with parameter names as keys and deserialized values.

        Raises
        ------
        ValueError
            If the specified serialization mode is not supported.

        References
        ----------
        https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#serializing-with-json

        Examples
        --------
        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number()
        ...     b = param.String()
        ...
        >>> serialized_data = '{"a": 1, "b": "hello"}'
        >>> deserialized_data = P.param.deserialize_parameters(serialized_data)
        >>> deserialized_data
        {'a': 1, 'b': 'hello'}
        >>> instance = P(**deserialized_data)
        >>> instance
        P(a=1, b='hello', name='P...')
        """
        if mode not in Parameter._serializers:
            raise ValueError(f'Mode {mode!r} not in available serialization formats {list(Parameter._serializers.keys())!r}')
        self_or_cls = self_.self_or_cls
        serializer = Parameter._serializers[mode]
        return serializer.deserialize_parameters(self_or_cls, serialization, subset=subset)

    def deserialize_value(self_, pname: str, value, mode: str='json'):
        """
        Deserialize the value of a specific parameter.

        This method deserializes a value for a given parameter on a
        :class:`Parameterized` object using the specified deserialization mode.

        Parameters
        ----------
        pname : str
            The name of the parameter whose value is to be deserialized.
        value : Any
            The serialized value to be deserialized.
        mode : str, optional
            The deserialization format to use. By default, only ``'json'`` is supported.
            Default is ``'json'``.

        Returns
        -------
        Any
            The deserialized value of the specified parameter.

        Raises
        ------
        ValueError
            If the specified deserialization mode is not supported.

        References
        ----------
        https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#deserializing-with-json

        Examples
        --------
        Deserialize the value of a specific parameter:

        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number()
        ...     b = param.String()
        >>> p = P(a=1, b="hello")

        Deserialize the value of parameter ``'a'``:

        >>> deserialized_value = p.param.deserialize_value('a', '10')
        >>> deserialized_value
        10
        """
        if mode not in Parameter._serializers:
            raise ValueError(f'Mode {mode!r} not in available serialization formats {list(Parameter._serializers.keys())!r}')
        self_or_cls = self_.self_or_cls
        serializer = Parameter._serializers[mode]
        return serializer.deserialize_parameter_value(self_or_cls, pname, value)

    def schema(self_, safe: bool=False, subset: Union[Iterable[str], None]=None, mode: str='json'):
        """
        Generate a schema for the parameters on this :class:`Parameterized` object.

        This method provides a schema representation of the parameters on a
        Parameterized object, including their metadata, using the specified
        serialization mode.

        Parameters
        ----------
        safe : bool, optional
            If ``True``, the schema will only include parameters marked as safe for
            serialization. Default is ``False``.
        subset : Iterable[str], optional
            An iterable of parameter names to include in the schema. If None, all
            parameters will be included. Default is ``None``.
        mode : str, optional
            The serialization format to use. By default, only ``'json'`` is supported.
            Default is ``'json'``.

        Returns
        -------
        dict
            A schema dictionary representing the parameters and their metadata.

        Raises
        ------
        ValueError
            If the specified serialization mode is not supported.

        References
        ----------
        https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#json-schemas

        Examples
        --------
        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number(default=1, bounds=(0, 10), doc="A numeric parameter")
        ...     b = param.String(default="hello", doc="A string parameter")
        >>> p = P()

        Generate the schema for all parameters:

        >>> schema = p.param.schema()
        >>> schema
        {'name': {'anyOf': [{'type': 'string'}, {'type': 'null'}],
         'description': "String identifier for this object. Default is the object's class name plus a unique integer",
         'title': 'Name'},
        'a': {'type': 'number',...
        }
        """
        if mode not in Parameter._serializers:
            raise ValueError(f'Mode {mode!r} not in available serialization formats {list(Parameter._serializers.keys())!r}')
        self_or_cls = self_.self_or_cls
        serializer = Parameter._serializers[mode]
        return serializer.schema(self_or_cls, safe=safe, subset=subset)

    def values(self_, onlychanged: bool = False) -> dict[str, Any]:
        """
        Retrieve a dictionary of parameter names and their current values.

        Parameters
        ----------
        onlychanged : bool, optional
            If ``True``, only parameters with values different from their defaults are
            included (applicable only to instances). Default is ``False``.

        Returns
        -------
        dict[str, Any]
            A dictionary containing parameter names as keys and their current values
            as values.

        Examples
        --------
        >>> import param
        >>> class P(param.Parameterized):
        ...     a = param.Number(default=0)
        ...     b = param.String(default="hello")
        >>> p = P(a=10)

        Get all parameter values:

        >>> p.param.values()
        {'a': 10, 'b': 'hello', 'name': 'P...'}

        Get only changed parameter values:

        >>> p.param.values(onlychanged=True)
        {'a': 10}
        """
        self_or_cls = self_.self_or_cls
        vals = []
        for name, val in self_or_cls.param.objects('existing').items():
            value = self_or_cls.param.get_value_generator(name)
            if name == 'name' and onlychanged and _is_auto_name(self_.cls.__name__, value):
                continue
            if not onlychanged or not Comparator.is_equal(value, val.default):
                vals.append((name, value))

        vals.sort(key=itemgetter(0))
        return dict(vals)

    # Please update the docstring with better description and examples
    # I've (MarcSkovMadsen) not been able to understand this. Its probably because I lack context.
    # Its not mentioned in the documentation or pytests
    def force_new_dynamic_value(self_, name): # pylint: disable-msg=E0213
        """
        Force a new value to be generated for the dynamic attribute
        name, and return it.

        If name is not dynamic, its current value is returned
        (i.e. equivalent to ``getattr(name)``).
        """
        cls_or_slf = self_.self_or_cls
        param_obj = cls_or_slf.param.objects('existing').get(name)

        if not param_obj:
            return getattr(cls_or_slf, name)

        cls, slf = None, None
        if isinstance(cls_or_slf,type):
            cls = cls_or_slf
        else:
            slf = cls_or_slf

        if not hasattr(param_obj,'_force'):
            return param_obj.__get__(slf, cls)
        else:
            return param_obj._force(slf, cls)

    def get_value_generator(self_,name: str) -> Any: # pylint: disable-msg=E0213
        """
        Retrieve the value or value-generating object of a named parameter.

        For most parameters, this is simply the parameter's value (i.e. the
        same as ``getattr()``), but :class:`param.Dynamic` parameters have their
        value-generating object returned.

        Parameters
        ----------
        name : str
            The name of the parameter whose value or value-generating object is
            to be retrieved.

        Returns
        -------
        Any
            The current value of the parameter, a value-generating object for
            :class:`param.Dynamic` parameters.

        Examples
        --------
        >>> import param
        >>> import numbergen
        >>> class MyClass(param.Parameterized):
        ...     x = param.String(default="Hello")
        ...     y = param.Dynamic(default=numbergen.UniformRandom(lbound=-1, ubound=1, seed=1))

        >>> instance = MyClass()

        Access the parameter value directly:

        >>> instance.y
        -0.7312715117751976
        >>> instance.y
        0.6948674738744653

        Retrieve the parameter's value or value-generating object:

        >>> instance.param.get_value_generator("y")
        <UniformRandom UniformRandom ...>
        """
        cls_or_slf = self_.self_or_cls
        param_obj = cls_or_slf.param.objects('existing').get(name)

        if not param_obj:
            value = getattr(cls_or_slf,name)

        # CompositeParameter detected by being a Parameter and having 'attribs'
        elif hasattr(param_obj,'attribs'):
            value = [cls_or_slf.param.get_value_generator(a) for a in param_obj.attribs]

        # not a Dynamic Parameter
        elif not hasattr(param_obj,'_value_is_dynamic'):
            value = getattr(cls_or_slf,name)

        # Dynamic Parameter...
        else:
            # TODO: is this always an instance?
            if isinstance(cls_or_slf, Parameterized) and name in cls_or_slf._param__private.values:
                # dealing with object and it's been set on this object
                value = cls_or_slf._param__private.values[name]
            elif not callable(param_obj.default):
                value = getattr(cls_or_slf, name)
            else:
                # dealing with class or isn't set on the object
                value = param_obj.default

        return value

    def inspect_value(self_,name: str) -> Any: # pylint: disable-msg=E0213
        """
        Inspect the current value of a parameter without modifying it.

        Parameters
        ----------
        name : str
            The name of the parameter whose value is to be inspected.

        Returns
        -------
        Any
            The current value of the parameter, the last generated value for
            :class:`param.Dynamic` parameters.

        Examples
        --------
        >>> import param
        >>> import numbergen
        >>> class MyClass(param.Parameterized):
        ...     x = param.String(default="Hello")
        ...     y = param.Dynamic(default=numbergen.UniformRandom(lbound=-1, ubound=1, seed=1), doc="nothing")

        >>> instance = MyClass()

        Access the parameter value directly:

        >>> instance.y
        -0.7312715117751976

        Inspect the parameter value without modifying it:

        >>> instance.param.inspect_value("y")
        -0.7312715117751976
        """
        cls_or_slf = self_.self_or_cls
        param_obj = cls_or_slf.param.objects('existing').get(name)

        if not param_obj:
            value = getattr(cls_or_slf,name)
        elif hasattr(param_obj,'attribs'):
            value = [cls_or_slf.param.inspect_value(a) for a in param_obj.attribs]
        elif not hasattr(param_obj,'_inspect'):
            value = getattr(cls_or_slf,name)
        else:
            if isinstance(cls_or_slf,type):
                value = param_obj._inspect(None,cls_or_slf)
            else:
                value = param_obj._inspect(cls_or_slf,None)

        return value

    def method_dependencies(self_, name: str, intermediate: bool = False) -> list[PInfo]:
        """
        Retrieve the parameter dependencies of a specified method.

        By default intermediate dependencies on sub-objects are not returned as
        these are primarily useful for internal use to determine when a
        sub-object dependency has to be updated.

        Parameters
        ----------
        name : str
            The name of the method whose dependencies are to be retrieved.
        intermediate : bool, optional
            If ``True``, includes intermediate dependencies on sub-objects. These are
            primarily useful for internal purposes. Default is ``False``.

        Returns
        -------
        list[PInfo]
            A list of :class:`PInfo` objects representing the dependencies of the specified
            method. Each :class:`PInfo` object contains information about the instance,
            parameter, and the type of dependency.

        Examples
        --------
        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Parameter()
        ...     b = param.Parameter()
        ...
        ...     @param.depends('a', 'b', watch=True)
        ...     def test(self):
        ...         pass

        Create an instance and inspect method dependencies:

        >>> instance = MyClass()
        >>> instance.param.method_dependencies('test')
        [PInfo(inst=MyClass(a=None, b=None, name='MyClass...]
        """
        method = getattr(self_.self_or_cls, name)
        minfo = MInfo(cls=self_.cls, inst=self_.self, name=name,
                      method=method)
        deps, dynamic = _params_depended_on(
            minfo, dynamic=False, intermediate=intermediate)
        if self_.self is None:
            return deps
        return _resolve_mcs_deps(
            self_.self, deps, dynamic, intermediate=intermediate)

    def outputs(self_) -> dict[str,tuple]:
        """
        Retrieve a mapping of declared outputs for the Parameterized object.

        Parameters are declared as outputs using the :meth`output` decorator.

        Returns
        -------
        dict
            A dictionary mapping output names to a tuple of:
            - Parameter type (:class:`Parameter`).
            - Bound method of the output.
            - Index into the output, or ``None`` if there is no specific index.

        Examples
        --------
        Declare a single output in a :class:`Parameterized` class:

        >>> import param
        >>> class P(param.Parameterized):
        ...     @param.output()
        ...     def single_output(self):
        ...         return 1

        Access the outputs:

        >>> p = P()
        >>> p.param.outputs()
        {'single_output': (<param.parameterized.Parameter at 0x...>,
          <bound method P.single_output of P(name='P...')>,
          None)}

        Declare multiple outputs:

        >>> class Q(param.Parameterized):
        ...     @param.output(('output1', param.Number), ('output2', param.String))
        ...     def multi_output(self):
        ...         return 42, "hello"

        Access the outputs:

        >>> q = Q()
        >>> q.param.outputs()
        {'output1': (<param.parameters.Number at 0x...>,
          <bound method Q.multi_output of Q(name='Q...')>,
          0),
         'output2': (<param.parameterized.String at 0x...>,
          <bound method Q.multi_output of Q(name='Q...')>,
          1)}
        """
        outputs = {}
        for cls in classlist(self_.cls):
            for name in dir(cls):
                method = getattr(self_.self_or_cls, name)
                dinfo = getattr(method, '_dinfo', {})
                if 'outputs' not in dinfo:
                    continue
                for override, otype, idx in dinfo['outputs']:
                    if override is not None:
                        name = override
                    outputs[name] = (otype, method, idx)
        return outputs

    def _spec_to_obj(self_, spec, dynamic=True, intermediate=True):
        """
        Resolve a dependency specification into lists of explicit
        parameter dependencies and dynamic dependencies.

        Dynamic dependencies are specifications to be resolved when
        the sub-object whose parameters are being depended on is
        defined.

        During class creation dynamic=False which means sub-object
        dependencies are not resolved. At instance creation and
        whenever a sub-object is set on an object this method will be
        invoked to determine whether the dependency is available.

        For sub-object dependencies we also return dependencies for
        every part of the path, e.g. for a dependency specification
        like "a.b.c" we return dependencies for sub-object "a" and the
        sub-sub-object "b" in addition to the dependency on the actual
        parameter "c" on object "b". This is to ensure that if a
        sub-object is swapped out we are notified and can update the
        dynamic dependency to the new object. Even if a sub-object
        dependency can only partially resolved, e.g. if object "a"
        does not yet have a sub-object "b" we must watch for changes
        to "b" on sub-object "a" in case such a subobject is put in "b".
        """
        if isinstance(spec, Parameter):
            inst = spec.owner if isinstance(spec.owner, Parameterized) else None
            cls = spec.owner if inst is None else type(inst)
            info = PInfo(inst=inst, cls=cls, name=spec.name,
                         pobj=spec, what='value')
            return [] if intermediate == 'only' else [info], []

        obj, attr, what = _parse_dependency_spec(spec)
        if obj is None:
            src = self_.self_or_cls
        elif not dynamic:
            return [], [DInfo(spec=spec)]
        else:
            if not hasattr(self_.self_or_cls, obj.split('.')[1]):
                raise AttributeError(
                    f'Dependency {obj[1:]!r} could not be resolved, {self_.self_or_cls} '
                    f'has no parameter or attribute {obj.split(".")[1]!r}. Ensure '
                    'the object being depended on is declared before calling the '
                    'Parameterized constructor.'
                )

            src = _getattrr(self_.self_or_cls, obj[1::], None)
            if src is None:
                path = obj[1:].split('.')
                deps = []
                # Attempt to partially resolve subobject path to ensure
                # that if a subobject is later updated making the full
                # subobject path available we have to be notified and
                # set up watchers
                if len(path) >= 1 and intermediate:
                    sub_src = None
                    subpath = path
                    while sub_src is None and subpath:
                        subpath = subpath[:-1]
                        sub_src = _getattrr(self_.self_or_cls, '.'.join(subpath), None)
                    if subpath:
                        subdeps, _ = self_._spec_to_obj(
                            '.'.join(path[:len(subpath)+1]), dynamic, intermediate)
                        deps += subdeps
                return deps, [] if intermediate == 'only' else [DInfo(spec=spec)]

        cls, inst = (src, None) if isinstance(src, type) else (type(src), src)
        if attr == 'param':
            deps, dynamic_deps = self_._spec_to_obj(obj[1:], dynamic, intermediate)
            for p in src.param:
                param_deps, param_dynamic_deps = src.param._spec_to_obj(p, dynamic, intermediate)
                deps += param_deps
                dynamic_deps += param_dynamic_deps
            return deps, dynamic_deps
        elif attr in src.param:
            info = PInfo(inst=inst, cls=cls, name=attr,
                         pobj=src.param[attr], what=what)
        elif hasattr(src, attr):
            attr_obj = getattr(src, attr)
            if isinstance(attr_obj, Parameterized):
                return [], []
            elif isinstance(attr_obj, (FunctionType, MethodType)):
                info = MInfo(inst=inst, cls=cls, name=attr,
                             method=attr_obj)
            else:
                raise AttributeError(f"Attribute {attr!r} could not be resolved on {src}.")
        elif getattr(src, "abstract", None):
            return [], [] if intermediate == 'only' else [DInfo(spec=spec)]
        else:
            raise AttributeError(f"Attribute {attr!r} could not be resolved on {src}.")

        if obj is None or not intermediate:
            return [info], []
        deps, dynamic_deps = self_._spec_to_obj(obj[1:], dynamic, intermediate)
        if intermediate != 'only':
            deps.append(info)
        return deps, dynamic_deps

    def _register_watcher(
        self_,
        action: Literal['append', 'remove'],
        watcher: Watcher, what: str = 'value',
    ):
        if self_.self is not None and not self_.self._param__private.initialized:
            raise RuntimeError(
                '(Un)registering a watcher on a partially initialized Parameterized instance '
                'is not allowed. Ensure you have called super().__init__(**) in the '
                'Parameterized instance constructor before trying to set up a watcher.',
            )

        parameter_names = watcher.parameter_names
        for parameter_name in parameter_names:
            if parameter_name not in self_.cls.param:
                raise ValueError("{} parameter was not found in list of "
                                 "parameters of class {}".format(parameter_name, self_.cls.__name__))

            if self_.self is not None and what == "value":
                watchers = self_.self._param__private.watchers
                if parameter_name not in watchers:
                    watchers[parameter_name] = {}
                if what not in watchers[parameter_name]:
                    watchers[parameter_name][what] = []
                method = getattr(watchers[parameter_name][what], action)
            else:
                watchers = self_[parameter_name].watchers
                if what not in watchers:
                    watchers[what] = []
                method = getattr(watchers[what], action)
            try:
                method(watcher)
            except ValueError:
                # ValueError raised when attempting to remove an already
                # removed watcher. Error swallowed as unwatch is idempotent.
                if action != 'remove':
                    raise

    def watch(
        self_,
        fn,
        parameter_names: Union[str, list[str]],
        what: str = 'value',
        onlychanged: bool = True,
        queued: bool = False,
        precedence: int = 0,
    ) -> Watcher:
        """
        Register a callback function to be invoked for parameter events.

        This method allows you to register a callback function (``fn``) that will
        be triggered when specified events occur on the indicated parameters. The
        behavior of the watcher can be customized using various options.

        Parameters
        ----------
        fn : callable
            The callback function to invoke when an event occurs. This function
            will be provided with :class:`Event` objects as positional arguments,
            allowing it to determine the triggering events.
        parameter_names : str or list[str]
            A parameter name or a list of parameter names to watch for events.
        what : str, optional
            The type of change to watch for. By default, this is ``'value'``,
            but it can be set to other parameter attributes such as ``'constant'``.
            Default is ``'value'``.
        onlychanged : bool, optional
            If ``True`` (default), the callback is only invoked when the watched
            item changes. If ``False``, the callback is invoked even when the ```what```
            item is set to its current value.
        queued : bool, optional
            By default (``False``), additional watcher events generated inside the
            callback fn are dispatched immediately, effectively doing depth-first
            processing of Watcher events. However, in certain scenarios, it is
            helpful to wait to dispatch such downstream events until all events
            that triggered this watcher have been processed. In such cases
            setting ``queued=True`` on this Watcher will queue up new downstream
            events generated during ``fn`` until ``fn`` completes and all other
            watchers invoked by that same event have finished executing),
            effectively doing breadth-first processing of Watcher events.
        precedence : int, optional
            The precedence level of the watcher. Lower precedence levels are
            executed earlier. User-defined watchers must use positive precedence
            values. Negative precedences are reserved for internal watchers
            (e.g., those set up by :func:`depends`). Default is ``0``.

        Returns
        -------
        Watcher
            The :class:`Watcher` object that encapsulates the registered callback.

        See Also
        --------
        Watcher : Contains detailed information about the watcher object.
        Event : Provides details about the triggering events.

        Examples
        --------
        Register two watchers for parameter changes, one directly in
        the constructor and one after the instance is created:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Number(default=1)
        ...     b = param.Number(default=2)
        ...
        ...     def __init__(self, **params):
        ...         super().__init__(**params)
        ...         self.param.watch(self.callback, ['a'])
        ...
        ...     def callback(self, event):
        ...         print(f"Event triggered by: {event.name}, new value: {event.new}")
        ...
        >>> instance = MyClass()

        Watch for changes to ``b``:

        >>> instance.param.watch(instance.callback, ['b'])

        Trigger a change to invoke the callback:

        >>> instance.a = 10
        Event triggered by: a, new value: 10
        >>> instance.b = 11
        Event triggered by: b, new value: 11
        """
        if precedence < 0:
            raise ValueError("User-defined watch callbacks must declare "
                             "a positive precedence. Negative precedences "
                             "are reserved for internal Watchers.")
        return self_._watch(fn, parameter_names, what, onlychanged, queued, precedence)

    def _watch(self_, fn, parameter_names, what='value', onlychanged=True, queued=False, precedence=-1):
        parameter_names = tuple(parameter_names) if isinstance(parameter_names, list) else (parameter_names,)
        watcher = Watcher(inst=self_.self, cls=self_.cls, fn=fn, mode='args',
                          onlychanged=onlychanged, parameter_names=parameter_names,
                          what=what, queued=queued, precedence=precedence)
        self_._register_watcher('append', watcher, what)
        return watcher

    def unwatch(self_, watcher: Watcher) -> None:
        """
        Remove a watcher from this object's list of registered watchers.

        This method unregisters a previously registered :class:`Watcher` object,
        effectively stopping it from being triggered by events on the associated
        parameters. Calling unwatch with an already unregistered watcher
        is a no-op.

        Parameters
        ----------
        watcher : Watcher
            The :class:`Watcher` object to remove. This should be an object returned
            by a previous call to :meth:`watch` or :meth:`watch_values`.

        See Also
        --------
        watch : Registers a new watcher to observe parameter changes.
        watch_values : Registers a watcher specifically for value changes.

        Examples
        --------
        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Number(default=1)
        ...
        ...     def callback(self, event):
        ...         print(f"Triggered by {event.name}")
        ...
        >>> instance = MyClass()

        Add a watcher:

        >>> watcher = instance.param.watch(instance.callback, ['a'])

        Trigger the watcher:

        >>> instance.a = 10
        Triggered by a

        Remove the watcher:

        >>> instance.param.unwatch(watcher)

        No callback is triggered after removing the watcher:

        >>> instance.a = 20  # No output

        Calling ``unwatch()`` again has no effect:

        >>> instance.param.unwatch(watcher)
        """
        self_._register_watcher('remove', watcher, what=watcher.what)

    def watch_values(
        self_,
        fn: Callable,
        parameter_names: Union[str, list[str]],
        what: Literal["value"] = 'value',
        onlychanged: bool = True,
        queued: bool = False,
        precedence: int = 0
    ) -> Watcher:
        """
        Register a callback function for changes in parameter values.

        This method is a simplified version of :meth:`watch`, specifically designed for
        monitoring changes in parameter values. Unlike :meth:`watch`, the callback is
        invoked with keyword arguments (``<param_name>=<new_value>``) instead of
        :class:`Event` objects.

        Parameters
        ----------
        fn : Callable
            The callback function to invoke when a parameter value changes. The
            function is called with keyword arguments where the parameter names
            are keys, and their new values are values.
        parameter_names : str or list of str
            The name(s) of the parameters to monitor. Can be a single parameter
            name, a list of parameter names, or a tuple of parameter names.
        what : str, optional
            The type of change to watch for. Must be ``'value'``. Default is ``'value'``.

            .. deprecated:: 2.3.0
        onlychanged : bool, optional
            If ``True`` (default), the callback is only invoked when the parameter value
            changes. If ``False``, the callback is invoked even when the parameter is
            set to its current value.
        queued : bool, optional
            By default (``False``), additional watcher events generated inside the
            callback fn are dispatched immediately, effectively doing depth-first
            processing of Watcher events. However, in certain scenarios, it is
            helpful to wait to dispatch such downstream events until all events
            that triggered this watcher have been processed. In such cases
            setting ``queued=True`` on this Watcher will queue up new downstream
            events generated during ``fn`` until ``fn`` completes and all other
            watchers invoked by that same event have finished executing),
            effectively doing breadth-first processing of Watcher events.
        precedence : int, optional
            The precedence level of the watcher. Lower precedence values are executed
            earlier. User-defined watchers must use positive precedence values.
            Default is ``0``.

        Returns
        -------
        Watcher
            The :class:`Watcher` object encapsulating the registered callback.

        Notes
        -----
        - This method is a convenient shorthand for :meth:`watch` when only monitoring
          changes in parameter values is needed.
        - Callback functions receive new values as keyword arguments, making it easier
          to work with parameter updates.

        See Also
        --------
        watch : General-purpose watcher registration supporting a broader range of events.

        Examples
        --------
        Monitor parameter value changes:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Number(default=1)
        ...     b = param.Number(default=2)
        ...
        ...     def callback(self, a=None, b=None):
        ...         print(f"Callback triggered with a={a}, b={b}")
        ...
        >>> instance = MyClass()

        Register a watcher:

        >>> instance.param.watch_values(instance.callback, ['a', 'b'])
        Watcher(inst=MyClass(a=1, b=2, name=...)

        Trigger changes to invoke the callback:

        >>> instance.a = 10
        Callback triggered with a=10, b=None
        >>> instance.b = 20
        Callback triggered with a=None, b=20
        """
        if precedence < 0:
            raise ValueError("User-defined watch callbacks must declare "
                             "a positive precedence. Negative precedences "
                             "are reserved for internal Watchers.")
        if what != 'value':
            warnings.warn(
                'The keyword "what" is deprecated and will be removed in a '
                'future version.',
                category=_ParamFutureWarning,
                stacklevel=_find_stack_level(),
            )
        assert what == 'value'
        if isinstance(parameter_names, list):
            parameter_names = tuple(parameter_names)
        else:
            parameter_names = (parameter_names,)
        watcher = Watcher(inst=self_.self, cls=self_.cls, fn=fn,
                          mode='kwargs', onlychanged=onlychanged,
                          parameter_names=parameter_names, what=what,
                          queued=queued, precedence=precedence)
        self_._register_watcher('append', watcher, what)
        return watcher

    # Instance methods

    # Designed to avoid any processing unless the print
    # level is high enough, though not all callers of message(),
    # verbose(), debug(), etc are taking advantage of this.
    def __db_print(self_,level,msg,*args,**kw):
        """
        Call the logger returned by the get_logger() function,
        prepending the result of calling dbprint_prefix() (if any).

        See python's logging module for details.
        """
        self_or_cls = self_.self_or_cls
        if get_logger(name=self_or_cls.name).isEnabledFor(level):

            if dbprint_prefix and callable(dbprint_prefix):
                msg = dbprint_prefix() + ": " + msg  # pylint: disable-msg=E1102

            get_logger(name=self_or_cls.name).log(level, msg, *args, **kw)

    def warning(self_, msg,*args,**kw):
        """
        Print msg merged with args as a warning, unless module variable
        warnings_as_exceptions is True, then raise an Exception
        containing the arguments.

        See Python's logging module for details of message formatting.
        """
        self_.log(WARNING, msg, *args, **kw)

    def log(self_, level: int, msg: str, *args, **kw) -> None:
        """
        Log a message at the specified logging level.

        This method logs a message constructed by merging ``msg`` with ``args`` at
        the indicated logging level. It supports logging levels defined in
        Python's ``logging`` module plus VERBOSE, either obtained directly from
        the logging module like ``logging.INFO``, or from parameterized like
        ``param.parameterized.INFO``.

        Supported logging levels include (in order of severity):
        DEBUG, VERBOSE, INFO, WARNING, ERROR, CRITICAL

        Parameters
        ----------
        level : int
            The logging level at which the message should be logged e.g., ``logging.INFO`` or
            ``param.INFO``.
        msg : str
            The message to log. This message can include format specifiers,
            which will be replaced with values from ``args``.
        *args : tuple
            Arguments to merge into `msg` using the format specifiers.
        **kw : dict
            Additional keyword arguments passed to the logging implementation.

        Raises
        ------
        Exception
            If the logging level is ``WARNING`` and warnings are treated as
            exceptions (:data:`warnings_as_exceptions` is True).

        Examples
        --------
        Log a message at the ``INFO`` level:

        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     def log_message(self):
        ...         self.param.log(INFO, "This is an info message.")

        >>> instance = MyClass()
        >>> instance.param.log(param.INFO, "This is an info message.")
        INFO:param.MyClass...: This is an info message.

        Log a warning and treat it as an exception:

        >>> param.parameterized.warnings_as_exceptions = True
        >>> instance.param.log(param.WARNING, "This will raise an exception.")
        ...
        Exception: Warning: This will raise an exception.
        """
        if level is WARNING:
            if warnings_as_exceptions:
                raise Exception("Warning: " + msg % args)
            else:
                global warning_count
                warning_count+=1
        self_.__db_print(level, msg, *args, **kw)

    # Note that there's no _state_push method on the class, so
    # dynamic parameters set on a class can't have state saved. This
    # is because, to do this, _state_push() would need to be a
    # @bothmethod, but that complicates inheritance in cases where we
    # already have a _state_push() method.
    # (isinstance(g,Parameterized) below is used to exclude classes.)

    def _state_push(self_):
        """
        Save this instance's state.

        For Parameterized instances, this includes the state of
        dynamically generated values.

        Subclasses that maintain short-term state should additionally
        save and restore that state using _state_push() and
        _state_pop().

        Generally, this method is used by operations that need to test
        something without permanently altering the objects' state.
        """
        self = self_.self_or_cls
        if not isinstance(self, Parameterized):
            raise NotImplementedError('_state_push is not implemented at the class level')
        for pname, p in self.param.objects('existing').items():
            g = self.param.get_value_generator(pname)
            if hasattr(g,'_Dynamic_last'):
                g._saved_Dynamic_last.append(g._Dynamic_last)
                g._saved_Dynamic_time.append(g._Dynamic_time)
                # CB: not storing the time_fn: assuming that doesn't
                # change.
            elif hasattr(g,'_state_push') and isinstance(g,Parameterized):
                g._state_push()

    def _state_pop(self_):
        """
        Restore the most recently saved state.

        See _state_push() for more details.
        """
        self = self_.self_or_cls
        if not isinstance(self, Parameterized):
            raise NotImplementedError('_state_pop is not implemented at the class level')
        for pname, p in self.param.objects('existing').items():
            g = self.param.get_value_generator(pname)
            if hasattr(g,'_Dynamic_last'):
                g._Dynamic_last = g._saved_Dynamic_last.pop()
                g._Dynamic_time = g._saved_Dynamic_time.pop()
            elif hasattr(g,'_state_pop') and isinstance(g,Parameterized):
                g._state_pop()

    def pprint(
        self_,
        imports: Union[list[str], None]=None,
        prefix: str = " ",
        unknown_value: str = "<?>",
        qualify: bool = False,
        separator: str = ""
    )->str:
        """
        Generate a pretty-printed representation of the object.

        This method provides a pretty-printed string representation of the object,
        which can be evaluated using Python :func:`eval` to reconstruct the object. It is intended
        for debugging, introspection, or generating reproducible representations
        of :class:`Parameterized` objects.

        Parameters
        ----------
        imports : list of str, optional
            A list of import statements to include in the generated representation.
            Defaults to None, meaning no imports are included.
        prefix : str, optional
            A string to prepend to each line of the representation for indentation
            or formatting purposes. Default is a single space (``" "``).
        unknown_value : str, optional
            A placeholder string for values that cannot be determined or represented.
            Default is ``<?>``.
        qualify : bool, optional
            If True, includes fully qualified names (e.g., ``module.Class``) in the
            representation. Default is ``False``.
        separator : str, optional
            A string used to separate elements in the generated representation.
            Default is an empty string (``""``).

        Returns
        -------
        str
            A pretty-printed string representation of the object that can be
            evaluated using :func:`eval`.

        Raises
        ------
        NotImplementedError
            If the method is called at the class level instead of an instance
            of :class:`Parameterized`.

        Notes
        -----
        - The generated representation assumes the necessary imports are provided
          for evaluation with :func:`eval`.

        Examples
        --------
        >>> import param
        >>> class MyClass(param.Parameterized):
        ...     a = param.Number(default=10)
        ...     b = param.String(default="hello")
        >>> instance = MyClass(a=20)

        Pretty-print the instance:

        >>> instance.param.pprint()
        'MyClass(a=20)'

        Use :func:`eval` to create an instance:

        >>> eval(instance.param.pprint())
        MyClass(a=20, b='hello', name='MyClass00004')
        """
        self = self_.self_or_cls
        if not isinstance(self, Parameterized):
            raise NotImplementedError('pprint is not implemented at the class level')
        # Wrapping the staticmethod _pprint with partial to pass `self` as the `_recursive_repr`
        # decorator expects `self`` to be the pprinted object (not `self_`).
        return partial(self_._pprint, self, imports=imports, prefix=prefix,
                       unknown_value=unknown_value, qualify=qualify, separator=separator)()

    @staticmethod
    @_recursive_repr()
    def _pprint(self, imports=None, prefix=" ", unknown_value='<?>',
               qualify=False, separator=""):
        if imports is None:
            imports = [] # would have been simpler to use a set from the start
        imports[:] = list(set(imports))

        # Generate import statement
        mod = self.__module__
        bits = mod.split('.')
        imports.append("import %s"%mod)
        imports.append("import %s"%bits[0])

        changed_params = self.param.values(onlychanged=script_repr_suppress_defaults)
        values = self.param.values()
        spec = getfullargspec(type(self).__init__)
        if 'self' not in spec.args or spec.args[0] != 'self':
            raise KeyError(f"'{type(self).__name__}.__init__.__signature__' must contain 'self' as its first Parameter.")
        args = spec.args[1:]

        if spec.defaults is not None:
            posargs = spec.args[:-len(spec.defaults)]
            kwargs = dict(zip(spec.args[-len(spec.defaults):], spec.defaults))
        else:
            posargs, kwargs = args, []

        parameters = self.param.objects('existing')
        ordering = sorted(
            sorted(changed_params), # alphanumeric tie-breaker
            key=lambda k: (- float('inf')  # No precedence is lowest possible precendence
                           if parameters[k].precedence is None else
                           parameters[k].precedence))

        arglist, keywords, processed = [], [], []
        for k in args + ordering:
            if k in processed: continue

            # Suppresses automatically generated names.
            if k == 'name' and (values[k] is not None
                                and re.match('^'+self.__class__.__name__+'[0-9]+$', values[k])):
                continue

            value = pprint(values[k], imports, prefix=prefix,settings=[],
                           unknown_value=unknown_value,
                           qualify=qualify) if k in values else None

            if value is None:
                if unknown_value is False:
                    raise Exception(f"{self.name}: unknown value of {k!r}")
                elif unknown_value is None:
                    # i.e. suppress repr
                    continue
                else:
                    value = unknown_value

            # Explicit kwarg (unchanged, known value)
            if (k in kwargs) and (k in values) and kwargs[k] == values[k]: continue

            if k in posargs:
                # value will be unknown_value unless k is a parameter
                arglist.append(value)
            elif (k in kwargs or
                  (hasattr(spec, 'varkw') and (spec.varkw is not None)) or
                  (hasattr(spec, 'keywords') and (spec.keywords is not None))):
                # Explicit modified keywords or parameters in
                # precendence order (if **kwargs present)
                keywords.append(f'{k}={value}')

            processed.append(k)

        qualifier = mod + '.'  if qualify else ''
        arguments = arglist + keywords + (['**%s' % spec.varargs] if spec.varargs else [])
        return qualifier + '{}({})'.format(self.__class__.__name__,  (','+separator+prefix).join(arguments))


class ParameterizedMetaclass(type):
    """
    The metaclass of Parameterized (and all its descendents).

    The metaclass overrides type.__setattr__ to allow us to set
    Parameter values on classes without overwriting the attribute
    descriptor.  That is, for a Parameterized class of type X with a
    Parameter y, the user can type X.y=3, which sets the default value
    of Parameter y to be 3, rather than overwriting y with the
    constant value 3 (and thereby losing all other info about that
    Parameter, such as the doc string, bounds, etc.).

    The ``__init__`` method is used when defining a Parameterized class,
    usually when the module where that class is located is imported
    for the first time.  That is, the ``__init__`` in this metaclass
    initializes the *class* object, while the ``__init__`` method defined
    in each Parameterized class is called for each new instance of
    that class.

    Additionally, a class can declare itself abstract by having an
    attribute ``__abstract`` set to True. The ``abstract`` attribute can be
    used to find out if a class is abstract or not.
    """

    def __init__(mcs, name, bases, dict_):
        """
        Initialize the class object (not an instance of the class, but
        the class itself).

        Initializes all the Parameters by looking up appropriate
        default values (see ``__param_inheritance()``) and setting
        ``attrib_names`` (see ``_set_names()``).
        """
        type.__init__(mcs, name, bases, dict_)

        # Compute which parameters explicitly do not support references
        # This can be removed when Parameter.allow_refs=True by default.
        explicit_no_refs = set()
        for base in bases:
            if issubclass(base, Parameterized):
                explicit_no_refs |= set(base._param__private.explicit_no_refs)

        _param__private = _ClassPrivate(explicit_no_refs=list(explicit_no_refs))
        mcs._param__private = _param__private
        mcs.__set_name(name, dict_)
        mcs._param__parameters = Parameters(mcs)

        # All objects (with their names) of type Parameter that are
        # defined in this class
        parameters = [(n, o) for (n, o) in dict_.items()
                      if isinstance(o, Parameter)]

        for param_name,param in parameters:
            mcs._initialize_parameter(param_name, param)

        # Override class-value with default_factory
        for class_ in classlist(mcs):
            for name, val in class_.__dict__.items():
                if not isinstance(val, Parameter):
                    continue
                dfactory = val.default_factory
                if (
                    dfactory is not Undefined
                    and isinstance(dfactory, DefaultFactory)
                    and dfactory.on_class
                ):
                    setattr(mcs, name, dfactory(cls=mcs, self=None, parameter=val))

        # retrieve depends info from methods and store more conveniently
        dependers = [(n, m, m._dinfo) for (n, m) in dict_.items()
                     if hasattr(m, '_dinfo')]

        # Resolve dependencies of current class
        _watch = []
        for dname, method, dinfo in dependers:
            watch = dinfo.get('watch', False)
            on_init = dinfo.get('on_init', False)
            minfo = MInfo(cls=mcs, inst=None, name=dname, method=method)
            deps, dynamic_deps = _params_depended_on(minfo, dynamic=False)
            if watch:
                _watch.append((dname, watch == 'queued', on_init, deps, dynamic_deps))

        # Resolve dependencies in class hierarchy
        _inherited = []
        for cls in classlist(mcs)[:-1][::-1]:
            if not hasattr(cls, '_param__parameters'):
                continue
            for dep in cls.param._depends['watch']:
                method = getattr(mcs, dep[0], None)
                dinfo = getattr(method, '_dinfo', {'watch': False})
                if (not any(dep[0] == w[0] for w in _watch+_inherited)
                    and dinfo.get('watch')):
                    _inherited.append(dep)

        mcs.param._depends = {'watch': _inherited+_watch}

        if docstring_signature:
            mcs.__class_docstring()

    def __set_name(mcs, name, dict_):
        """
        Give Parameterized classes a useful 'name' attribute that is by
        default the class name, unless a class in the hierarchy has defined
        a `name` String Parameter with a defined `default` value, in which case
        that value is used to set the class name.
        """
        name_param = dict_.get("name", None)
        if name_param is not None:
            if type(name_param) is not String:
                raise TypeError(
                    f"Parameterized class {name!r} cannot override "
                    f"the 'name' Parameter with type {type(name_param)}. "
                    "Overriding 'name' is only allowed with a 'String' Parameter."
                )
            if name_param.default:
                mcs.name = name_param.default
                mcs._param__private.renamed = True
            else:
                mcs.name = name
        else:
            classes = classlist(mcs)[::-1]
            found_renamed = False
            for c in classes:
                if hasattr(c, '_param__private') and c._param__private.renamed:
                    found_renamed = True
                    break
            if not found_renamed:
                mcs.name = name

    def __class_docstring(mcs):
        """
        Customize the class docstring with a Parameter table if
        `docstring_describe_params` and the `param_pager` is available.
        """
        if not docstring_describe_params or not param_pager:
            return
        class_docstr = mcs.__doc__ if mcs.__doc__ else ''
        description = param_pager(mcs)
        mcs.__doc__ = class_docstr + '\n' + description

    def _initialize_parameter(mcs, param_name, param):
        # A Parameter has no way to find out the name a
        # Parameterized class has for it
        param._set_names(param_name)
        mcs.__param_inheritance(param_name, param)

    # Should use the official Python 2.6+ abstract base classes; see
    # https://github.com/holoviz/param/issues/84
    def __is_abstract(mcs):
        """
        Return True if the class has an attribute __abstract set to True.
        Subclasses will return False unless they themselves have
        __abstract set to true.  This mechanism allows a class to
        declare itself to be abstract (e.g. to avoid it being offered
        as an option in a GUI), without the "abstract" property being
        inherited by its subclasses (at least one of which is
        presumably not abstract).
        """
        # Can't just do ".__abstract", because that is mangled to
        # _ParameterizedMetaclass__abstract before running, but
        # the actual class object will have an attribute
        # _ClassName__abstract.  So, we have to mangle it ourselves at
        # runtime. Mangling follows description in
        # https://docs.python.org/2/tutorial/classes.html#private-variables-and-class-local-references
        try:
            return getattr(mcs,'_%s__abstract'%mcs.__name__.lstrip("_"))
        except AttributeError:
            return False

    def __get_signature(mcs):
        """
        For classes with a constructor signature that matches the default
        Parameterized.__init__ signature (i.e. ``__init__(self, **params)``)
        this method will generate a new signature that expands the
        parameters. If the signature differs from the default the
        custom signature is returned.
        """
        if mcs._param__private.signature:
            return mcs._param__private.signature
        # allowed_signature must be the signature of Parameterized.__init__
        # Inspecting `mcs.__init__` instead of `mcs` to avoid a recursion error
        if inspect.signature(mcs.__init__) != DEFAULT_SIGNATURE:
            return None
        processed_kws, keyword_groups = set(), []
        for cls in reversed(mcs.mro()):
            keyword_group = []
            for k, v in sorted(cls.__dict__.items()):
                if isinstance(v, Parameter) and k not in processed_kws and not v.readonly:
                    keyword_group.append(k)
                    processed_kws.add(k)
            keyword_groups.append(keyword_group)

        keywords = [el for grp in reversed(keyword_groups) for el in grp]
        mcs._param__private.signature = signature = inspect.Signature([
            inspect.Parameter(k, inspect.Parameter.KEYWORD_ONLY)
            for k in keywords
        ])
        return signature

    __signature__ = property(__get_signature)

    abstract = property(__is_abstract)

    def _get_param(mcs):
        return mcs._param__parameters

    param = property(_get_param)

    def __setattr__(mcs, attribute_name, value):
        """Set an attribute, supporting special behavior for Parameters.

        If the attribute being set corresponds to a Parameter descriptor and the
        new value is not a Parameter, the descriptor's ``__set__`` method is invoked
        with the provided value. This ensures proper handling of Parameter values.

        In all other cases, the attribute is set normally. If the new value is a
        Parameter, the method ensures that the value is inherited correctly from
        Parameterized superclasses as described in ``__param_inheritance()``.

        Parameters
        ----------
        attribute_name : str
            The name of the attribute to set.
        value: Any
            The value to assign to the attribute.
        """
        # Find out if there's a Parameter called attribute_name as a
        # class attribute of this class - if not, parameter is None.
        parameter,owning_class = mcs.get_param_descriptor(attribute_name)

        if parameter and not isinstance(value,Parameter):
            if owning_class != mcs:
                parameter = copy.copy(parameter)
                parameter.owner = mcs
                type.__setattr__(mcs,attribute_name,parameter)
            mcs.__dict__[attribute_name].__set__(None,value)

        else:
            type.__setattr__(mcs,attribute_name,value)

            if isinstance(value,Parameter):
                mcs.__param_inheritance(attribute_name,value)

    def __param_inheritance(mcs, param_name, param):
        """
        Look for Parameter values in superclasses of this
        Parameterized class.

        Ordinarily, when a Python object is instantiated, attributes
        not given values in the constructor will inherit the value
        given in the object's class, or in its superclasses.  For
        Parameters owned by Parameterized classes, we have implemented
        an additional level of default lookup, should this ordinary
        lookup return only `Undefined`.

        In such a case, i.e. when no non-`Undefined` value was found for a
        Parameter by the usual inheritance mechanisms, we explicitly
        look for Parameters with the same name in superclasses of this
        Parameterized class, and use the first such value that we
        find.

        The goal is to be able to set the default value (or other
        slots) of a Parameter within a Parameterized class, just as we
        can set values for non-Parameter objects in Parameterized
        classes, and have the values inherited through the
        Parameterized hierarchy as usual.

        Note that instantiate is handled differently: if there is a
        parameter with the same name in one of the superclasses with
        instantiate set to True, this parameter will inherit
        instantiate=True.
        """
        # get all relevant slots (i.e. slots defined in all
        # superclasses of this parameter)
        p_type = type(param)
        slots = dict.fromkeys(p_type._all_slots_)

        # note for some eventual future: python 3.6+ descriptors grew
        # __set_name__, which could replace this and _set_names
        setattr(param, 'owner', mcs)
        del slots['owner']

        # backwards compatibility (see Composite parameter)
        if 'objtype' in slots:
            setattr(param, 'objtype', mcs)
            del slots['objtype']

        supers = classlist(mcs)[::-1]

        # Explicitly inherit instantiate from super class and
        # check if type has changed to a more specific or different
        # Parameter type, requiring extra validation
        type_change = False
        for superclass in supers:
            super_param = superclass.__dict__.get(param_name)
            if not isinstance(super_param, Parameter):
                continue
            if super_param.instantiate is True:
                param.instantiate = True
            super_type = type(super_param)
            if not issubclass(super_type, p_type):
                type_change = True
        del slots['instantiate']

        callables, slot_values = {}, {}
        slot_overridden = False
        for slot in slots.keys():
            # Search up the hierarchy until param.slot (which has to
            # be obtained using getattr(param,slot)) is not Undefined,
            # is a new value (using identity) or we run out of classes
            # to search.
            for scls in supers:
                # Class may not define parameter or slot might not be
                # there because could be a more general type of Parameter
                new_param = scls.__dict__.get(param_name)
                if new_param is None or not hasattr(new_param, slot):
                    continue

                new_value = getattr(new_param, slot)
                old_value = slot_values.get(slot, Undefined)
                if new_value is Undefined:
                    continue
                elif new_value is old_value:
                    continue
                elif old_value is Undefined:
                    slot_values[slot] = new_value
                    # If we already know we have to re-validate abort
                    # early to avoid costly lookups
                    if slot_overridden or type_change:
                        break
                else:
                    if slot not in param._non_validated_slots:
                        slot_overridden = True
                    break

            if slot_values.get(slot, Undefined) is Undefined:
                try:
                    default_val = param._slot_defaults[slot]
                except KeyError as e:
                    raise KeyError(
                        f'Slot {slot!r} of parameter {param_name!r} has no '
                        'default value defined in `_slot_defaults`'
                    ) from e
                if slot != 'default_factory' and callable(default_val):
                    callables[slot] = default_val
                else:
                    slot_values[slot] = default_val
            elif slot == 'allow_refs':
                # Track Parameters that explicitly declared no refs
                explicit_no_refs = mcs._param__private.explicit_no_refs
                if param.allow_refs is False:
                    explicit_no_refs.append(param.name)
                elif param.allow_refs is True and param.name in explicit_no_refs:
                    explicit_no_refs.remove(param.name)

        # Now set the actual slot values
        for slot, value in slot_values.items():
            setattr(param, slot, value)

            # Avoid crosstalk between mutable slot values in different Parameter objects
            if slot != "default":
                v = getattr(param, slot)
                if _is_mutable_container(v):
                    setattr(param, slot, copy.copy(v))

        # Once all the static slots have been filled in, fill in the dynamic ones
        # (which are only allowed to use static values or results are undefined)
        for slot, fn in callables.items():
            setattr(param, slot, fn(param))

        # Once all the slot values have been set, call _update_state for Parameters
        # that need updates to make sure they're set up correctly after inheritance.
        param._update_state()

        # If the type has changed to a more specific or different type
        # or a slot value has been changed validate the default again.

        # Hack: Had to disable re-validation of None values because the
        # automatic appending of an unknown value on Selector opens a whole
        # rabbit hole in regard to the validation.
        if type_change or slot_overridden and param.default is not None:
            try:
                param._validate(param.default)
            # Param has no base validation exception class. Param Parameters raise
            # ValueError, TypeError, OSError exceptions but external Parameters
            # might raise other types of error, so we catch them all.
            except Exception as e:
                msg = f'{_validate_error_prefix(param)} failed to validate its ' \
                      'default value on class creation. '
                parents = ', '.join(klass.__name__ for klass in mcs.__mro__[1:-2])
                if not type_change and slot_overridden:
                    msg += (
                        f'The Parameter is defined with attributes which when '
                        'combined with attributes inherited from its parent '
                        f'classes ({parents}) make it invalid. '
                        'Please fix the Parameter attributes.'
                    )
                elif type_change and not slot_overridden:
                    msg += (
                        f'The Parameter type changed between class {mcs.__name__!r} '
                        f'and one of its parent classes ({parents}) which '
                        f'made it invalid. Please fix the Parameter type.'
                    )
                else:
                    # type_change and slot_overriden is not possible as when
                    # the type changes checking the slots is aborted for
                    # performance reasons.
                    pass
                msg += f'\nValidation failed with:\n{e}'
                raise RuntimeError(msg) from e

    def get_param_descriptor(mcs,param_name):
        """
        Goes up the class hierarchy (starting from the current class)
        looking for a Parameter class attribute param_name. As soon as
        one is found as a class attribute, that Parameter is returned
        along with the class in which it is declared.
        """
        classes = classlist(mcs)
        for c in classes[::-1]:
            attribute = c.__dict__.get(param_name)
            if isinstance(attribute,Parameter):
                return attribute,c
        return None,None



# Whether script_repr should avoid reporting the values of parameters
# that are just inheriting their values from the class defaults.
# Because deepcopying creates a new object, cannot detect such
# inheritance when instantiate = True, so such values will be printed
# even if they are just being copied from the default.
script_repr_suppress_defaults=True


def script_repr(
    val: 'Parameterized',
    imports: Optional[list[str]] = None,
    prefix: str = "\n    ",
    settings: list[Any] = [],
    qualify: bool = True,
    unknown_value: Optional[Any] = None,
    separator: str = "\n",
    show_imports: bool = True,
) -> str:
    r"""
    Generate a nearly runnable Python script representation of a Parameterized object.

    The ``script_repr`` function generates a string representation of a
    :class:`Parameterized` object, focusing on its parameter state. The output is
    intended to serve as a starting point for creating a Python script that,
    after minimal edits, can recreate an object with a similar parameter
    configuration. It captures only the state of the object's parameters, not
    its internal (non-parameter) attributes.

    Parameters
    ----------
    val : Parameterized
        The class`Parameterized` object to be represented.
    imports : list of str, optional
        A list of import statements to include in the output. If not provided,
        the function will populate this list based on the required modules.
    prefix : str, optional
        A string prefix added to each line of the representation for
        indentation. Default is ``"\n    "``.
    settings : list of Any, optional
        A list of settings affecting the formatting of the representation.
        Default is an empty list.
    qualify : bool, optional
        Whether to include fully qualified names (e.g., ``module.Class``).
        Default is ``True``.
    unknown_value : Any, optional
        The value to use for parameters or attributes with unknown values.
        Default is ``None``.
    separator : str, optional
        The string used to separate elements in the representation. Default is
        ``"\n"``.
    show_imports : bool, optional
        Whether to include import statements in the output. Default is ``True``.

    Returns
    -------
    str
        A string representation of the object, suitable for use in a Python
        script.

    Notes
    -----
    - The output script is designed to be a good starting point for
      recreating the parameter state of the object. However, it may require
      manual edits to ensure full compatibility or to recreate complex states.
    - The ``imports`` list may not include all modules required for parameter
      values, focusing primarily on the modules needed for the Parameterized
      object itself.

    References
    ----------
    See https://param.holoviz.org/user_guide/Serialization_and_Persistence.html#script-repr.

    Examples
    --------
    Create a Python script representation of a Parameterized object:

    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     a = param.Number(default=10, doc="A numeric parameter.")
    ...     b = param.String(default="hello", doc="A string parameter.")
    ...
    >>> instance = MyClass(a=20, b="world")
    >>> print(param.script_repr(instance))

    .. code-block:: text

        import __main__

        __main__.MyClass(a=20,

                b='world')
    """
    if imports is None:
        imports = []

    rep = pprint(val, imports, prefix, settings, unknown_value,
                 qualify, separator)

    imports = list(set(imports))
    imports_str = ("\n".join(imports) + "\n\n") if show_imports else ""

    return imports_str + rep


# PARAM2_DEPRECATION: Remove entirely unused settings argument
def pprint(val,imports=None, prefix="\n    ", settings=[],
           unknown_value='<?>', qualify=False, separator=''):
    """
    Pretty printed representation of a parameterized
    object that may be evaluated with eval.

    Similar to repr except introspection of the constructor (__init__)
    ensures a valid and succinct representation is generated.

    Only parameters are represented (whether specified as standard,
    positional, or keyword arguments). Parameters specified as
    positional arguments are always shown, followed by modified
    parameters specified as keyword arguments, sorted by precedence.

    unknown_value determines what to do where a representation cannot be
    generated for something required to recreate the object. Such things
    include non-parameter positional and keyword arguments, and certain
    values of parameters (e.g. some random state objects).

    Supplying an unknown_value of None causes unrepresentable things
    to be silently ignored. If unknown_value is a string, that
    string will appear in place of any unrepresentable things. If
    unknown_value is False, an Exception will be raised if an
    unrepresentable value is encountered.

    If supplied, imports should be a list, and it will be populated
    with the set of imports required for the object and all of its
    parameter values.

    If qualify is True, the class's path will be included (e.g. "a.b.C()"),
    otherwise only the class will appear ("C()").

    Parameters will be separated by a comma only by default, but the
    separator parameter allows an additional separator to be supplied
    (e.g. a newline could be supplied to have each Parameter appear on a
    separate line).

    Instances of types that require special handling can use the
    script_repr_reg dictionary. Using the type as a key, add a
    function that returns a suitable representation of instances of
    that type, and adds the required import statement. The repr of a
    parameter can be suppressed by returning None from the appropriate
    hook in script_repr_reg.
    """
    if imports is None:
        imports = []

    if isinstance(val,type):
        rep = type_script_repr(val,imports,prefix,settings)

    elif type(val) in script_repr_reg:
        rep = script_repr_reg[type(val)](val,imports,prefix,settings)

    elif isinstance(val, _no_script_repr):
        rep = None

    elif isinstance(val, Parameterized) or (type(val) is type and issubclass(val, Parameterized)):
        rep=val.param.pprint(imports=imports, prefix=prefix+"    ",
                        qualify=qualify, unknown_value=unknown_value,
                        separator=separator)
    else:
        rep=repr(val)

    return rep


# Registry for special handling for certain types in script_repr and pprint
script_repr_reg = {}


# currently only handles list and tuple
def container_script_repr(container,imports,prefix,settings):
    result=[]
    for i in container:
        result.append(pprint(i,imports,prefix,settings))

    ## (hack to get container brackets)
    if isinstance(container,list):
        d1,d2='[',']'
    elif isinstance(container,tuple):
        d1,d2='(',')'
    else:
        raise NotImplementedError
    rep=d1+','.join(result)+d2

    # no imports to add for built-in types

    return rep


@gen_types
def _no_script_repr():
    # Suppress scriptrepr for objects not yet having a useful string representation
    if random := sys.modules.get("random"):
        yield random.Random
    if npr := sys.modules.get("numpy.random"):
        yield npr.RandomState


def function_script_repr(fn,imports,prefix,settings):
    name = fn.__name__
    module = fn.__module__
    imports.append('import %s'%module)
    return module+'.'+name

def type_script_repr(type_,imports,prefix,settings):
    module = type_.__module__
    if module!='__builtin__':
        imports.append('import %s'%module)
    return module+'.'+type_.__name__

script_repr_reg[list] = container_script_repr
script_repr_reg[tuple] = container_script_repr
script_repr_reg[FunctionType] = function_script_repr


#: If not None, the value of this Parameter will be called (using '()')
#: before every call to __db_print, and is expected to evaluate to a
#: string that is suitable for prefixing messages and warnings (such
#: as some indicator of the global state).
dbprint_prefix=None


def truncate(str_, maxlen = 30):
    """Return HTML-safe truncated version of given string."""
    import html
    rep = (str_[:(maxlen-2)] + '..') if (len(str_) > (maxlen-2)) else str_
    return html.escape(rep)


def _get_param_repr(key, val, p, vallen=30, doclen=40):
    """HTML representation for a single Parameter object and its value."""
    import html
    if isinstance(val, Parameterized) or (type(val) is type and issubclass(val, Parameterized)):
        value = val.param._repr_html_(open=False)
    elif hasattr(val, "_repr_html_"):
        value = val._repr_html_()
    else:
        value = truncate(repr(val), vallen)

    if hasattr(p, 'bounds'):
        if p.bounds is None:
            range_ = ''
        elif hasattr(p,'inclusive_bounds'):
            # Numeric bounds use ( and [ to indicate exclusive and inclusive
            bl,bu = p.bounds
            il,iu = p.inclusive_bounds

            lb = '' if bl is None else ('>=' if il else '>') + str(bl)
            ub = '' if bu is None else ('<=' if iu else '<') + str(bu)
            range_ = lb + (', ' if lb and bu else '') + ub
        else:
            range_ = repr(p.bounds)
    elif hasattr(p, 'objects') and p.objects:
        range_ = ', '.join(list(map(repr, p.objects)))
    elif hasattr(p, 'class_'):
        if isinstance(p.class_, tuple):
            range_ = ' | '.join(kls.__name__ for kls in p.class_)
        else:
            range_ = p.class_.__name__
    elif hasattr(p, 'regex') and p.regex is not None:
        range_ = f'regex({p.regex})'
    else:
        range_ = ''

    if p.readonly:
        range_ = ' '.join(s for s in ['<i>read-only</i>', range_] if s)
    elif p.constant:
        range_ = ' '.join(s for s in ['<i>constant</i>', range_] if s)

    if getattr(p, 'allow_None', False):
        range_ = ' '.join(s for s in ['<i>nullable</i>', range_] if s)

    tooltip = f' class="param-doc-tooltip" data-tooltip="{html.escape(p.doc.strip())}"' if p.doc else ''

    return (
        f'<tr>'
        f'  <td><p style="margin-bottom: 0px;"{tooltip}>{key}</p></td>'
        f'  <td style="max-width: 200px; text-align:left;">{value}</td>'
        f'  <td style="text-align:left;">{p.__class__.__name__}</td>'
        f'  <td style="max-width: 300px;">{range_}</td>'
        f'</tr>\n'
    )


def _parameterized_repr_html(p, open):
    """HTML representation for a Parameterized object."""
    if isinstance(p, Parameterized):
        cls = p.__class__
        title = cls.name + "()"
        value_field = 'Value'
    else:
        cls = p
        title = cls.name
        value_field = 'Default'

    tooltip_css = """
.param-doc-tooltip{
  position: relative;
  cursor: help;
}
.param-doc-tooltip:hover:after{
  content: attr(data-tooltip);
  background-color: black;
  color: #fff;
  border-radius: 3px;
  padding: 10px;
  position: absolute;
  z-index: 1;
  top: -5px;
  left: 100%;
  margin-left: 10px;
  min-width: 250px;
}
.param-doc-tooltip:hover:before {
  content: "";
  position: absolute;
  top: 50%;
  left: 100%;
  margin-top: -5px;
  border-width: 5px;
  border-style: solid;
  border-color: transparent black transparent transparent;
}
"""
    openstr = " open" if open else ""
    param_values = p.param.values().items()
    contents = "".join(_get_param_repr(key, val, p.param[key])
                       for key, val in param_values)
    return (
        f'<style>{tooltip_css}</style>\n'
        f'<details {openstr}>\n'
        ' <summary style="display:list-item; outline:none;">\n'
        f'  <tt>{title}</tt>\n'
        ' </summary>\n'
        ' <div style="padding-left:10px; padding-bottom:5px;">\n'
        '  <table style="max-width:100%; border:1px solid #AAAAAA;">\n'
        f'   <tr><th style="text-align:left;">Name</th><th style="text-align:left;">{value_field}</th><th style="text-align:left;">Type</th><th>Range</th></tr>\n'
        f'{contents}\n'
        '  </table>\n </div>\n</details>\n'
    )

# _ClassPrivate and _InstancePrivate are the private namespaces of Parameterized
# classes and instance respectively, stored on the `_param__private` attribute.
# They are implemented with slots for performance reasons.

class _ClassPrivate:
    """
    parameters_state: dict
        Dict holding some transient states
    disable_instance_params: bool
        Whether to disable instance parameters
    renamed: bool
        Whethe the class has been renamed by a super class
    params: dict
        Dict of parameter_name:parameter.
    """

    __slots__ = [
        'parameters_state',
        'disable_instance_params',
        'renamed',
        'params',
        'initialized',
        'signature',
        'explicit_no_refs',
    ]

    def __init__(
        self,
        parameters_state=None,
        disable_instance_params=False,
        explicit_no_refs=None,
        renamed=False,
        params=None,
    ):
        if parameters_state is None:
            parameters_state = {
                "BATCH_WATCH": False, # If true, Event and watcher objects are queued.
                "TRIGGER": False,
                "events": [], # Queue of batched events
                "watchers": [] # Queue of batched watchers
            }
        self.parameters_state = parameters_state
        self.disable_instance_params = disable_instance_params
        self.renamed = renamed
        self.params = {} if params is None else params
        self.initialized = False
        self.signature = None
        self.explicit_no_refs = [] if explicit_no_refs is None else explicit_no_refs

    def __getstate__(self):
        return {slot: getattr(self, slot) for slot in self.__slots__}

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)


class _InstancePrivate:
    """
    initialized: bool
        Flag that can be tested to see if e.g. constant Parameters can still be set
    parameters_state: dict
        Dict holding some transient states
    dynamic_watchers: defaultdict
        Dynamic watchers
    ref_watchers: list[Watcher]
        Watchers used for internal references
    params: dict
        Dict of parameter_name:parameter
    refs: dict
        Dict of parameter name:reference
    watchers: dict
        Dict of dict:
            parameter_name:
                parameter_attribute (e.g. 'value'): list of `Watcher`s
    values: dict
        Dict of parameter name: value.
    """

    __slots__ = [
        'initialized',
        'parameters_state',
        'dynamic_watchers',
        'params',
        'async_refs',
        'refs',
        'ref_watchers',
        'syncing',
        'watchers',
        'values',
        'explicit_no_refs',
    ]

    def __init__(
        self,
        initialized=False,
        parameters_state=None,
        dynamic_watchers=None,
        refs=None,
        params=None,
        watchers=None,
        values=None,
        explicit_no_refs=None
    ):
        self.initialized = initialized
        self.explicit_no_refs = [] if explicit_no_refs is None else explicit_no_refs
        self.syncing = set()
        if parameters_state is None:
            parameters_state = {
                "BATCH_WATCH": False, # If true, Event and watcher objects are queued.
                "TRIGGER": False,
                "events": [], # Queue of batched events
                "watchers": [] # Queue of batched watchers
            }
        self.ref_watchers = []
        self.async_refs = {}
        self.parameters_state = parameters_state
        self.dynamic_watchers = defaultdict(list) if dynamic_watchers is None else dynamic_watchers
        self.params = {} if params is None else params
        self.refs = {} if refs is None else refs
        self.watchers = {} if watchers is None else watchers
        self.values = {} if values is None else values

    def __getstate__(self):
        return {slot: getattr(self, slot) for slot in self.__slots__}

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)


class DefaultFactory:
    """
    A callable factory wrapper for the ``default_factory`` Parameter attribute.

    The callable can be any callable that accepts three arguments ``cls``,
    ``self``, and ``parameter``.

    - When an instance is created, the callable receives the
      :class:`Parameterized` class as ``cls``, the instance itself as ``self``,
      and the instance-level :class:`Parameter` object as `parameter`.
    - During class creation (typically when a module defining
      :class:`Parameterized` classes is imported), and only if the
      ``DefaultFactory`` is initialized with ``on_class=True`` (default is
      ``False``), the callable is instead passed the :class:`Parameterized`
      class as ``cls``, ``None`` for ``self`` (since no instance exists yet),
      and the class-level :class:`Parameter` object as ``parameter``. This is
      the only case where the callable can influence the *class-level* attribute
      value.

    Parameters
    ----------
    factory : Callable
        A callable that produces a value when invoked. It should accept three
        arguments: ``cls``, ``self``, and ``parameter``.
    on_class : bool, optional
        Whether to call the factory on :class:`Parameterized` class creation,
        by default False.

    Examples
    --------
    We will use ``my_factory`` to set the value of parameter ``d`` to a
    dictionary that allows us to inspect the values passed to the factory.

    >>> import param
    >>> def my_factory(cls, self, parameter):
    >>>     if self is None:
    >>>         return dict(cls_a=cls.a, self_a=None, pname=parameter.name)
    >>>     else:
    >>>         return dict(cls_a=cls.a, self_a=self.a, pname=parameter.name)

    The factory is wrapped with ``DefaultFactory` with ``on_class=True``, and
    set to the ``default_factory`` attribute.

    >>> class P(param.Parameterized):
    >>>    a = param.String(default="A")
    >>>    d = param.Dict(
    >>>        default_factory=param.parameterized.DefaultFactory(
    >>>            my_factory, on_class=True
    >>>        )
    >>>    )
    ...

    Because ``on_class=True``, the factory was executed on class creation,
    setting the value of ``d`` at the class-level.

    >>> P.d
    {'cls_a': 'A', 'self_a': None, 'pname': 'd'}

    The factory is always called on instance creation.

    >>> p = P(a="foo")
    >>> p.d
    {'cls_a': 'A', 'self_a': 'foo', 'pname': 'd'}
    """

    def __init__(
        self,
        factory: Callable,
        on_class: bool = False,
    ):
        self.factory = factory
        self.on_class = on_class

    def __call__(self_, *, cls=None, self=None, parameter=None):
        return self_.factory(cls, self, parameter)


class Parameterized(metaclass=ParameterizedMetaclass):
    """
    A base class for creating Parameterized objects.

    The ``Parameterized`` base class enables two main use cases:

    - Defining rich and run-time validated class and instance attributes,
      called :class:`Parameter`.
    - Watching :class:`Parameter` for changes and reacting through callbacks.

    This makes it well-suited for robust, maintainable code bases and
    particularly useful in interactive applications requiring reactive behavior.

    To construct an instance, optional Parameter values must be supplied as
    keyword arguments (``param_name=value``), overriding their default values
    for this one instance. Any parameters not explicitly set will retain their
    defined default values.

    If no ``name`` parameter is provided, the instance's ``name`` attribute will
    default to an identifier string composed of the class name followed by
    an incremental 5-digit number.

    Attributes
    ----------
    name : str
        Class/instance name.
    param : Parameters
        ``.param`` namespace.

    References
    ----------
    https://param.holoviz.org/user_guide/Parameters.html.

    Examples
    --------
    Defining a class with two :class:`Parameter`s and a callback run on changes
    to ``my_number``.

    >>> import param
    >>> class MyClass(param.Parameterized):
    ...     my_number = param.Number(default=1, bounds=(0, 10), doc='A numeric value')
    ...     my_list = param.List(default=[1, 2, 3], item_type=int, doc='A list of integers')
    ...
    ...     @param.depends('my_number', watch=True)
    ...     def callback(self):
    ...         print(f'my_number new value: {self.my_number}')

    Parameters are available as class attributes:

    >>> MyClass.my_number
    1

    >>> obj = MyClass(my_number=2)

    Constructor arguments override default values and default :class:`Parameter`
    values are set unless overridden:

    >>> obj.my_number
    2
    >>> obj.my_list
    [1, 2, 3]

    :class:`Parameter` values are dynamically validated:

    >>> obj.my_number = 5  # Valid update within bounds.

    Attempting to set an invalid value raises an error:

    >>> try:
    >>>     obj.my_number = 15
    >>> except Exception as e:
    >>>     print(repr(e))
    ValueError: Number parameter 'MyClass.my_number' must be at most 10, not 15.

    Updating ``my_number`` executes the callback method:

    >>> obj.my_number = 7
    my_number new value: 7
    """

    name = String(default=None, constant=True, doc="""
        String identifier for this object. Default is the object's class name
        plus a unique integer""")

    def __init__(self, **params):
        # No __init__ docstring to avoid shadowing the user class docstring
        # displayed in IDEs.

        global object_count

        # Setting a Parameter value in an __init__ block before calling
        # Parameterized.__init__ (via super() generally) already sets the
        # _InstancePrivate namespace over the _ClassPrivate namespace
        # (see Parameter.__set__) so we shouldn't override it here.
        if not isinstance(self._param__private, _InstancePrivate):
            self._param__private = _InstancePrivate(
                explicit_no_refs=type(self)._param__private.explicit_no_refs
            )

        # Skip generating a custom instance name when a class in the hierarchy
        # has overriden the default of the `name` Parameter.
        if self.param.name.default == self.__class__.__name__:
            self.param._generate_name()
        refs, deps = self.param._setup_params(**params)
        object_count += 1

        self._param__private.initialized = True

        # Find parameters with default_factory through the class
        # parameters to avoid making a copy.
        params_with_default_factory = [
            pname
            for pname, pobj in self.param._cls_parameters.items()
            if pname not in params
            and callable(pobj.default_factory)
        ]
        # Set from default_factory once initialized so instance parameters
        # are copied.
        if params_with_default_factory:
            for pname in params_with_default_factory:
                pobj = self.param[pname]
                dfactory = pobj.default_factory
                if isinstance(dfactory, DefaultFactory):
                    default_val = dfactory(cls=type(self), self=self, parameter=pobj)
                else:
                    default_val = dfactory()
                with discard_events(self):
                    setattr(self, pname, default_val)

        self.param._setup_refs(deps)
        self.param._update_deps(init=True)
        self._param__private.refs = refs

    @property
    def param(self) -> Parameters:
        """
        The ``.param`` namespace for :class:`Parameterized` classes and instances.

        This namespace provides access to powerful methods and properties for managing
        parameters in a `Parameterized` object. It includes utilities for adding parameters,
        updating parameters, debugging, serialization, logging, and more.

        References
        ----------
        For more details on parameter objects and instances, see:
        https://param.holoviz.org/user_guide/Parameters.html#parameter-objects-and-instances

        Examples
        --------
        Basic usage of ``.param`` in a :class:`Parameterized` class:

        >>> import param
        >>>
        >>> class MyClass(param.Parameterized):
        ...     value = param.Parameter()
        >>>
        >>> my_instance = MyClass(value=0)

        Access the ``value`` parameter of ``my_instance``:

        >>> my_instance.param.value  # the Parameter instance

        Note that this is different from the current ``value`` of ``my_instance``:

        >>> my_instance.value  # the current parameter value
        0

        """
        return Parameters(self.__class__, self=self)

    # 'Special' methods

    def __getstate__(self):
        """
        Save the object's state: return a dictionary that is a shallow
        copy of the object's __dict__ and that also includes the
        object's __slots__ (if it has any).
        """
        # Unclear why this is a copy and not simply state.update(self.__dict__)
        state = self.__dict__.copy()
        for slot in get_occupied_slots(self):
            state[slot] = getattr(self,slot)

        # Note that Parameterized object pickling assumes that
        # attributes to be saved are only in __dict__ or __slots__
        # (the standard Python places to store attributes, so that's a
        # reasonable assumption). (Additionally, class attributes that
        # are Parameters are also handled, even when they haven't been
        # instantiated - see PickleableClassAttributes.)

        return state

    def __setstate__(self, state):
        """
        Restore objects from the state dictionary to this object.

        During this process the object is considered uninitialized.
        """
        explicit_no_refs = type(self)._param__private.explicit_no_refs
        self._param__private = _InstancePrivate(explicit_no_refs=explicit_no_refs)
        self._param__private.initialized = False

        _param__private = state.get('_param__private', None)
        if _param__private is None:
            _param__private = _InstancePrivate(explicit_no_refs=explicit_no_refs)

        # When making a copy the internal watchers have to be
        # recreated and point to the new instance
        if _param__private.watchers:
            param_watchers = _param__private.watchers
            for p, attrs in param_watchers.items():
                for attr, watchers in attrs.items():
                    new_watchers = []
                    for watcher in watchers:
                        watcher_args = list(watcher)
                        if watcher.inst is not None:
                            watcher_args[0] = self
                        fn = watcher.fn
                        if hasattr(fn, '_watcher_name'):
                            watcher_args[2] = _m_caller(self, fn._watcher_name)
                        elif get_method_owner(fn) is watcher.inst:
                            watcher_args[2] = getattr(self, fn.__name__)
                        new_watchers.append(Watcher(*watcher_args))
                    param_watchers[p][attr] = new_watchers

        state.pop('param', None)

        for name,value in state.items():
            setattr(self,name,value)
        self._param__private.initialized = True

    @_recursive_repr()
    def __repr__(self):
        """
        Provide a nearly valid Python representation that could be used to recreate
        the item with its parameters, if executed in the appropriate environment.

        Returns 'classname(parameter1=x,parameter2=y,...)', listing
        all the parameters of this object.
        """
        try:
            settings = [f'{name}={val!r}'
                        for name, val in self.param.values().items()]
        except RuntimeError: # Handle recursion in parameter depth
            settings = []
        return self.__class__.__name__ + "(" + ", ".join(settings) + ")"

    def __str__(self):
        """Return a short representation of the name and class of this object."""
        return f"<{self.__class__.__name__} {self.name}>"


class ParameterizedABCMetaclass(abc.ABCMeta, ParameterizedMetaclass):
    """Metaclass for abstract base classes using :class:`Parameterized`.

    Ensures compatibility between ``abc.ABCMeta`` and ``ParameterizedMetaclass``.
    """


class ParameterizedABC(Parameterized, metaclass=ParameterizedABCMetaclass):
    """Base class for user-defined :class:`abc.ABC` that extends :class:`Parameterized`."""

    def __init_subclass__(cls, **kwargs):
        if cls.__bases__ and cls.__bases__[0] is ParameterizedABC:
            setattr(cls, f'_{cls.__name__}__abstract', True)
        super().__init_subclass__(**kwargs)


# As of Python 2.6+, a fn's **args no longer has to be a
# dictionary. This might allow us to use a decorator to simplify using
# ParamOverrides (if that does indeed make them simpler to use).
# http://docs.python.org/whatsnew/2.6.html
class ParamOverrides(dict):
    """
    A dictionary-like object that provides two-level lookup for parameter values.

    ``ParamOverrides`` allows overriding the parameters of a :class:`ParameterizedFunction`
    or other :class:`Parameterized` object. When a parameter is accessed, it first checks
    for the value in the supplied dictionary. If the value is not present, it falls
    back to the parameter's default value on the overridden object.

    This mechanism is used to combine explicit parameter values provided at
    runtime with the default parameter values defined on a :class:`ParameterizedFunction`
    or :class:`Parameterized` object.

    References
    ----------
    See https://param.holoviz.org/user_guide/ParameterizedFunctions.html

    Examples
    --------
    Use ``ParamOverrides`` to combine runtime arguments with default parameters:

    >>> from param import Parameter, ParameterizedFunction, ParamOverrides
    >>> class multiply(ParameterizedFunction):
    ...    left  = Parameter(2, doc="Left-hand-side argument")
    ...    right = Parameter(4, doc="Right-hand-side argument")
    ...
    ...    def __call__(self, **params):
    ...        p = ParamOverrides(self, params)
    ...        return p.left * p.right
    >>> multiply()
    8
    >>> multiply(left=3, right=7)
    21
    """

    # NOTE: Attribute names of this object block parameters of the
    # same name, so all attributes of this object should have names
    # starting with an underscore (_).

    def __init__(
        self,
        overridden: Parameterized,
        dict_: dict[str, Any],
        allow_extra_keywords: bool=False
    ):
        """
        Initialize a ``ParamOverrides`` object.

        Parameters
        ----------
        overridden : Parameterized
            The object whose parameters are being overridden.
        dict_ : dict[str, Any]
            A dictionary containing parameter overrides. Keys must match
            parameter names on the overridden object unless
            ``allow_extra_keywords`` is True.
        allow_extra_keywords : bool, optional
            If ``False``, all keys in ``dict_`` must correspond to parameter names
            on the overridden object. A warning is printed for any mismatched
            keys. If ``True``, mismatched keys are stored in ``_extra_keywords``
            and can be accessed using the :meth:`extra_keywords` method. Default is
            ``False``.
        """
        # This method should be fast because it's going to be
        # called a lot. This _might_ be faster (not tested):
        #  def __init__(self,overridden,**kw):
        #      ...
        #      dict.__init__(self,**kw)
        self._overridden = overridden
        dict.__init__(self,dict_)

        if allow_extra_keywords:
            self._extra_keywords=self._extract_extra_keywords(dict_)
        else:
            self._check_params(dict_)

    def extra_keywords(self) -> dict[str, Any]:
        """
        Retrieve extra keyword arguments not matching the overridden object's parameters.

        This method returns a dictionary containing key-value pairs from the
        originally supplied ``dict_`` that do not correspond to parameter names
        of the overridden object. These extra keywords are only available if
        ``allow_extra_keywords`` was set to ``True`` during the initialization of
        the ``ParamOverrides`` instance.

        Returns
        -------
        dict[str, Any]
            A dictionary of extra keyword arguments that were not recognized as
            parameters of the overridden object. If ``allow_extra_keywords`` was
            set to ``False``, this method will return an empty dictionary.
        """
        return self._extra_keywords

    def param_keywords(self)->dict[str, Any]:
        """
        Retrieve parameters matching the overridden object's declared parameters.

        This method returns a dictionary containing key-value pairs from the
        originally supplied ``dict_`` whose keys correspond to parameters of the
        overridden object. It excludes any extra keywords that are not part of
        the object's declared parameters.

        Returns
        -------
        dict[str, Any]
            A dictionary of parameter names and their corresponding values,
            limited to those recognized as parameters of the overridden object.
        """
        return {key: self[key] for key in self if key not in self.extra_keywords()}

    def __missing__(self,name):
        # Return 'name' from the overridden object
        return getattr(self._overridden,name)

    def __repr__(self):
        # As dict.__repr__, but indicate the overridden object
        return dict.__repr__(self)+" overriding params from %s"%repr(self._overridden)

    def __getattr__(self,name):
        # Provide 'dot' access to entries in the dictionary.
        # (This __getattr__ method is called only if 'name' isn't an
        # attribute of self.)
        return self.__getitem__(name)

    def __setattr__(self,name,val):
        # Attributes whose name starts with _ are set on self (as
        # normal), but all other attributes are inserted into the
        # dictionary.
        if not name.startswith('_'):
            self.__setitem__(name,val)
        else:
            dict.__setattr__(self,name,val)

    def get(self, key: str, default: Any=None)->Any:
        """
        Retrieve the value for a given key, with a default if the key is not found.

        This method attempts to retrieve the value associated with the specified
        ``key``. If the ``key`` is not present in the ``ParamOverrides`` object, the
        method returns the provided ``default`` value instead.

        Parameters
        ----------
        key : str
            The name of the parameter or key to look up.
        default : Any, optional
            The value to return if the specified ``key`` is not found. Default is ``None``.

        Returns
        -------
        Any
            The value associated with the ``key``, or the ``default`` value if the
            ``key`` is not found.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return key in self.__dict__ or key in self._overridden.param

    def _check_params(self,params):
        """
        Print a warning if params contains something that is not a
        Parameter of the overridden object.
        """
        overridden_object_params = list(self._overridden.param)
        for item in params:
            if item not in overridden_object_params:
                self.param.warning("'%s' will be ignored (not a Parameter).",item)

    def _extract_extra_keywords(self,params):
        """
        Return any items in params that are not also
        parameters of the overridden object.
        """
        extra_keywords = {}
        overridden_object_params = list(self._overridden.param)
        for name, val in params.items():
            if name not in overridden_object_params:
                extra_keywords[name]=val
                # Could remove name from params (i.e. del params[name])
                # so that it's only available via extra_keywords()
        return extra_keywords


# Helper function required by ParameterizedFunction.__reduce__
def _new_parameterized(cls):
    return Parameterized.__new__(cls)


class ParameterizedFunction(Parameterized):
    """
    Acts like a Python function, but with arguments that are Parameters.

    When a ``ParameterizedFunction`` is instantiated, it automatically calls
    its ``__call__`` method and returns the result, acting like a Python function.

    Features:

    - Declarative parameterization: Arguments are defined as :class:`Parameter` objects,
      enabling type validation, bounds checking, and documentation.
    - Dynamic updates: Changes to parameters can dynamically influence the
      function's behavior.

    References
    ----------
    See https://param.holoviz.org/user_guide/ParameterizedFunctions.html

    Notes
    -----
    - Subclasses of ``ParameterizedFunction`` must implement the ``__call__`` method,
      which defines the primary functionality of the function.
    - The :meth:`instance` method can be used to obtain an object representation of the
      class without triggering the function's execution.

    Examples
    --------
    Define a subclass of :class:`ParameterizedFunction`:

    >>> import param
    >>> class Scale(param.ParameterizedFunction):
    ...     multiplier = param.Number(default=2, bounds=(0, 10), doc="The multiplier value.")
    ...
    ...     def __call__(self, x):
    ...         return x * self.multiplier

    Instantiating the parameterized function calls it immediately:

    >>> result = Scale(5)
    >>> result
    10

    Access the instance explicitly:

    >>> scale3 = Scale.instance(multiplier=3)
    >>> scale3.multiplier
    3
    >>> scale3(5)
    15

    Customize parameters dynamically:

    >>> scale3.multiplier = 4
    >>> scale3(5)
    20
    """

    __abstract = True

    def __str__(self):
        return self.__class__.__name__+"()"

    @bothmethod
    def instance(self_or_cls,**params):
        """
        Create and return an instance of this class.

        This method returns an instance of the :class:`ParameterizedFunction` class,
        copying parameter values from any existing instance provided or initializing
        them with the specified ``params``.

        This method is useful for obtaining a persistent object representation of
        a :class:`ParameterizedFunction` without triggering its execution (``__call__``).

        Parameters
        ----------
        **params : dict
            Parameter values to initialize the instance with. If an existing instance
            is used, its parameters are copied and updated with the provided values.

        Returns
        -------
        ParameterizedFunction
            An instance of the class with the specified or inherited parameters.

        References
        ----------
        See https://param.holoviz.org/user_guide/ParameterizedFunctions.html

        Examples
        --------
        Create an instance with default parameters:

        >>> import param
        >>> class Scale(param.ParameterizedFunction):
        ...     multiplier = param.Number(default=2, bounds=(0, 10), doc="The multiplier value.")
        ...
        >>> instance = Scale.instance()
        >>> instance.multiplier
        2

        Use the instance:

        >>> instance(5)
        10
        """
        if isinstance (self_or_cls,ParameterizedMetaclass):
            cls = self_or_cls
        else:
            p = params
            params = self_or_cls.param.values()
            params.update(p)
            params.pop('name')
            cls = self_or_cls.__class__

        inst=Parameterized.__new__(cls)
        Parameterized.__init__(inst,**params)
        if 'name' in params:  inst.__name__ = params['name']
        else:                 inst.__name__ = self_or_cls.name
        return inst

    def __new__(class_,*args,**params) -> Any:
        # Create and __call__() an instance of this class.
        inst = class_.instance()
        inst.param._set_name(class_.__name__)
        return inst.__call__(*args,**params)

    def __call__(self,*args,**kw):
        raise NotImplementedError("Subclasses must implement __call__.")

    def __reduce__(self):
        # Control reconstruction (during unpickling and copying):
        # ensure that ParameterizedFunction.__new__ is skipped
        state = ParameterizedFunction.__getstate__(self)
        # Here it's necessary to use a function defined at the
        # module level rather than Parameterized.__new__ directly
        # because otherwise pickle will find .__new__'s module to be
        # __main__. Pretty obscure aspect of pickle.py...
        return (_new_parameterized,(self.__class__,),state)

    def _pprint(self, imports=None, prefix="\n    ",unknown_value='<?>',
                qualify=False, separator=""):
        """Pretty-print the object with adjustments for instance representation.

        This method is similar to `self.param.pprint`, but replaces
        `X.classname(Y` with `X.classname.instance(Y`.

        Parameters
        ----------
        imports : optional
            Additional imports to include in the output.
        prefix : str, optional
            String prefix for each line of the output.
        unknown_value : str, optional
            Placeholder for unknown values. Defaults to '<?>'.
        qualify : bool, optional
            Whether to include full qualification for names.
        separator : str, optional
            String separator for elements.

        Returns
        -------
        str
            The formatted string representation of the object.
        """
        r = self.param.pprint(imports,prefix,
                              unknown_value=unknown_value,
                              qualify=qualify,separator=separator)
        classname=self.__class__.__name__
        return r.replace(".%s("%classname,".%s.instance("%classname)


class default_label_formatter(ParameterizedFunction):
    """Default formatter to turn parameter names into appropriate widget labels."""

    capitalize = Parameter(default=True, doc="""
        Whether or not the label should be capitalized.""")

    replace_underscores = Parameter(default=True, doc="""
        Whether or not underscores should be replaced with spaces.""")

    overrides = Parameter(default={}, doc="""
        Allows custom labels to be specified for specific parameter
        names using a dictionary where key is the parameter name and the
        value is the desired label.""")

    def __call__(self, pname):
        if pname in self.overrides:
            return self.overrides[pname]
        if self.replace_underscores:
            pname = pname.replace('_',' ')
        if self.capitalize:
            pname = pname[:1].upper() + pname[1:]
        return pname


label_formatter = default_label_formatter
