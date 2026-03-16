from __future__ import annotations

import collections
import datetime as dt
import functools
import inspect
import numbers
import os
import re
import traceback
import warnings
from collections import OrderedDict, abc, defaultdict
from contextlib import contextmanager
from numbers import Real
from textwrap import dedent
from threading import get_ident
from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Concatenate, ParamSpec

    P = ParamSpec("P")
    R = TypeVar("R")
    CallableT = TypeVar("CallableT", bound=Callable)

DEFAULT_SIGNATURE = inspect.Signature([
    inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
    inspect.Parameter('params', inspect.Parameter.VAR_KEYWORD),
])

MUTABLE_TYPES = (abc.MutableSequence, abc.MutableSet, abc.MutableMapping)

class ParamWarning(Warning):
    """Base Param Warning."""

class ParamPendingDeprecationWarning(ParamWarning, PendingDeprecationWarning):
    """
    Param PendingDeprecationWarning.

    This warning type is useful when the warning is not meant to be displayed
    to REPL/notebooks users, as DeprecationWarning are displayed when triggered
    by code in __main__ (__name__ == '__main__' in a REPL).
    """


class ParamDeprecationWarning(ParamWarning, DeprecationWarning):
    """
    Param DeprecationWarning.

    Ignored by default except when triggered by code in __main__
    """


class ParamFutureWarning(ParamWarning, FutureWarning):
    """
    Param FutureWarning.

    Always displayed.
    """


class Skip(Exception):
    """Exception that allows skipping an update when resolving a reference.

    References
    ----------
    https://param.holoviz.org/user_guide/References.html#skipping-reference-updates

    Examples
    --------
    >>> import param
    >>> class W(param.Parameterized):
    ...    a = param.Number()
    ...    b = param.Number(allow_refs=True)
    ...    run = param.Event()
    >>> w = W(a=0, b=2)

    Let's define a function:

    >>> def add(a, b, run):
    ...    if not run:
    ...        raise param.Skip
    ...    return a + b

    Let's use the function as a reference:

    >>> v = W(b=param.bind(add, w.param.a, w.param.b, w.param.run))

    We can see that b has not yet been resolved:

    >>> v.b
    0.0

    Let's trigger `w.param.run` and check `v.b` has been resolved:

    >>> w.param.trigger('run')
    >>> v.b
    2
    """


def _deprecated(extra_msg="", warning_cat=ParamDeprecationWarning):
    def decorator(func):
        """Mark a function or method as deprecated.

        This internal decorator issues a warning when the decorated function
        or method is called, indicating that it has been deprecated and will
        be removed in a future version.

        Parameters
        ----------
        func: FunctionType | MethodType
            The function or method to mark as deprecated.

        Returns
        -------
        callable:
            The wrapped function that issues a deprecation warning.
        """
        @functools.wraps(func)
        def inner(*args, **kwargs):
            msg = f"{func.__name__!r} has been deprecated and will be removed in a future version."
            if extra_msg:
                em = dedent(extra_msg)
                em = em.strip().replace('\n', ' ')
                msg = msg + ' ' + em
            warnings.warn(msg, category=warning_cat, stacklevel=_find_stack_level())
            return func(*args, **kwargs)
        return inner
    return decorator


# Copy of Python 3.2 reprlib's recursive_repr but allowing extra arguments
def _recursive_repr(fillvalue='...'):
    """Decorate a repr function to return a fill value for recursive calls."""

    def decorating_function(user_function):
        repr_running = set()

        def wrapper(self, *args, **kwargs):
            key = id(self), get_ident()
            if key in repr_running:
                return fillvalue
            repr_running.add(key)
            try:
                result = user_function(self, *args, **kwargs)
            finally:
                repr_running.discard(key)
            return result
        return wrapper

    return decorating_function


def _is_auto_name(class_name, instance_name):
    return re.match('^'+class_name+'[0-9]{5}$', instance_name)


def _find_pname(pclass):
    """
    Go up the stack and attempt to find a Parameter declaration of the form
    `pname = param.Parameter(` or `pname = pm.Parameter(`.
    """
    stack = traceback.extract_stack()
    for frame in stack:
        match = re.match(r"^(\S+)\s*=\s*(param|pm)\." + pclass + r"\(", frame.line)
        if match:
            return match.group(1)


def _validate_error_prefix(parameter, attribute=None):
    """
    Generate an error prefix suitable for Parameters when they raise a validation
    error.

    - unbound and name can't be found: "Number parameter"
    - unbound and name can be found: "Number parameter 'x'"
    - bound parameter: "Number parameter 'P.x'"
    """
    from param.parameterized import ParameterizedMetaclass

    pclass = type(parameter).__name__
    if parameter.owner is not None:
        if issubclass(type(parameter.owner), ParameterizedMetaclass):
            powner = parameter.owner.__name__
        else:
            powner = type(parameter.owner).__name__
    else:
        powner = None
    pname = parameter.name
    out = []
    if attribute:
        out.append(f'Attribute {attribute!r} of')
    out.append(f'{pclass} parameter')
    if pname:
        if powner:
            desc = f'{powner}.{pname}'
        else:
            desc = pname
        out.append(f'{desc!r}')
    else:
        try:
            pname = _find_pname(pclass)
            if pname:
                out.append(f'{pname!r}')
        except Exception:
            pass
    return ' '.join(out)

def _is_mutable_container(value):
    """Determine if the value is a mutable container.

    Mutable containers typically require special handling when being copied.
    """
    return isinstance(value, MUTABLE_TYPES)

def full_groupby(l, key=lambda x: x):
    """Groupby implementation which does not require a prior sort."""
    d = defaultdict(list)
    for item in l:
        d[key(item)].append(item)
    return d.items()

def iscoroutinefunction(function):
    """Whether the function is an asynchronous generator or a coroutine."""
    # Partial unwrapping not required starting from Python 3.11.0
    # See https://github.com/holoviz/param/pull/894#issuecomment-1867084447
    while isinstance(function, functools.partial):
        function = function.func
    return (
        inspect.isasyncgenfunction(function) or
        inspect.iscoroutinefunction(function)
    )

async def _to_async_gen(sync_gen):
    import asyncio

    done = object()

    def safe_next():
        # Converts StopIteration to a sentinel value to avoid:
        # TypeError: StopIteration interacts badly with generators and cannot be raised into a Future
        try:
            return next(sync_gen)
        except StopIteration:
            return done

    while True:
        value = await asyncio.to_thread(safe_next)
        if value is done:
            break
        yield value

def flatten(line):
    """
    Flatten an arbitrarily nested sequence.

    Inspired by: pd.core.common.flatten

    Parameters
    ----------
    line : sequence
        The sequence to flatten

    Notes
    -----
    This only flattens list, tuple, and dict sequences.

    Returns
    -------
    flattened : generator

    """
    for element in line:
        if any(isinstance(element, tp) for tp in (list, tuple, dict)):
            yield from flatten(element)
        else:
            yield element


def accept_arguments(
    f: Callable[Concatenate[CallableT, P], R]
) -> Callable[P, Callable[[CallableT], R]]:
    """Decorate a decorator to accept arguments."""
    @functools.wraps(f)
    def _f(*args: P.args, **kwargs: P.kwargs) -> Callable[[CallableT], R]:
        return lambda actual_f: f(actual_f, *args, **kwargs)
    return _f


def _produce_value(value_obj):
    """Produce an actual value from a stored object.

    If the object is callable, call it; otherwise, return the object.
    """
    if callable(value_obj):
        return value_obj()
    else:
        return value_obj


def _hashable(x):
    """
    Return a hashable version of the given object x, with lists and
    dictionaries converted to tuples.  Allows mutable objects to be
    used as a lookup key in cases where the object has not actually
    been mutated. Lookup will fail (appropriately) in cases where some
    part of the object has changed.  Does not (currently) recursively
    replace mutable subobjects.
    """
    if isinstance(x, collections.abc.MutableSequence):
        return tuple(x)
    elif isinstance(x, collections.abc.MutableMapping):
        return tuple([(k,v) for k,v in x.items()])
    else:
        return x


def _named_objs(objlist, namesdict=None):
    """
    Given a list of objects, returns a dictionary mapping from
    string name for the object to the object itself. Accepts
    an optional name,obj dictionary, which will override any other
    name if that item is present in the dictionary.
    """
    objs = OrderedDict()

    objtoname = {}
    unhashables = []
    if namesdict is not None:
        for k, v in namesdict.items():
            try:
                objtoname[_hashable(v)] = k
            except TypeError:
                unhashables.append((k, v))

    for obj in objlist:
        if objtoname and _hashable(obj) in objtoname:
            k = objtoname[_hashable(obj)]
        elif any(obj is v for (_, v) in unhashables):
            k = [k for (k, v) in unhashables if v is obj][0]
        elif hasattr(obj, "name"):
            k = obj.name
        elif hasattr(obj, '__name__'):
            k = obj.__name__
        else:
            k = str(obj)
        objs[k] = obj
    return objs


def _get_min_max_value(min, max, value=None, step=None):
    """Return min, max, value given input values with possible None."""
    # Either min and max need to be given, or value needs to be given
    if value is None:
        if min is None or max is None:
            raise ValueError(
                f'unable to infer range, value from: ({min}, {max}, {value})'
            )
        diff = max - min
        value = min + (diff / 2)
        # Ensure that value has the same type as diff
        if not isinstance(value, type(diff)):
            value = min + (diff // 2)
    else:  # value is not None
        if not isinstance(value, Real):
            raise TypeError('expected a real number, got: %r' % value)
        # Infer min/max from value
        if value == 0:
            # This gives (0, 1) of the correct type
            vrange = (value, value + 1)
        elif value > 0:
            vrange = (-value, 3*value)
        else:
            vrange = (3*value, -value)
        if min is None:
            min = vrange[0]
        if max is None:
            max = vrange[1]
    if step is not None:
        # ensure value is on a step
        tick = int((value - min) / step)
        value = min + tick * step
    if not min <= value <= max:
        raise ValueError(f'value must be between min and max (min={min}, value={value}, max={max})')
    return min, max, value


def _deserialize_from_path(ext_to_routine, path, type_name):
    """
    Call deserialization routine with path according to extension.
    ext_to_routine should be a dictionary mapping each supported
    file extension to a corresponding loading function.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(
            "Could not parse file '{}' as {}: does not exist or is not a file"
            "".format(path, type_name))
    root, ext = os.path.splitext(path)
    if ext in {'.gz', '.bz2', '.xz', '.zip'}:
        # A compressed type. We'll assume the routines can handle such extensions
        # transparently (if not, we'll fail later)
        ext = os.path.splitext(root)[1]
    # FIXME(sdrobert): try...except block below with "raise from" might be a good idea
    # once py2.7 support is removed. Provides error + fact that failure occurred in
    # deserialization
    if ext in ext_to_routine:
        return ext_to_routine[ext](path)
    raise ValueError(
        "Could not parse file '{}' as {}: no deserialization method for files with "
        "'{}' extension. Supported extensions: {}"
        "".format(path, type_name, ext, ', '.join(sorted(ext_to_routine))))


def _is_number(obj):
    if isinstance(obj, numbers.Number): return True
    # The extra check is for classes that behave like numbers, such as those
    # found in numpy, gmpy2, etc.
    elif (hasattr(obj, '__int__') and hasattr(obj, '__add__')): return True
    # This is for older versions of gmpy
    elif hasattr(obj, 'qdiv'): return True
    else: return False


def _is_abstract(class_: type) -> bool:
    if inspect.isabstract(class_):
        return True
    try:
        return class_.abstract
    except AttributeError:
        return False


def descendents(class_: type, concrete: bool = False) -> list[type]:
    """
    Return a list of all descendent classes of a given class.

    This function performs a breadth-first traversal of the class hierarchy,
    collecting all subclasses of ``class_``. The result includes ``class_`` itself
    and all of its subclasses. If ``concrete=True``, abstract base classes
    are excluded from the result, including :class:`Parameterized` abstract
    classes declared with ``__abstract = True``.

    Parameters
    ----------
    class_ : type
        The base class whose descendants should be found.
    concrete : bool, optional
        If ``True``, exclude abstract classes from the result. Default is ``False``.

        .. versionadded:: 2.3.0

        Added to encourage users to use :func:`descendents` in favor of
        :func:`concrete_descendents` that clobbers classes sharing the same name.

    Returns
    -------
    list[type]
        A list of descendent classes, ordered from the most base to the most derived.

    Examples
    --------
    >>> class A: pass
    >>> class B(A): pass
    >>> class C(A): pass
    >>> class D(B): pass
    >>> descendents(A)
    [A, B, C, D]
    """
    if not isinstance(class_, type):
        raise TypeError(f"descendents expected a class object, not {type(class_).__name__}")
    q = [class_]
    out = []
    while len(q):
        x = q.pop(0)
        out.insert(0, x)
        try:
            subclasses = x.__subclasses__()
        except TypeError:
            # TypeError raised when __subclasses__ is called on unbound methods,
            # on `type` for example.
            continue
        for b in subclasses:
            if b not in q and b not in out:
                q.append(b)
    return [kls for kls in out if not (concrete and _is_abstract(kls))][::-1]


# Could be a method of ClassSelector.
def concrete_descendents(parentclass: type) -> dict[str, type]:
    """
    Return a dictionary containing all subclasses of the specified
    parentclass, including the parentclass (prefer :func:`descendents`).

    Only classes that are defined in scripts that have been run or modules
    that have been imported are included, so the caller will usually first
    do ``from package import *``.

    Only non-abstract classes will be included.

    .. warning::
       ``concrete_descendents`` overrides descendents that share the same
       class name. To avoid this, use :func:`descendents` with ``concrete=True``.
    """
    # Warns
    # -----
    # ParamWarning
    #     ``concrete_descendents`` overrides descendents that share the same
    #     class name. To avoid this, use :func:`descendents` with ``concrete=True``.

    desc = descendents(parentclass, concrete=True)
    concrete_desc = {c.__name__: c for c in desc}
    # Descendents with the same name are clobbered.
    # if len(desc) != len(concrete_desc):
    #     class_count = Counter([kls.__name__ for kls in desc])
    #     clobbered = [kls for kls, count in class_count.items() if count > 1]
    #     warnings.warn(
    #         '`concrete_descendents` overrides descendents that share the same '
    #         'class name. Other descendents with the same name as the following '
    #         f'classes exist but were not returned: {clobbered!r}\n'
    #         'Use `descendents(parentclass, concrete=True)` instead.',
    #         ParamWarning,
    #     )
    return concrete_desc


def _abbreviate_paths(pathspec,named_paths):
    """
    Given a dict of (pathname,path) pairs, removes any prefix shared by all pathnames.
    Helps keep menu items short yet unambiguous.
    """
    from os.path import commonprefix, dirname, sep

    prefix = commonprefix([dirname(name)+sep for name in named_paths.keys()]+[pathspec])
    return OrderedDict([(name[len(prefix):],path) for name,path in named_paths.items()])


def _to_datetime(x):
    """Convert a date object to a datetime object for comparison.

    This internal function ensures that date and datetime objects can be
    compared without errors by converting date objects to datetime objects.

    Parameters
    ----------
    x: Any
        The object to convert, which may be a `date` or `datetime` object.

    Returns
    -------
    datetime.datetime:
        A datetime object if the input was a date object;
        otherwise, the input is returned unchanged.
    """
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return dt.datetime(*x.timetuple()[:6])
    return x


@contextmanager
def exceptions_summarized():
    """Context manager to display exceptions concisely without tracebacks.

    This utility is useful for writing documentation or examples where
    only the exception type and message are needed, without the full
    traceback.

    Yields
    ------
    None:
        Allows the code inside the context to execute.

    Prints
    ------
    A concise summary of any exception raised, including the exception
    type and message, to `sys.stderr`.
    """
    try:
        yield
    except Exception:
        import sys
        etype, value, tb = sys.exc_info()
        print(f"{etype.__name__}: {value}", file=sys.stderr)


def _in_ipython():
    try:
        get_ipython
        return True
    except NameError:
        return False

_running_tasks = set()

def async_executor(func):
    import asyncio
    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError:
        event_loop = asyncio.new_event_loop()
    if event_loop.is_running():
        task = asyncio.ensure_future(func())
        _running_tasks.add(task)
        task.add_done_callback(_running_tasks.discard)
    else:
        event_loop.run_until_complete(func())

class _GeneratorIsMeta(type):
    def __instancecheck__(cls, inst):
        return isinstance(inst, tuple(cls.types()))

    def __subclasscheck__(cls, sub):
        return issubclass(sub, tuple(cls.types()))

    def __iter__(cls):
        yield from cls.types()

class _GeneratorIs(metaclass=_GeneratorIsMeta):
    @classmethod
    def __iter__(cls):
        yield from cls.types()

def gen_types(gen_func):
    """Decorate a generator function to support type checking.

    This decorator modifies a generator function that yields different types
    so that it can be used with `isinstance` and `issubclass`.

    Parameters
    ----------
    gen_func : GeneratorType
        The generator function to decorate.

    Returns
    -------
    type:
        A new type that supports `isinstance` and `issubclass` checks
        based on the generator function's yielded types.

    Raises
    ------
    TypeError: If the provided function is not a generator function.
    """
    if not inspect.isgeneratorfunction(gen_func):
        msg = "gen_types decorator can only be applied to generator"
        raise TypeError(msg)
    return type(gen_func.__name__, (_GeneratorIs,), {"types": staticmethod(gen_func)})


def _find_stack_level() -> int:
    """
    Find the first place in the stack that is not inside numbergen and param.

    Inspired by: pandas.util._exceptions.find_stack_level.
    """
    import numbergen
    import param

    ng_dir = os.path.dirname(numbergen.__file__)
    param_dir = os.path.dirname(param.__file__)

    # https://stackoverflow.com/questions/17407119/python-inspect-stack-is-slow
    frame = inspect.currentframe()
    try:
        n = 0
        while frame:
            filename = inspect.getfile(frame)
            if filename.startswith((ng_dir, param_dir)):
                frame = frame.f_back
                n += 1
            else:
                break
    finally:
        # See note in
        # https://docs.python.org/3/library/inspect.html#inspect.Traceback
        del frame
    return n
