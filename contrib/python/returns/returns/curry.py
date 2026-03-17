from collections.abc import Callable
from functools import partial as _partial
from functools import wraps
from inspect import BoundArguments, Signature
from typing import Any, TypeAlias, TypeVar

_ReturnType = TypeVar('_ReturnType')


def partial(
    func: Callable[..., _ReturnType],
    *args: Any,
    **kwargs: Any,
) -> Callable[..., _ReturnType]:
    """
    Typed partial application.

    It is just a ``functools.partial`` wrapper with better typing support.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this function.

    .. code:: python

      >>> from returns.curry import partial

      >>> def sum_two_numbers(first: int, second: int) -> int:
      ...     return first + second

      >>> sum_with_ten = partial(sum_two_numbers, 10)
      >>> assert sum_with_ten(2) == 12
      >>> assert sum_with_ten(-5) == 5

    See also:
        - https://docs.python.org/3/library/functools.html#functools.partial

    """
    return _partial(func, *args, **kwargs)


def curry(function: Callable[..., _ReturnType]) -> Callable[..., _ReturnType]:
    """
    Typed currying decorator.

    Currying is a conception from functional languages that does partial
    applying. That means that if we pass one argument in a function that
    gets 2 or more arguments, we'll get a new function that remembers all
    previously passed arguments. Then we can pass remaining arguments, and
    the function will be executed.

    :func:`~partial` function does a similar thing,
    but it does partial application exactly once.
    ``curry`` is a bit smarter and will do partial
    application until enough arguments passed.

    If wrong arguments are passed, ``TypeError`` will be raised immediately.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this function.

    .. code:: pycon

      >>> from returns.curry import curry

      >>> @curry
      ... def divide(number: int, by: int) -> float:
      ...     return number / by

      >>> divide(1)  # doesn't call the func and remembers arguments
      <function divide at ...>
      >>> assert divide(1)(by=10) == 0.1  # calls the func when possible
      >>> assert divide(1)(10) == 0.1  # calls the func when possible
      >>> assert divide(1, by=10) == 0.1  # or call the func like always

    Here are several examples with wrong arguments:

    .. code:: pycon

      >>> divide(1, 2, 3)
      Traceback (most recent call last):
        ...
      TypeError: too many positional arguments

      >>> divide(a=1)
      Traceback (most recent call last):
        ...
      TypeError: got an unexpected keyword argument 'a'

    Limitations:

    - It is kinda slow. Like 100 times slower than a regular function call.
    - It does not work with several builtins like ``str``, ``int``,
      and possibly other ``C`` defined callables
    - ``*args`` and ``**kwargs`` are not supported
      and we use ``Any`` as a fallback
    - Support of arguments with default values is very limited,
      because we cannot be totally sure which case we are using:
      with the default value or without it, be careful
    - We use a custom ``mypy`` plugin to make types correct,
      otherwise, it is currently impossible
    - It might not work as expected with curried ``Klass().method``,
      it might generate invalid method signature
      (looks like a bug in ``mypy``)
    - It is probably a bad idea to ``curry`` a function with lots of arguments,
      because you will end up with lots of overload functions,
      that you won't be able to understand.
      It might also be slow during the typecheck
    - Currying of ``__init__`` does not work because of the bug in ``mypy``:
      https://github.com/python/mypy/issues/8801

    We expect people to use this tool responsibly
    when they know that they are doing.

    See also:
    - https://en.wikipedia.org/wiki/Currying
    - https://stackoverflow.com/questions/218025/

    """
    argspec = Signature.from_callable(function).bind_partial()

    def decorator(*args, **kwargs):
        return _eager_curry(function, argspec, args, kwargs)

    return wraps(function)(decorator)


def _eager_curry(
    function: Callable[..., _ReturnType],
    argspec,
    args: tuple,
    kwargs: dict,
) -> _ReturnType | Callable[..., _ReturnType]:
    """
    Internal ``curry`` implementation.

    The interesting part about it is that it return the result
    or a new callable that will return a result at some point.
    """
    intermediate, full_args = _intermediate_argspec(argspec, args, kwargs)
    if full_args is not None:
        return function(*full_args[0], **full_args[1])

    # We use closures to avoid names conflict between
    # the function args and args of the curry implementation.
    def decorator(*inner_args, **inner_kwargs):
        return _eager_curry(function, intermediate, inner_args, inner_kwargs)

    return wraps(function)(decorator)


_ArgSpec: TypeAlias = (
    # Case when all arguments are bound and function can be called:
    tuple[None, tuple[tuple, dict]]
    |
    # Case when there are still unbound arguments:
    tuple[BoundArguments, None]
)


def _intermediate_argspec(
    argspec: BoundArguments,
    args: tuple,
    kwargs: dict,
) -> _ArgSpec:
    """
    That's where ``curry`` magic happens.

    We use ``Signature`` objects from ``inspect`` to bind existing arguments.

    If there's a ``TypeError`` while we ``bind`` the arguments we try again.
    The second time we try to ``bind_partial`` arguments. It can fail too!
    It fails when there are invalid arguments
    or more arguments than we can fit in a function.

    This function is slow. Any optimization ideas are welcome!
    """
    full_args = argspec.args + args
    full_kwargs = {**argspec.kwargs, **kwargs}

    try:
        argspec.signature.bind(*full_args, **full_kwargs)
    except TypeError:
        # Another option is to copy-paste and patch `getcallargs` func
        # but in this case we get responsibility to maintain it over
        # python releases.
        # This place is also responsible for raising ``TypeError`` for cases:
        # 1. When incorrect argument is provided
        # 2. When too many arguments are provided
        return argspec.signature.bind_partial(*full_args, **full_kwargs), None
    return None, (full_args, full_kwargs)
