from textwrap import dedent
from types import LambdaType
from typing import Callable, List, Mapping, Sequence

from .types import T


def list_from_indexed_dict(dct: Mapping[int, T]) -> List[T]:
    """
    Given `{0: 'v_0', ..., n: 'v_N'}`, return `['v_0', ... 'v_n']`.
    """
    return [dct[index] for index in range(max(dct) + 1)] if dct else []


def check_strictly_positive(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"{name!r} must be > 0")


def is_lambda(func: Callable) -> bool:
    """
    Return True if `func` was defined as a lambda expression, False otherwise.
    """
    return isinstance(func, LambdaType) and func.__name__ == (lambda: None).__name__


def _truncate_seq_str(seq: Sequence, max_size: int) -> str:
    if len(seq) <= max_size:  # pragma: no cover
        return str(seq)
    content = ", ".join(repr(item) for item in seq[:max_size])
    return f"[{content}, ...]"


def check_no_lambdas(funcs: Sequence[Callable], entrypoint: str) -> None:
    if all(not is_lambda(func) for func in funcs):
        return

    message = dedent(
        f"""
        lambda expression detected in {_truncate_seq_str(funcs, max_size=3)}

        HINT: Are you trying to do something like this...?

            arg = "example static arg"
            kwarg = "example static kwarg"

            await {entrypoint}(
                [lambda: async_fn(value, arg, kwarg=kwarg) for value in values]
            )

        This is almost always not what you want, as all lambdas would bind
        to the same `value` (the last one in the loop).

        Instead, use a `partial`:

            import functools

            await {entrypoint}(
                [
                    functools.partial(async_fn, value, arg, kwarg=kwarg)
                    for value in values
                ]
            )
        """
    )

    raise ValueError(message)
