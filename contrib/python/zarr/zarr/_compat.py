import warnings
from collections.abc import Callable
from functools import wraps
from inspect import Parameter, signature
from typing import Any, TypeVar

from zarr.errors import ZarrFutureWarning

T = TypeVar("T")

# Based off https://github.com/scikit-learn/scikit-learn/blob/e87b32a81c70abed8f2e97483758eb64df8255e9/sklearn/utils/validation.py#L63


def _deprecate_positional_args(
    func: Callable[..., T] | None = None, *, version: str = "3.1.0"
) -> Callable[..., T]:
    """Decorator for methods that issues warnings for positional arguments.

    Using the keyword-only argument syntax in pep 3102, arguments after the
    * will issue a warning when passed as a positional argument.

    Parameters
    ----------
    func : callable, default=None
        Function to check arguments on.
    version : callable, default="3.1.0"
        The version when positional arguments will result in error.
    """

    def _inner_deprecate_positional_args(f: Callable[..., T]) -> Callable[..., T]:
        sig = signature(f)
        kwonly_args = []
        all_args = []

        for name, param in sig.parameters.items():
            if param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                all_args.append(name)
            elif param.kind == Parameter.KEYWORD_ONLY:
                kwonly_args.append(name)

        @wraps(f)
        def inner_f(*args: Any, **kwargs: Any) -> T:
            extra_args = len(args) - len(all_args)
            if extra_args <= 0:
                return f(*args, **kwargs)

            # extra_args > 0
            args_msg = [
                f"{name}={arg}"
                for name, arg in zip(kwonly_args[:extra_args], args[-extra_args:], strict=False)
            ]
            formatted_args_msg = ", ".join(args_msg)
            warnings.warn(
                (
                    f"Pass {formatted_args_msg} as keyword args. From version "
                    f"{version} passing these as positional arguments "
                    "will result in an error"
                ),
                ZarrFutureWarning,
                stacklevel=2,
            )
            kwargs.update(zip(sig.parameters, args, strict=False))
            return f(**kwargs)

        return inner_f

    if func is not None:
        return _inner_deprecate_positional_args(func)

    return _inner_deprecate_positional_args  # type: ignore[return-value]
