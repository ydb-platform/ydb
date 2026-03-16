from typing import TypeVar

from returns.io import IO

_ValueType = TypeVar('_ValueType')


def unsafe_perform_io(wrapped_in_io: IO[_ValueType]) -> _ValueType:
    """
    Compatibility utility and escape mechanism from ``IO`` world.

    Just unwraps the internal value
    from :class:`returns.io.IO` container.
    Should be used with caution!
    Since it might be overused by lazy and ignorant developers.

    It is recommended to have only one place (module / file)
    in your program where you allow unsafe operations.

    We recommend to use ``import-linter`` to enforce this rule.

    .. code:: python

      >>> from returns.io import IO
      >>> assert unsafe_perform_io(IO(1)) == 1

    See also:
        - https://github.com/seddonym/import-linter

    """
    return wrapped_in_io._inner_value  # noqa: SLF001
