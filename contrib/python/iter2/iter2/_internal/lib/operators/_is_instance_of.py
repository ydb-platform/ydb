import typing as tp

from ..undefined import UNDEFINED


# ---

@tp.overload
def is_instance_of[DesiredType](
    obj: tp.Any,
    type: tp.Type[DesiredType],
) -> tp.TypeGuard[DesiredType]: ...


@tp.overload
def is_instance_of[DesiredType](
    type: tp.Type[DesiredType],
) -> tp.Callable[[tp.Any], tp.TypeGuard[DesiredType]]: ...


def is_instance_of(arg1, arg2=UNDEFINED):  # type: ignore
    if UNDEFINED.is_not(arg2):
        return isinstance(arg1, arg2)
    else:
        return lambda item: isinstance(item, arg1)
