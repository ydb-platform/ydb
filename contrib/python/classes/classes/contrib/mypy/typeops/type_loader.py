from typing import Union

from mypy.plugin import MethodContext, MethodSigContext
from mypy.types import Instance
from mypy.types import Type as MypyType
from typing_extensions import Final

_SUPPORTS_QUALIFIED_NAME: Final = 'classes.Supports'


def load_supports_type(
    arg_type: MypyType,
    ctx: Union[MethodContext, MethodSigContext],
) -> Instance:
    """
    Loads ``Supports[]`` type with proper generic type.

    It uses the short name,
    because for some reason full name is not always loaded.
    """
    supports_spec = ctx.api.named_generic_type(
        _SUPPORTS_QUALIFIED_NAME,
        [arg_type],
    )
    assert supports_spec
    supports_spec.type._promote = None  # noqa: WPS437
    return supports_spec


def load_typeclass(
    fullname: str,
    ctx: MethodContext,
) -> Instance:
    """Loads given typeclass from a symboltable by a fullname."""
    typeclass_info = ctx.api.lookup_qualified(fullname)  # type: ignore
    assert isinstance(typeclass_info.type, Instance)
    return typeclass_info.type
