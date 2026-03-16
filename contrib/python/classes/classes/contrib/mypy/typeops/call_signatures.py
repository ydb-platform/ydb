from typing import Optional

from mypy.messages import callable_name
from mypy.plugin import MethodSigContext
from mypy.subtypes import is_subtype
from mypy.typeops import get_type_vars, make_simplified_union
from mypy.types import CallableType, Instance, ProperType
from mypy.types import Type as MypyType
from mypy.types import TypeVarType, union_items
from typing_extensions import Final, final

from classes.contrib.mypy.typeops import type_loader

_INCOMPATIBLE_TYPEVAR_MSG: Final = (
    'Argument 1 to {0} has incompatible type "{1}"; expected "{2}"'
)


@final
class SmartCallSignature(object):
    """
    Infers the ``__call__`` signature of a typeclass.

    What it does?
    1. It handles ``instance: X`` (where ``X`` is a type variable) case properly
    2. It handles ``instance: Iterable[X]`` case properly
    3. And ``instance: Any`` works as well

    """

    __slots__ = (
        '_signature',
        '_instance_type',
        '_associated_type',
        '_ctx',
    )

    def __init__(
        self,
        signature: CallableType,
        instance_type: MypyType,
        associated_type: MypyType,
        ctx: MethodSigContext,
    ) -> None:
        """Context that we need."""
        self._signature = signature.copy_modified()
        self._instance_type = instance_type
        self._associated_type = associated_type
        self._ctx = ctx

    def mutate_and_infer(self, passed_type: MypyType) -> CallableType:
        """Main entry point."""
        first_arg = self._signature.arg_types[0]
        # TODO: later we can refactor this to be `TypeTransformer`
        if isinstance(first_arg, TypeVarType):
            return self._infer_type_var(first_arg, passed_type)
        return self._infer_regular(first_arg)

    def _infer_type_var(
        self,
        first_arg: TypeVarType,
        passed_type: MypyType,
    ) -> CallableType:
        instance_types = union_items(self._instance_type)
        if isinstance(self._associated_type, Instance):
            instance_types.append(_load_supports_type(
                first_arg,
                self._associated_type,
                self._ctx,
            ))

        instance_type = make_simplified_union(instance_types)
        if not is_subtype(passed_type, instance_type):
            # Let's explain: what happens here?
            # We need to enforce
            self._ctx.api.fail(
                _INCOMPATIBLE_TYPEVAR_MSG.format(
                    callable_name(self._signature),
                    passed_type,
                    instance_type,
                ),
                self._ctx.context,
            )
        return self._signature

    def _infer_regular(self, first_arg: MypyType) -> CallableType:
        supports_type: Optional[MypyType] = None
        if isinstance(self._associated_type, Instance):
            supports_type = _load_supports_type(
                first_arg,
                self._associated_type,
                self._ctx,
                should_replace_typevars=True,
            )
        self._signature.arg_types[0] = make_simplified_union(
            list(filter(None, [self._instance_type, supports_type])),
        )
        return self._signature


def _load_supports_type(
    first_arg: MypyType,
    associated_type: Instance,
    ctx: MethodSigContext,
    *,
    should_replace_typevars: bool = False,
) -> ProperType:
    """
    Load ``Supports`` type and injects proper type vars into associated type.

    Why do we need this function?

    Let's see what will happen without it:
    For example, typeclass `ToJson` with `int` and `str` have will have
    `Union[str, int]` as the first argument type.
    But, we need `Union[str, int, Supports[ToJson]]`
    That's why we are loading this type if the definition is there.
    """
    if should_replace_typevars:
        # Why do we replace typevars here?
        # Well, because of how `mypy` treats different type variables.
        # `mypy` compares type variables by their ids.
        # We might end with this case:
        #
        # instance: Iterable[X`1]
        # associated_type: List[Y`2]
        #
        # These variables won't match. And we will break inference.
        # Something like "Y`2" will be returned as the result.
        # That's why we need to have proper type variables
        # injected into the associated type instance.
        associated_type = associated_type.copy_modified(
            args=set(get_type_vars(first_arg)),
        )
    return type_loader.load_supports_type(associated_type, ctx)
