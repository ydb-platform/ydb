
from typing import List, Optional

from mypy.nodes import Decorator, Expression
from mypy.plugin import MethodContext
from mypy.typeops import make_simplified_union
from mypy.types import (
    CallableType,
    FunctionLike,
    Instance,
    LiteralType,
    TupleType,
)
from mypy.types import Type as MypyType
from mypy.types import TypeVarType, UninhabitedType
from typing_extensions import Final

_TYPECLASS_DEF_FULLNAMES: Final = frozenset((
    'classes._typeclass._TypeClassInstanceDef',
))


def try_to_apply_generics(
    signature: CallableType,
    instance_type: MypyType,
    ctx: MethodContext,
) -> CallableType:
    """
    Conditionally applies instance type on typeclass'es signature.

    What we are looking for here?
    We are interested in typeclass where instance is a type variable.
    For example:

    .. code:: python

      def copy(instance: X) -> X:
          ...

    In this case, we would apply instance type on top of this signature
    to get the real one for this instance.
    """
    if not isinstance(signature.arg_types[0], TypeVarType):
        return signature

    type_args: List[Optional[MypyType]] = [instance_type]
    if len(signature.variables) > 1:
        # `None` here means that a type variable won't be replaced
        type_args.extend(None for _ in range(len(signature.variables) - 1))

    checker = ctx.api.expr_checker  # type: ignore
    return checker.apply_type_arguments_to_callable(
        signature,
        type_args,
        ctx.context,
    )


def all_same_instance_calls(
    fullname: str,
    ctx: MethodContext,
) -> bool:
    """
    Checks whether a given context has instance calls of only one typeclass.

    In other words, it checks that all decorators come from the same typeclass.
    """
    if isinstance(ctx.context, Decorator) and len(ctx.context.decorators) > 1:
        return all(
            _get_typeclass_instance_type(func_dec, fullname, ctx) is not None
            for func_dec in ctx.context.decorators
        )
    return True


def type_obj(type_: MypyType) -> MypyType:
    """Returns type object from a function like."""
    if isinstance(type_, FunctionLike) and type_.is_type_obj():
        # What happens here?
        # Let's say you define a function like this:
        #
        # @some.instance(Sized)
        # (instance: Sized, b: int) -> str: ...
        #
        # So, you will receive callable type
        # `def () -> Sized` as `runtime_type` in this case.
        # We need to convert it back to regular `Instance`.
        #
        # It can also be `Overloaded` type,
        # but they are safe to return the same `type_object`,
        # however we still use `ret_type`,
        # because it is practically the same thing,
        # but with proper type arguments.
        return type_.items[0].ret_type
    return type_


def infer_instance_type_from_context(
    passed_args: TupleType,
    fullname: str,
    ctx: MethodContext,
) -> MypyType:
    """
    Infers instance type from several ``@some.instance()`` decorators.

    We have a problem: when user has two ``.instance()`` decorators
    on a single function, inference will work only
    for a single one of them at the time.

    So, let's say you have this:

    .. code:: python

      @some.instance(str)
      @some.instance(int)
      def _some_int_str(instance: Union[str, int]): ...

    Your instance has ``Union[str, int]`` annotation as it should have.
    But, our ``fallback`` type would be just ``int`` on the first call
    and just ``str`` on the second call.

    And this will break our ``is_same_type`` check,
    because ``Union[str, int]`` is not the same as ``int`` or ``str``.

    In this case we need to fetch all typeclass decorators and infer
    the resulting type manually.

    The same works for ``protocol=`` and ``delegate=`` types.
    """
    if isinstance(ctx.context, Decorator) and len(ctx.context.decorators) > 1:
        # Why do we only care for this case?
        # Because if it is a call / or just a single decorator,
        # then we are fine with regular type inference.
        # Inferred type from `mypy` is good enough, just return `fallback`.
        instance_types = []
        for decorator in ctx.context.decorators:
            instance_type = _get_typeclass_instance_type(
                decorator,
                fullname,
                ctx,
            )
            if instance_type is not None:
                instance_types.append(type_obj(instance_type))

        # Inferred resulting type:
        return make_simplified_union(instance_types)

    fallback = _first_real_passed_arg(passed_args)
    return type_obj(fallback) if fallback is not None else UninhabitedType()


def _get_typeclass_instance_type(
    expr: Expression,
    fullname: str,
    ctx: MethodContext,
) -> Optional[MypyType]:
    expr_type = ctx.api.expr_checker.accept(expr)  # type: ignore
    is_typeclass_instance_def = (
        isinstance(expr_type, Instance) and
        bool(expr_type.type) and
        expr_type.type.fullname in _TYPECLASS_DEF_FULLNAMES and
        isinstance(expr_type.args[1], Instance)
    )
    if is_typeclass_instance_def:
        inst = expr_type.args[1]
        is_same_typeclass = (
            isinstance(inst.args[3], LiteralType) and
            inst.args[3].value == fullname
        )
        if is_same_typeclass:
            # We need the first, existing, non-NoReturn argument:
            return _first_real_passed_arg(expr_type.args[0])
    return None


def _first_real_passed_arg(passed_args: TupleType) -> Optional[MypyType]:
    usable_args = (
        type_arg
        for type_arg in passed_args.items
        if not isinstance(type_arg, UninhabitedType)
    )
    return next(usable_args, None)
