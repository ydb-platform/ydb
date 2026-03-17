
from mypy.plugin import MethodContext
from mypy.subtypes import is_subtype
from mypy.types import AnyType, CallableType, TypeOfAny
from typing_extensions import Final

from classes.contrib.mypy.typeops import inference
from classes.contrib.mypy.typeops.instance_context import InstanceContext

_INCOMPATIBLE_INSTANCE_SIGNATURE_MSG: Final = (
    'Instance callback is incompatible "{0}"; expected "{1}"'
)

_INSTANCE_RESTRICTION_MSG: Final = (
    'Instance "{0}" does not match original type "{1}"'
)

_DIFFERENT_INSTANCE_CALLS_MSG: Final = (
    'Found different typeclass ".instance" calls, use only "{0}"'
)


def check_type(
    instance_context: InstanceContext,
) -> bool:
    """
    We need to typecheck passed functions in order to build correct typeclasses.

    Please, see docs on each step.
    """
    return all([
        _check_typeclass_signature(
            instance_context.inferred_signature,
            instance_context.instance_signature,
            instance_context.ctx,
        ),
        _check_instance_type(
            instance_context.inferred_signature,
            instance_context.instance_signature,
            instance_context.ctx,
        ),
        _check_same_typeclass(instance_context.fullname, instance_context.ctx),
    ])


def _check_typeclass_signature(
    typeclass_signature: CallableType,
    instance_signature: CallableType,
    ctx: MethodContext,
) -> bool:
    """
    Checks that instance signature is compatible with.

    We use contravariant on arguments and covariant on return type logic here.
    What does this mean?

    Let's say that you have this typeclass signature:

    .. code:: python

      class A: ...
      class B(A): ...
      class C(B): ...

      @typeclass
      def some(instance, arg: B) -> B: ...

    What instance signatures will be compatible?

    .. code:: python

      (instance: ..., arg: B) -> B: ...
      (instance: ..., arg: A) -> C: ...

    But, any other cases will raise an error.

    .. note::
        We don't check instance types here at all,
        we replace it with ``Any``.
        See special function, where we check instance type.

    """
    simplified_typeclass_signature = typeclass_signature.copy_modified(
        arg_types=[
            AnyType(TypeOfAny.implementation_artifact),
            *typeclass_signature.arg_types[1:],
        ],
    )
    simplified_instance_signature = instance_signature.copy_modified(
        arg_types=[
            AnyType(TypeOfAny.implementation_artifact),
            *instance_signature.arg_types[1:],
        ],
    )
    signature_check = is_subtype(
        simplified_instance_signature,
        simplified_typeclass_signature,
    )
    if not signature_check:
        ctx.api.fail(
            _INCOMPATIBLE_INSTANCE_SIGNATURE_MSG.format(
                instance_signature,
                typeclass_signature.copy_modified(arg_types=[
                    instance_signature.arg_types[0],  # Better error message
                    *typeclass_signature.arg_types[1:],
                ]),
            ),
            ctx.context,
        )
    return signature_check


def _check_instance_type(
    typeclass_signature: CallableType,
    instance_signature: CallableType,
    ctx: MethodContext,
) -> bool:
    """
    Checks instance type, helpful when typeclass has type restrictions.

    We use covariant logic on instance type.
    What does this mean?

    .. code:: python

      class A: ...
      class B(A): ...
      class C(B): ...

      @typeclass
      def some(instance: B): ...

    What can we use on instance callbacks?

    .. code:: python

      @some.instance(B)
      def _some_b(instance: B):
          ...

      @some.instance(C)
      def _some_c(instance: C):
          ...

    Any types that are not subtypes of ``B`` will raise a type error.
    """
    instance_check = is_subtype(
        instance_signature.arg_types[0],
        typeclass_signature.arg_types[0],
    )
    if not instance_check:
        ctx.api.fail(
            _INSTANCE_RESTRICTION_MSG.format(
                instance_signature.arg_types[0],
                typeclass_signature.arg_types[0],
            ),
            ctx.context,
        )
    return instance_check


def _check_same_typeclass(
    fullname: str,
    ctx: MethodContext,
) -> bool:
    """
    Checks that only one typeclass can be referenced in all of decorators.

    If we have multiple decorators on a function,
    it is not safe to assume
    that we have ``.instance`` calls from the same typeclass.
    We don't want this:

    .. code:: python

      @some.instance(str)
      @other.instance(int)
      def some(instance: Union[str, int]) -> None:
          ...

    We don't allow this way of instance definition.
    See "FAQ" in docs for more information.
    """
    if not inference.all_same_instance_calls(fullname, ctx):
        ctx.api.fail(
            _DIFFERENT_INSTANCE_CALLS_MSG.format(fullname),
            ctx.context,
        )
        return False
    return True
