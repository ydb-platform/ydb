from typing import NamedTuple, Optional

from mypy.plugin import MethodContext
from mypy.types import CallableType, TupleType
from mypy.types import Type as MypyType
from typing_extensions import final

from classes.contrib.mypy.typeops import inference


@final
class _InferredArgs(NamedTuple):
    """Represents our argument inference result."""

    exact_type: Optional[MypyType]
    protocol: Optional[MypyType]
    delegate: Optional[MypyType]

    @classmethod
    def build(cls, passed_args: TupleType) -> '_InferredArgs':
        exact_type, protocol, delegate = passed_args.items
        return _InferredArgs(
            _infer_type_arg(exact_type),
            _infer_type_arg(protocol),
            _infer_type_arg(delegate),
        )


@final
class InstanceContext(NamedTuple):
    """
    Instance definition context.

    We use it to store all important types and data in one place
    to help with validation and type manipulations.
    """

    # Signatures:
    typeclass_signature: CallableType
    instance_signature: CallableType
    inferred_signature: CallableType

    # Instance and inferred types:
    instance_type: MypyType
    inferred_type: MypyType

    # Arguments:
    passed_args: TupleType
    inferred_args: _InferredArgs

    # Meta:
    fullname: str
    associated_type: MypyType

    # Mypy context:
    ctx: MethodContext

    @classmethod  # noqa: WPS211
    def build(  # noqa: WPS211
        # It has a lot of arguments, but I don't see how I can simply it.
        # I don't want to add steps or intermediate types.
        # It is okay for this method to have a lot arguments,
        # because it store a lot of data.
        cls,
        typeclass_signature: CallableType,
        instance_signature: CallableType,
        passed_args: TupleType,
        associated_type: MypyType,
        fullname: str,
        ctx: MethodContext,
    ) -> 'InstanceContext':
        """
        Builds instance context.

        It also infers several missing parts from the present data.
        Like real instance signature and "ideal" inferred type.
        """
        inferred_type = inference.infer_instance_type_from_context(
            passed_args=passed_args,
            fullname=fullname,
            ctx=ctx,
        )
        inferred_signature = inference.try_to_apply_generics(
            signature=typeclass_signature,
            instance_type=instance_signature.arg_types[0],
            ctx=ctx,
        )

        return InstanceContext(
            typeclass_signature=typeclass_signature,
            instance_signature=instance_signature,
            inferred_signature=inferred_signature,
            instance_type=instance_signature.arg_types[0],
            inferred_type=inferred_type,
            passed_args=passed_args,
            inferred_args=_InferredArgs.build(passed_args),
            associated_type=associated_type,
            fullname=fullname,
            ctx=ctx,
        )


def _infer_type_arg(type_arg: MypyType) -> Optional[MypyType]:
    inferred = inference.type_obj(type_arg)
    return inferred if type_arg is not inferred else None
