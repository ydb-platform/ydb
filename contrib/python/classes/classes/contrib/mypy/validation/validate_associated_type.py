from mypy.plugin import MethodContext
from mypy.typeops import get_type_vars
from mypy.types import CallableType, Instance
from typing_extensions import Final

# Messages:

_WRONG_SUBCLASS_MSG: Final = (
    'Single direct subclass of "{0}" required; got "{1}"'
)

_TYPE_REUSE_MSG: Final = (
    'AssociatedType "{0}" must not be reused, originally associated with "{1}"'
)

_GENERIC_MISSMATCH_MSG: Final = (
    'Generic type "{0}" with "{1}" type arguments does not match ' +
    'generic instance declaration "{2}" with "{3}" type arguments'
)

_REDUNDANT_BODY_MSG: Final = 'Associated types must not have bodies'


def check_type(
    associated_type: Instance,
    associated_type_fullname: str,
    typeclass: Instance,
    ctx: MethodContext,
) -> bool:
    """Checks passed ``AssociatedType`` instance."""
    return all([
        _check_base_class(associated_type, associated_type_fullname, ctx),
        _check_body(associated_type, ctx),
        _check_type_reuse(associated_type, typeclass, ctx),
        _check_generics(associated_type, typeclass, ctx),
        # TODO: we also need to check type vars used on definition:
        # no values, no bounds (?)
    ])


def _check_base_class(
    associated_type: Instance,
    associated_type_fullname: str,
    ctx: MethodContext,
) -> bool:
    bases = associated_type.type.bases
    has_correct_base = (
        len(bases) == 1 and
        associated_type_fullname == bases[0].type.fullname
    )
    if not has_correct_base:
        ctx.api.fail(
            _WRONG_SUBCLASS_MSG.format(
                associated_type_fullname,
                associated_type,
            ),
            ctx.context,
        )
    return has_correct_base


def _check_body(
    associated_type: Instance,
    ctx: MethodContext,
) -> bool:
    if associated_type.type.names:
        ctx.api.fail(_REDUNDANT_BODY_MSG, ctx.context)
        return False
    return True


def _check_type_reuse(
    associated_type: Instance,
    typeclass: Instance,
    ctx: MethodContext,
) -> bool:
    fullname = getattr(typeclass.args[3], 'value', None)
    metadata = associated_type.type.metadata.setdefault('classes', {})

    has_reuse = (
        fullname is not None and
        'typeclass' in metadata and
        metadata['typeclass'] != fullname
    )
    if has_reuse:
        ctx.api.fail(
            _TYPE_REUSE_MSG.format(associated_type.type.fullname, fullname),
            ctx.context,
        )

    metadata['typeclass'] = fullname
    return has_reuse


def _check_generics(
    associated_type: Instance,
    typeclass: Instance,
    ctx: MethodContext,
) -> bool:
    assert isinstance(typeclass.args[1], CallableType)
    instance_decl = typeclass.args[1].arg_types[0]
    if not isinstance(instance_decl, Instance):
        return True

    # We use `get_type_vars` here to exclude cases like `Supports[ToJson]`
    # and `List[int]` from validation:
    instance_args = get_type_vars(instance_decl)
    if len(instance_args) != len(associated_type.type.type_vars):
        ctx.api.fail(
            _GENERIC_MISSMATCH_MSG.format(
                associated_type.type.fullname,
                len(associated_type.type.type_vars),
                instance_decl,
                len(instance_args),
            ),
            ctx.context,
        )
        return False
    return True
