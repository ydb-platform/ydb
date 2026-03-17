from mypy.nodes import SymbolTableNode, TypeInfo
from mypy.plugin import AnalyzeTypeContext
from mypy.subtypes import is_subtype
from mypy.types import Instance
from typing_extensions import Final

# Messages:

_ARG_SUBTYPE_MSG: Final = (
    'Type argument "{0}" of "{1}" must be a subtype of "{2}"'
)


def check_type(
    instance: Instance,
    associated_type_node: SymbolTableNode,
    ctx: AnalyzeTypeContext,
) -> bool:
    """Checks how ``Supports`` is used."""
    return all([
        _check_instance_args(instance, associated_type_node, ctx),
    ])


def _check_instance_args(
    instance: Instance,
    associated_type_node: SymbolTableNode,
    ctx: AnalyzeTypeContext,
) -> bool:
    assert isinstance(associated_type_node.node, TypeInfo)
    associated_type = Instance(associated_type_node.node, [])

    is_correct = True
    for type_arg in instance.args:
        if not is_subtype(type_arg, associated_type):
            is_correct = False
            ctx.api.fail(
                _ARG_SUBTYPE_MSG.format(
                    type_arg,
                    instance.type.name,
                    associated_type,
                ),
                ctx.context,
            )
    return is_correct
