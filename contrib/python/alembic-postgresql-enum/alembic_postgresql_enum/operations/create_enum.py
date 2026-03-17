import alembic
from alembic.autogenerate.api import AutogenContext

from .enum_lifecycle_base import EnumLifecycleOp


class CreateEnumOp(EnumLifecycleOp):
    operation_name = "create_enum"

    def reverse(self):
        from .drop_enum import DropEnumOp

        return DropEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(CreateEnumOp)
def render_create_enum_op(autogen_context: AutogenContext, op: CreateEnumOp):
    assert autogen_context.dialect is not None
    sqlalchemy_module_prefix = autogen_context.opts.get("sqlalchemy_module_prefix", "sa.")
    alembic_module_prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    if op.schema != autogen_context.dialect.default_schema_name:
        return f"""
            {sqlalchemy_module_prefix}Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}', schema='{op.schema}').create({alembic_module_prefix}get_bind())
            """.strip()

    return f"""
        {sqlalchemy_module_prefix}Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create({alembic_module_prefix}get_bind())
        """.strip()
