from typing import Union
import sqlalchemy as sa
import ydb

from .base import (
    BaseYqlCompiler,
    BaseYqlDDLCompiler,
    BaseYqlIdentifierPreparer,
    BaseYqlTypeCompiler,
)


class YqlTypeCompiler(BaseYqlTypeCompiler):
    # We use YDB Double for sa.Float for compatibility with old dialect version
    def visit_FLOAT(self, type_: sa.FLOAT, **kw):
        return "DOUBLE"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        if isinstance(type_, sa.Float):
            ydb_type = ydb.PrimitiveType.Double
            if is_optional:
                return ydb.OptionalType(ydb_type)
            return ydb_type

        return super().get_ydb_type(type_, is_optional)


class YqlIdentifierPreparer(BaseYqlIdentifierPreparer):
    ...


class YqlCompiler(BaseYqlCompiler):
    _type_compiler_cls = YqlTypeCompiler

    def visit_upsert(self, insert_stmt, **kw):
        return self.visit_insert(insert_stmt, **kw).replace("INSERT", "UPSERT", 1)


class YqlDDLCompiler(BaseYqlDDLCompiler):
    ...
