from typing import Any, Optional, Union

from sqlalchemy import JSON, Dialect, TypeDecorator, null
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.sql.type_api import TypeEngine, to_instance

from ...common import TypeHint
from ...morphing.facade.retort import AdornedRetort

_SQL_NULL = null()
_JSON_NULL = JSON.NULL


class AdaptixJSON(TypeDecorator):
    impl = JSON
    cache_ok = True

    def __init__(
        self,
        retort: AdornedRetort,
        tp: TypeHint,
        *,
        impl: Optional[TypeEngine] = None,
    ):
        super().__init__()
        self.retort = retort
        self.tp = tp
        self._custom_impl = impl
        self._loader = retort.get_loader(tp)
        self._dumper = retort.get_dumper(tp)
        if impl is not None:
            self.impl = impl  # type: ignore[assignment]

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if self._custom_impl is not None:
            return self._custom_impl
        return to_instance(self._load_default_dialect_impl(dialect))

    def _load_default_dialect_impl(self, dialect: Dialect) -> Union[TypeEngine[Any], type[TypeEngine[Any]]]:
        if isinstance(dialect, PGDialect):
            return postgresql.JSONB
        return JSON

    def process_bind_param(self, value, dialect):
        if value is None or value is _SQL_NULL or value is _JSON_NULL:
            return value
        return self._dumper(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return self._loader(value)

    def coerce_compared_value(self, op, value):
        return self.impl.coerce_compared_value(op, value)
