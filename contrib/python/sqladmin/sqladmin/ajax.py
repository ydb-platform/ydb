from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import String, cast, inspect, or_, select

from sqladmin.helpers import get_object_identifier, get_primary_keys

if TYPE_CHECKING:
    from sqladmin.models import ModelView


DEFAULT_PAGE_SIZE = 10


class QueryAjaxModelLoader:
    def __init__(
        self,
        name: str,
        model: type,
        model_admin: "ModelView",
        **options: Any,
    ):
        self.name = name
        self.model = model
        self.model_admin = model_admin
        self.fields = options.get("fields", {})
        self.order_by = options.get("order_by")
        self.limit = options.get("limit", DEFAULT_PAGE_SIZE)

        pks = get_primary_keys(self.model)
        self.pk = pks[0] if len(pks) == 1 else None

        if not self.fields:
            raise ValueError(
                "AJAX loading requires `fields` to be specified for "
                f"{self.model}.{self.name}"
            )

        self._cached_fields = self._process_fields()

    def _process_fields(self) -> list:
        remote_fields = []

        for field in self.fields:
            if isinstance(field, str):
                attr = getattr(self.model, field, None)

                if not attr:
                    raise ValueError(f"{self.model}.{field} does not exist.")

                remote_fields.append(attr)
            else:
                remote_fields.append(field)

        return remote_fields

    def format(self, model: type) -> dict[str, Any]:
        if not model:
            return {}

        return {"id": str(get_object_identifier(model)), "text": str(model)}

    async def get_list(self, term: str) -> list[Any]:
        stmt = select(self.model)

        # no type casting to string if a ColumnAssociationProxyInstance is given
        filters = [
            cast(field, String).ilike("%%%s%%" % term) for field in self._cached_fields
        ]

        stmt = stmt.filter(or_(*filters))

        if self.order_by:
            if isinstance(self.order_by, list):
                for o in self.order_by:
                    stmt = stmt.order_by(o)
            else:
                stmt = stmt.order_by(self.order_by)

        stmt = stmt.limit(self.limit)
        result = await self.model_admin._run_query(stmt)
        return result


def create_ajax_loader(
    *,
    model_admin: "ModelView",
    name: str,
    options: dict,
) -> QueryAjaxModelLoader:
    mapper = inspect(model_admin.model)

    try:
        attr = mapper.relationships[name]
    except KeyError as exc:
        raise ValueError(f"{model_admin.model}.{name} is not a relation.") from exc

    remote_model = attr.mapper.class_
    return QueryAjaxModelLoader(name, remote_model, model_admin, **options)
