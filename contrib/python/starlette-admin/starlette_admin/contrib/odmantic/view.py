import re
from functools import partial
from typing import Any, Dict, List, Optional, Sequence, Type, Union

import anyio
from bson import ObjectId
from odmantic import Model, query
from odmantic.query import QueryExpression
from odmantic.session import AIOSession, SyncSession
from pydantic import ValidationError
from starlette.requests import Request
from starlette_admin.contrib.odmantic.converters import ModelConverter
from starlette_admin.contrib.odmantic.helpers import (
    normalize_list,
    resolve_deep_query,
    resolve_proxy,
)
from starlette_admin.fields import (
    BaseField,
    CollectionField,
    ColorField,
    EmailField,
    HasMany,
    HasOne,
    ListField,
    PhoneField,
    StringField,
    TextAreaField,
    URLField,
)
from starlette_admin.helpers import (
    prettify_class_name,
    pydantic_error_to_form_validation_errors,
    slugify_class_name,
)
from starlette_admin.views import BaseModelView


class ModelView(BaseModelView):
    def __init__(
        self,
        model: Type[Model],
        icon: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        identity: Optional[str] = None,
        converter: Optional[ModelConverter] = None,
    ):
        self.model = model
        self.identity = (
            identity or self.identity or slugify_class_name(self.model.__name__)
        )
        self.label = (
            label or self.label or prettify_class_name(self.model.__name__) + "s"
        )
        self.name = name or self.name or prettify_class_name(self.model.__name__)
        self.icon = icon
        self.pk_attr = "id"
        if self.fields is None or len(self.fields) == 0:
            _all_list = list(model.__odm_fields__.keys())
            self.fields = (
                _all_list[-1:] + _all_list[:-1]  # type: ignore
            )  # Move 'id' to first position.
        self.fields = (converter or ModelConverter()).convert_fields_list(
            fields=self.fields, model=self.model
        )
        self.exclude_fields_from_list = normalize_list(self.exclude_fields_from_list)  # type: ignore
        self.exclude_fields_from_detail = normalize_list(self.exclude_fields_from_detail)  # type: ignore
        self.exclude_fields_from_create = normalize_list(self.exclude_fields_from_create)  # type: ignore
        self.exclude_fields_from_edit = normalize_list(self.exclude_fields_from_edit)  # type: ignore
        self.searchable_fields = normalize_list(self.searchable_fields)
        self.sortable_fields = normalize_list(self.sortable_fields)
        self.export_fields = normalize_list(self.export_fields)
        self.fields_default_sort = normalize_list(
            self.fields_default_sort, is_default_sort_list=True
        )
        super().__init__()

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: Union[Dict[str, Any], str, None] = None,
        order_by: Optional[List[str]] = None,
    ) -> Sequence[Any]:
        session: Union[AIOSession, SyncSession] = request.state.session
        q = await self._build_query(request, where)
        o = await self._build_order_clauses([] if order_by is None else order_by)
        if isinstance(session, AIOSession):
            return await session.find(
                self.model,
                q,
                sort=o,
                skip=skip,
                limit=limit,
            )
        return await anyio.to_thread.run_sync(
            partial(  # type: ignore
                session.find,
                self.model,
                q,
                sort=o,
                skip=skip,
                limit=limit,
            )
        )

    async def count(
        self, request: Request, where: Union[Dict[str, Any], str, None] = None
    ) -> int:
        session: Union[AIOSession, SyncSession] = request.state.session
        q = await self._build_query(request, where)
        if isinstance(session, AIOSession):
            return await session.count(self.model, q)
        return await anyio.to_thread.run_sync(session.count, self.model, q)

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        session: Union[AIOSession, SyncSession] = request.state.session
        if isinstance(session, AIOSession):
            return await session.find_one(self.model, self.model.id == ObjectId(pk))
        return await anyio.to_thread.run_sync(
            session.find_one, self.model, self.model.id == ObjectId(pk)
        )

    async def find_by_pks(self, request: Request, pks: List[Any]) -> Sequence[Any]:
        pks = list(map(ObjectId, pks))
        session: Union[AIOSession, SyncSession] = request.state.session
        if isinstance(session, AIOSession):
            return await session.find(self.model, self.model.id.in_(pks))  # type: ignore
        return list(await anyio.to_thread.run_sync(session.find, self.model, self.model.id.in_(pks)))  # type: ignore

    async def create(self, request: Request, data: Dict) -> Any:
        session: Union[AIOSession, SyncSession] = request.state.session
        data = await self._arrange_data(request, data)
        try:
            obj = self.model(**data)
            await self.before_create(request, data, obj)
            if isinstance(session, AIOSession):
                await session.save(obj)
            else:
                await anyio.to_thread.run_sync(session.save, obj)
            await self.after_create(request, obj)
            return obj
        except Exception as e:
            self.handle_exception(e)

    async def edit(self, request: Request, pk: Any, data: Dict[str, Any]) -> Any:
        session: Union[AIOSession, SyncSession] = request.state.session
        data = await self._arrange_data(request, data, is_edit=True)
        try:
            obj = await self.find_by_pk(request, pk)
            obj.update(data)
            await self.before_edit(request, data, obj)
            if isinstance(session, AIOSession):
                obj = await session.save(obj)
            else:
                obj = await anyio.to_thread.run_sync(session.save, obj)
            await self.after_edit(request, obj)
            return obj
        except Exception as e:
            self.handle_exception(e)

    async def delete(self, request: Request, pks: List[Any]) -> Optional[int]:
        objs = await self.find_by_pks(request, pks)
        pks = list(map(ObjectId, pks))
        for obj in objs:
            await self.before_delete(request, obj)
        session: Union[AIOSession, SyncSession] = request.state.session
        if isinstance(session, AIOSession):
            deleted_count = await session.remove(self.model, self.model.id.in_(pks))  # type: ignore
        else:
            deleted_count = await anyio.to_thread.run_sync(session.remove, self.model, self.model.id.in_(pks))  # type: ignore
        for obj in objs:
            await self.after_delete(request, obj)
        return deleted_count

    def handle_exception(self, exc: Exception) -> None:
        if isinstance(exc, ValidationError):
            raise pydantic_error_to_form_validation_errors(exc)
        raise exc  # pragma: no cover

    async def _arrange_data(
        self,
        request: Request,
        data: Dict[str, Any],
        is_edit: bool = False,
        fields: Optional[Sequence[BaseField]] = None,
    ) -> Dict[str, Any]:
        arranged_data: Dict[str, Any] = {}
        if fields is None:
            fields = self.get_fields_list(request, request.state.action)
        for field in fields:
            name, value = field.name, data.get(field.name, None)
            if isinstance(field, CollectionField) and value is not None:
                arranged_data[name] = await self._arrange_data(
                    request,
                    value,
                    is_edit,
                    field.get_fields_list(request, request.state.action),
                )
            elif (
                isinstance(field, ListField)
                and isinstance(field.field, CollectionField)
                and value is not None
            ):
                arranged_data[name] = [
                    await self._arrange_data(
                        request,
                        v,
                        is_edit,
                        field.field.get_fields_list(request, request.state.action),
                    )
                    for v in value
                ]
            elif isinstance(field, HasOne) and value is not None:
                foreign_model = self._find_foreign_model(field.identity)  # type: ignore
                arranged_data[name] = await foreign_model.find_by_pk(request, value)
            elif isinstance(field, HasMany) and value is not None:  # pragma: no cover
                """
                Note: Currently, ODMantic does not support mapped multi-references yet.
                Read more at https://art049.github.io/odmantic/modeling/#referenced-models
                """
                arranged_data[name] = [ObjectId(v) for v in value]
            else:
                arranged_data[name] = value
        return arranged_data

    async def _build_query(
        self, request: Request, where: Union[Dict[str, Any], str, None] = None
    ) -> Any:
        if where is None:
            return {}
        if isinstance(where, dict):
            return resolve_deep_query(where, self.model)
        return await self.build_full_text_search_query(request, where)

    async def _build_order_clauses(self, order_list: List[str]) -> Any:
        clauses = []
        for value in order_list:
            key, order = value.strip().split(maxsplit=1)
            clause = resolve_proxy(self.model, key)
            if clause is not None:
                clauses.append(clause.desc() if order.lower() == "desc" else clause)
        return tuple(clauses) if len(clauses) > 0 else None

    async def build_full_text_search_query(
        self, request: Request, term: str
    ) -> QueryExpression:
        _list = []
        for field in self.get_fields_list(request):
            if (
                field.searchable
                and field.name != "id"
                and type(field)
                in [
                    StringField,
                    TextAreaField,
                    EmailField,
                    URLField,
                    PhoneField,
                    ColorField,
                ]
            ):
                _list.append(
                    getattr(self.model, field.name).match(
                        re.compile(
                            r"%s" % re.escape(term), re.IGNORECASE  # noqa: UP031
                        )
                    )
                )
        return query.or_(*_list) if len(_list) > 0 else QueryExpression({})
