import functools
from typing import Any, Dict, List, Optional, Sequence, Type, Union

import mongoengine as me
import starlette_admin.fields as sa
from bson import ObjectId
from mongoengine.base import BaseDocument
from mongoengine.errors import DoesNotExist, ValidationError
from mongoengine.fields import GridFSProxy
from mongoengine.queryset import QNode
from starlette.datastructures import UploadFile
from starlette.requests import Request
from starlette_admin.contrib.mongoengine.converters import (
    BaseMongoEngineModelConverter,
    ModelConverter,
)
from starlette_admin.contrib.mongoengine.fields import FileField, ImageField
from starlette_admin.contrib.mongoengine.helpers import (
    Q,
    build_order_clauses,
    normalize_list,
    resolve_deep_query,
)
from starlette_admin.exceptions import FormValidationError
from starlette_admin.helpers import not_none, prettify_class_name, slugify_class_name
from starlette_admin.views import BaseModelView


class ModelView(BaseModelView):
    def __init__(
        self,
        document: Type[me.Document],
        icon: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        identity: Optional[str] = None,
        converter: Optional[BaseMongoEngineModelConverter] = None,
    ):
        self.document = document
        self.identity = (
            identity or self.identity or slugify_class_name(self.document.__name__)
        )
        self.label = (
            label or self.label or prettify_class_name(self.document.__name__) + "s"
        )
        self.name = name or self.name or prettify_class_name(self.document.__name__)
        self.icon = icon
        self.pk_attr = "id"
        if self.fields is None or len(self.fields) == 0:
            self.fields = document._fields_ordered
        self.fields = (converter or ModelConverter()).convert_fields_list(
            fields=self.fields, model=self.document
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

    async def count(
        self,
        request: Request,
        where: Union[Dict[str, Any], str, None] = None,
    ) -> int:
        q = await self._build_query(request, where)
        return self.document.objects(q).count()

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: Union[Dict[str, Any], str, None] = None,
        order_by: Optional[List[str]] = None,
    ) -> Sequence[Any]:
        q = await self._build_query(request, where)
        objs = self.document.objects(q).order_by(*build_order_clauses(order_by or []))
        if limit > 0:
            return objs[skip : skip + limit]
        return objs[skip:]

    async def find_by_pk(self, request: Request, pk: Any) -> Optional[me.Document]:
        try:
            return self.document.objects(id=pk).get()
        except (DoesNotExist, ValidationError):
            return None

    async def find_by_pks(
        self, request: Request, pks: List[Any]
    ) -> Sequence[me.Document]:
        return self.document.objects(id__in=pks)

    async def create(self, request: Request, data: Dict[str, Any]) -> Any:
        try:
            obj = await self._populate_obj(request, self.document(), data)
            await self.before_create(request, data, obj)
            obj.save()
            await self.after_create(request, obj)
            return obj
        except Exception as e:
            self.handle_exception(e)

    async def edit(self, request: Request, pk: Any, data: Dict[str, Any]) -> Any:
        try:
            obj = await self.find_by_pk(request, pk)
            obj = await self._populate_obj(request, obj, data, True)
            await self.before_edit(request, data, obj)
            obj.save()
            await self.after_edit(request, obj)
            return obj
        except Exception as e:
            self.handle_exception(e)

    async def _populate_obj(  # noqa: C901
        self,
        request: Request,
        obj: me.Document,
        data: Dict[str, Any],
        is_edit: bool = False,
        document: Optional[BaseDocument] = None,
        fields: Optional[Sequence[sa.BaseField]] = None,
    ) -> me.Document:
        if document is None:
            document = self.document
        if fields is None:
            fields = self.get_fields_list(request, request.state.action)
        for field in fields:
            name, value = field.name, data.get(field.name, None)
            me_field = getattr(document, name)
            if isinstance(field, (FileField, ImageField)):
                proxy: GridFSProxy = getattr(obj, name)
                value, should_be_deleted = not_none(value)
                if should_be_deleted:
                    proxy.delete()
                elif isinstance(value, UploadFile):
                    if proxy.grid_id is not None:
                        proxy.replace(
                            value.file,
                            filename=value.filename,
                            content_type=value.content_type,
                        )
                    else:
                        proxy.put(
                            value.file,
                            filename=value.filename,
                            content_type=value.content_type,
                        )

            elif isinstance(me_field, me.EmbeddedDocumentField) and value is not None:
                assert isinstance(field, sa.CollectionField)
                old_value = getattr(obj, name, None)
                if old_value is None:
                    old_value = me_field.document_type()
                setattr(
                    obj,
                    name,
                    await self._populate_obj(
                        request,
                        old_value,
                        value,
                        is_edit,
                        me_field.document_type,
                        field.fields,
                    ),
                )
            elif (
                isinstance(me_field, me.ListField)
                and isinstance(me_field.field, me.EmbeddedDocumentField)
                and value is not None
            ):
                assert isinstance(field, sa.ListField) and isinstance(
                    field.field, sa.CollectionField
                )
                old_value = getattr(obj, name, [])
                if len(old_value) < len(value):
                    old_value.extend(
                        [
                            me_field.field.document_type()
                            for _ in range(len(value) - len(old_value))
                        ]
                    )
                setattr(
                    obj,
                    name,
                    [
                        await self._populate_obj(
                            request,
                            old_value[idx],
                            _val,
                            is_edit,
                            me_field.field.document_type,
                            field.field.fields,
                        )
                        for idx, _val in enumerate(value)
                    ],
                )
            elif isinstance(field, sa.HasOne) and value is not None:
                setattr(obj, name, ObjectId(value))
            elif isinstance(field, sa.HasMany) and value is not None:
                setattr(obj, name, [ObjectId(v) for v in value])
            else:
                setattr(obj, name, value)
        return obj

    async def delete(self, request: Request, pks: List[Any]) -> Optional[int]:
        objs = self.document.objects(id__in=pks)
        for obj in objs:
            await self.before_delete(request, obj)
        deleted_count = objs.delete()
        for obj in objs:
            await self.after_delete(request, obj)
        return deleted_count

    def handle_exception(self, exc: Exception) -> None:
        if isinstance(exc, ValidationError):
            raise FormValidationError(exc.to_dict())
        raise exc  # pragma: no cover

    async def _build_query(
        self, request: Request, where: Union[Dict[str, Any], str, None] = None
    ) -> QNode:
        if where is None:
            return Q.empty()
        if isinstance(where, dict):
            return resolve_deep_query(where, self.document)
        return await self.build_full_text_search_query(request, where)

    async def build_full_text_search_query(self, request: Request, term: str) -> QNode:
        queries = []
        for field in self.get_fields_list(request):
            if (
                field.searchable
                and field.name != "id"
                and type(field)
                in [
                    sa.StringField,
                    sa.TextAreaField,
                    sa.EmailField,
                    sa.URLField,
                    sa.PhoneField,
                    sa.ColorField,
                ]
            ):
                queries.append(Q(field.name, term, "icontains"))
        return (
            functools.reduce(lambda q1, q2: q1 | q2, queries) if queries else Q.empty()
        )
