import functools
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import bson.errors
import starlette_admin.fields as sa
from beanie import Document, Link, PydanticObjectId
from beanie.odm.operators.find import BaseFindOperator
from beanie.operators import Or, RegEx, Text
from pydantic import ValidationError
from starlette.requests import Request
from starlette_admin._types import RequestAction
from starlette_admin.contrib.beanie.converters import BeanieModelConverter
from starlette_admin.contrib.beanie.helpers import (
    BeanieLogicalOperator,
    build_order_clauses,
    is_link_type,
    is_list_of_links_type,
    normalize_field_list,
    resolve_deep_query,
)
from starlette_admin.helpers import (
    not_none,
    prettify_class_name,
    pydantic_error_to_form_validation_errors,
    slugify_class_name,
)
from starlette_admin.views import BaseModelView

T = TypeVar("T", bound=Document)


class ModelView(BaseModelView, Generic[T]):
    full_text_override_order_by: bool = False

    def __init__(
        self,
        document: Type[T],
        icon: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        identity: Optional[str] = None,
        converter: Optional[BeanieModelConverter] = None,
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
        self.has_full_text_index: Optional[bool] = None

        self.fields_pydantic = list(document.model_fields.items())

        self.exclude_fields_from_create = [
            *self.exclude_fields_from_create,
            "revision_id",
        ]
        self.exclude_fields_from_edit = [
            *self.exclude_fields_from_edit,
            "revision_id",
            self.pk_attr,
        ]
        self.exclude_fields_from_list = [*self.exclude_fields_from_list, "revision_id"]
        self.exclude_fields_from_detail = [
            *self.exclude_fields_from_detail,
            "revision_id",
        ]

        self.exclude_fields_from_create = normalize_field_list(
            field_list=self.exclude_fields_from_create
        )
        self.exclude_fields_from_edit = normalize_field_list(
            field_list=self.exclude_fields_from_edit
        )
        self.exclude_fields_from_list = normalize_field_list(
            field_list=self.exclude_fields_from_list
        )
        self.exclude_fields_from_detail = normalize_field_list(
            field_list=self.exclude_fields_from_detail
        )

        if self.fields is None or len(self.fields) == 0:
            self.fields = document.model_fields.keys()

        self.fields = list(
            (converter or BeanieModelConverter()).convert_fields_list(
                fields=self.fields, model=self.document
            )
        )

        super().__init__()

    async def check_full_text_index(self) -> None:
        indexes = await self.document.get_motor_collection().index_information()
        for index in indexes.values():
            if any(field_type == "text" for _, field_type in index["key"]):
                self.has_full_text_index = True
                return
        self.has_full_text_index = False

    async def _build_query(
        self, request: Request, where: Union[Dict[str, Any], str, None] = None
    ) -> Tuple[BaseFindOperator, bool]:
        if where is None:
            return BeanieLogicalOperator(), False
        if isinstance(where, dict):
            return resolve_deep_query(where, self.document), False
        return await self.build_full_text_search_query(request, where)

    async def build_full_text_search_query(
        self, request: Request, term: str
    ) -> Tuple[Union[BaseFindOperator], bool]:
        # use a full text index if the collection has one,
        # otherwise use a combination of RegEx queries
        if self.has_full_text_index is None:
            await self.check_full_text_index()
        if self.has_full_text_index:
            return (
                Text(term, case_sensitive=False),
                True,
            )
        queries: List[BaseFindOperator] = []
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
                queries.append(RegEx(field.name, term, options="i"))
        if queries:
            return (
                functools.reduce(lambda q1, q2: Or(q1, q2), queries),
                False,
            )
        return BeanieLogicalOperator(), False

    async def count(
        self, request: Request, where: Union[Dict[str, Any], str, None] = None
    ) -> int:
        query, _ = await self._build_query(request, where)
        if not bool(query):
            return await self.document.get_motor_collection().estimated_document_count()
        result = self.document.find(query.query)
        return await result.count()

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: Union[Dict[str, Any], str, None] = None,
        order_by: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[T]:
        if not where:
            where = {}
        query, is_full_text_query = await self._build_query(request, where)
        result = self.document.find(query.query, **kwargs)

        # handle order_by
        if is_full_text_query and self.full_text_override_order_by:
            result = result.sort(("score", {"$meta": "textScore"}))  # type: ignore
        elif order_by:
            result = result.sort(build_order_clauses(order_by))
        return await result.skip(skip).limit(limit).to_list()

    async def find_by_pk(self, request: Request, pk: PydanticObjectId) -> Optional[T]:
        if not isinstance(pk, PydanticObjectId):
            try:
                pk = PydanticObjectId(pk)
            except bson.errors.InvalidId:
                return None

        return await self.document.get(pk, fetch_links=True, nesting_depth=1)

    async def find_by_pks(
        self, request: Request, pks: Iterable[PydanticObjectId]
    ) -> List[T]:
        docs = []
        for pk in pks:
            doc = await self.document.get(pk)
            if doc:
                docs.append(doc)
        return docs

    async def get_pk_value(self, request: Request, obj: Any) -> Any:
        if isinstance(obj, Link):
            return getattr(obj.ref, not_none(self.pk_attr))

        return getattr(obj, not_none(self.pk_attr))

    async def get_serialized_pk_value(self, request: Request, obj: Any) -> Any:
        return str(await self.get_pk_value(request, obj))

    async def create(self, request: Request, data: dict) -> Any:
        data = {
            k: v
            for k, v in data.items()
            if k not in self.get_fields_list(request, action=RequestAction.CREATE)
        }
        try:
            doc = self.document(**data)
            await self.before_create(request, data, doc)
            doc = await doc.create()
            await self.after_create(request, doc)
            return doc
        except Exception as e:
            return self.handle_exception(e)

    async def edit(self, request: Request, pk: PydanticObjectId, data: dict) -> Any:
        doc: Union[Document, None] = await self.document.get(pk)
        assert doc is not None, "Document not found"
        data = {
            k: v
            for k, v in data.items()
            if k not in self.get_fields_list(request, action=RequestAction.EDIT)
        }
        try:

            for key in data:
                if key in self.document.model_fields:
                    field_type = self.document.model_fields[key].annotation
                    if is_link_type(field_type):
                        if not isinstance(data[key], PydanticObjectId):
                            data[key] = (
                                None if not data[key] else PydanticObjectId(data[key])
                            )
                    elif is_list_of_links_type(field_type):
                        data[key] = [
                            PydanticObjectId(item) for item in data[key] if item
                        ]

                    setattr(doc, key, data[key])

            # ensure doc still passes validation
            validated_doc: T = self.document.model_validate(doc.model_dump())

            await self.before_edit(request, data=data, obj=validated_doc)
            updated_doc = await validated_doc.replace()
            await self.after_edit(request, updated_doc)

            return updated_doc

        except Exception as e:
            return self.handle_exception(e)

    async def delete(self, request: Request, pks: List[Any]) -> Optional[int]:
        cnt = 0
        for pk in pks:
            value = await self.find_by_pk(request, pk)
            if value is not None:
                await self.before_delete(request, value)
                await value.delete()
                await self.after_delete(request, value)
                cnt += 1
        return cnt

    def handle_exception(self, exc: Exception) -> None:
        if isinstance(exc, ValidationError):
            raise pydantic_error_to_form_validation_errors(exc) from exc
        raise exc
