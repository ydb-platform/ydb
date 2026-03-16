from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from typing import OrderedDict as OrderedDictType

from bson import DBRef, ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel
from pymongo import ASCENDING, IndexModel
from typing_extensions import get_args

from beanie.odm.enums import SortDirection
from beanie.odm.operators.find.comparison import (
    GT,
    GTE,
    LT,
    LTE,
    NE,
    Eq,
    In,
)
from beanie.odm.registry import DocsRegistry
from beanie.odm.utils.parsing import parse_obj
from beanie.odm.utils.pydantic import (
    IS_PYDANTIC_V2,
    get_field_type,
    get_model_fields,
    parse_object_as,
)

if IS_PYDANTIC_V2:
    from pydantic import (
        GetCoreSchemaHandler,
        GetJsonSchemaHandler,
        TypeAdapter,
    )
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema, core_schema
    from pydantic_core.core_schema import (
        ValidationInfo,
        simple_ser_schema,
    )
else:
    from pydantic.fields import ModelField  # type: ignore
    from pydantic.json import ENCODERS_BY_TYPE

if TYPE_CHECKING:
    from beanie.odm.documents import DocType

if IS_PYDANTIC_V2:
    plain_validator = (
        core_schema.with_info_plain_validator_function
        if hasattr(core_schema, "with_info_plain_validator_function")
        else core_schema.general_plain_validator_function
    )
else:

    def plain_validator(v):
        return v


@dataclass(frozen=True)
class IndexedAnnotation:
    _indexed: Tuple[int, Dict[str, Any]]


def Indexed(typ=None, index_type=ASCENDING, **kwargs: Any):
    """
    If `typ` is defined, returns a subclass of `typ` with an extra attribute
    `_indexed` as a tuple:
    - Index 0: `index_type` such as `pymongo.ASCENDING`
    - Index 1: `kwargs` passed to `IndexModel`
    When instantiated the type of the result will actually be `typ`.

    When `typ` is not defined, returns an `IndexedAnnotation` instance, to be
    used as metadata in `Annotated` fields.

    Example:
    ```py
    # Both fields would have the same behavior
    class MyModel(BaseModel):
        field1: Indexed(str, unique=True)
        field2: Annotated[str, Indexed(unique=True)]
    ```
    """
    if typ is None:
        return IndexedAnnotation(_indexed=(index_type, kwargs))

    class NewType(typ):
        _indexed = (index_type, kwargs)

        def __new__(cls, *args: Any, **kwargs: Any):
            return typ.__new__(typ, *args, **kwargs)

        if IS_PYDANTIC_V2:

            @classmethod
            def __get_pydantic_core_schema__(
                cls, _source_type: Any, _handler: GetCoreSchemaHandler
            ) -> core_schema.CoreSchema:
                custom_type = getattr(
                    typ, "__get_pydantic_core_schema__", None
                )
                if custom_type is not None:
                    return custom_type(_source_type, _handler)

                return core_schema.no_info_after_validator_function(
                    lambda v: v,
                    simple_ser_schema(typ.__name__),
                )

    NewType.__name__ = f"Indexed {typ.__name__}"
    return NewType


class PydanticObjectId(ObjectId):
    """
    Object Id field. Compatible with Pydantic.
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    if IS_PYDANTIC_V2:

        @classmethod
        def validate(cls, v, _: ValidationInfo):
            if isinstance(v, bytes):
                v = v.decode("utf-8")
            try:
                return PydanticObjectId(v)
            except (InvalidId, TypeError):
                raise ValueError("Id must be of type PydanticObjectId")

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            return core_schema.json_or_python_schema(
                python_schema=plain_validator(cls.validate),
                json_schema=plain_validator(
                    cls.validate,
                    metadata={
                        "pydantic_js_input_core_schema": core_schema.str_schema(
                            pattern="^[0-9a-f]{24}$",
                            min_length=24,
                            max_length=24,
                        )
                    },
                ),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda instance: str(instance), when_used="json"
                ),
            )

        @classmethod
        def __get_pydantic_json_schema__(
            cls,
            schema: core_schema.CoreSchema,
            handler: GetJsonSchemaHandler,  # type: ignore
        ) -> JsonSchemaValue:
            json_schema = handler(schema)
            json_schema.update(
                type="string",
                example="5eb7cf5a86d9755df3a6c593",
            )
            return json_schema

    else:

        @classmethod
        def validate(cls, v):
            if isinstance(v, bytes):
                v = v.decode("utf-8")
            try:
                return PydanticObjectId(v)
            except InvalidId:
                raise TypeError("Id must be of type PydanticObjectId")

        @classmethod
        def __modify_schema__(cls, field_schema):
            field_schema.update(
                type="string",
                example="5eb7cf5a86d9755df3a6c593",
            )


if not IS_PYDANTIC_V2:
    ENCODERS_BY_TYPE[PydanticObjectId] = (
        str  # it is a workaround to force pydantic make json schema for this field
    )

BeanieObjectId = PydanticObjectId


class ExpressionField(str):
    def __getitem__(self, item):
        """
        Get sub field

        :param item: name of the subfield
        :return: ExpressionField
        """
        return ExpressionField(f"{self}.{item}")

    def __getattr__(self, item):
        """
        Get sub field

        :param item: name of the subfield
        :return: ExpressionField
        """
        return ExpressionField(f"{self}.{item}")

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, ExpressionField):
            return super(ExpressionField, self).__eq__(other)
        return Eq(field=self, other=other)

    def __gt__(self, other):
        return GT(field=self, other=other)

    def __ge__(self, other):
        return GTE(field=self, other=other)

    def __lt__(self, other):
        return LT(field=self, other=other)

    def __le__(self, other):
        return LTE(field=self, other=other)

    def __ne__(self, other):
        return NE(field=self, other=other)

    def __pos__(self):
        return self, SortDirection.ASCENDING

    def __neg__(self):
        return self, SortDirection.DESCENDING

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class DeleteRules(str, Enum):
    DO_NOTHING = "DO_NOTHING"
    DELETE_LINKS = "DELETE_LINKS"


class WriteRules(str, Enum):
    DO_NOTHING = "DO_NOTHING"
    WRITE = "WRITE"


class LinkTypes(str, Enum):
    DIRECT = "DIRECT"
    OPTIONAL_DIRECT = "OPTIONAL_DIRECT"
    LIST = "LIST"
    OPTIONAL_LIST = "OPTIONAL_LIST"

    BACK_DIRECT = "BACK_DIRECT"
    BACK_LIST = "BACK_LIST"
    OPTIONAL_BACK_DIRECT = "OPTIONAL_BACK_DIRECT"
    OPTIONAL_BACK_LIST = "OPTIONAL_BACK_LIST"


class LinkInfo(BaseModel):
    field_name: str
    lookup_field_name: str
    document_class: Type[BaseModel]  # Document class
    link_type: LinkTypes
    nested_links: Optional[Dict] = None
    is_fetchable: bool = True


T = TypeVar("T")


class Link(Generic[T]):
    def __init__(self, ref: DBRef, document_class: Type[T]):
        self.ref = ref
        self.document_class = document_class

    async def fetch(self, fetch_links: bool = False) -> Union[T, Link]:
        result = await self.document_class.get(  # type: ignore
            self.ref.id, with_children=True, fetch_links=fetch_links
        )
        return result or self

    @classmethod
    async def fetch_one(cls, link: Link):
        return await link.fetch()

    @classmethod
    async def fetch_list(
        cls, links: List[Union[Link, DocType]], fetch_links: bool = False
    ):
        """
        Fetch list that contains links and documents
        :param links:
        :param fetch_links:
        :return:
        """
        data = Link.repack_links(links)  # type: ignore
        ids_to_fetch = []
        document_class = None
        for doc_id, link in data.items():
            if isinstance(link, Link):
                if document_class is None:
                    document_class = link.document_class
                else:
                    if document_class != link.document_class:
                        raise ValueError(
                            "All the links must have the same model class"
                        )
                ids_to_fetch.append(link.ref.id)

        if ids_to_fetch:
            fetched_models = await document_class.find(  # type: ignore
                In("_id", ids_to_fetch),
                with_children=True,
                fetch_links=fetch_links,
            ).to_list()

            for model in fetched_models:
                data[model.id] = model

        return list(data.values())

    @staticmethod
    def repack_links(
        links: List[Union[Link, DocType]],
    ) -> OrderedDictType[Any, Any]:
        result = OrderedDict()
        for link in links:
            if isinstance(link, Link):
                result[link.ref.id] = link
            else:
                result[link.id] = link
        return result

    @classmethod
    async def fetch_many(cls, links: List[Link]):
        coros = []
        for link in links:
            coros.append(link.fetch())
        return await asyncio.gather(*coros)

    if IS_PYDANTIC_V2:

        @staticmethod
        def serialize(value: Union[Link, BaseModel]):
            if isinstance(value, Link):
                return value.to_dict()
            return value.model_dump(mode="json")

        @classmethod
        def build_validation(cls, handler, source_type):
            def validate(v: Union[DBRef, T], validation_info: ValidationInfo):
                document_class = DocsRegistry.evaluate_fr(
                    get_args(source_type)[0]
                )  # type: ignore  # noqa: F821

                if isinstance(v, DBRef):
                    return cls(ref=v, document_class=document_class)
                if isinstance(v, Link):
                    return v
                if isinstance(v, dict) and v.keys() == {"id", "collection"}:
                    return cls(
                        ref=DBRef(
                            collection=v["collection"],
                            id=TypeAdapter(
                                document_class.model_fields["id"].annotation
                            ).validate_python(v["id"]),
                        ),
                        document_class=document_class,
                    )
                if isinstance(v, dict) or isinstance(v, BaseModel):
                    return parse_obj(document_class, v)
                new_id = TypeAdapter(
                    document_class.model_fields["id"].annotation
                ).validate_python(v)
                ref = DBRef(
                    collection=document_class.get_collection_name(), id=new_id
                )
                return cls(ref=ref, document_class=document_class)

            return validate

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            return core_schema.json_or_python_schema(
                python_schema=plain_validator(
                    cls.build_validation(handler, source_type)
                ),
                json_schema=core_schema.typed_dict_schema(
                    {
                        "id": core_schema.typed_dict_field(
                            core_schema.str_schema()
                        ),
                        "collection": core_schema.typed_dict_field(
                            core_schema.str_schema()
                        ),
                    }
                ),
                serialization=core_schema.plain_serializer_function_ser_schema(  # type: ignore
                    lambda instance: cls.serialize(instance),
                    when_used="json",  # type: ignore
                ),
            )

    else:

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, v: Union[DBRef, T], field: ModelField):
            document_class = field.sub_fields[0].type_  # type: ignore
            if isinstance(v, DBRef):
                return cls(ref=v, document_class=document_class)
            if isinstance(v, Link):
                return v
            if isinstance(v, dict) or isinstance(v, BaseModel):
                return parse_obj(document_class, v)
            new_id = parse_object_as(
                get_field_type(get_model_fields(document_class)["id"]), v
            )
            ref = DBRef(
                collection=document_class.get_collection_name(), id=new_id
            )
            return cls(ref=ref, document_class=document_class)

    def to_ref(self):
        return self.ref

    def to_dict(self):
        return {"id": str(self.ref.id), "collection": self.ref.collection}


if not IS_PYDANTIC_V2:
    ENCODERS_BY_TYPE[Link] = lambda o: o.to_dict()


class BackLink(Generic[T]):
    """Back reference to a document"""

    def __init__(self, document_class: Type[T]):
        self.document_class = document_class

    if IS_PYDANTIC_V2:

        @classmethod
        def build_validation(cls, handler, source_type):
            def validate(v: Union[DBRef, T], field):
                document_class = DocsRegistry.evaluate_fr(
                    get_args(source_type)[0]
                )  # type: ignore  # noqa: F821
                if isinstance(v, dict) or isinstance(v, BaseModel):
                    return parse_obj(document_class, v)
                return cls(document_class=document_class)

            return validate

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            return plain_validator(cls.build_validation(handler, source_type))

    else:

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, v: Union[DBRef, T], field: ModelField):
            document_class = field.sub_fields[0].type_  # type: ignore
            if isinstance(v, dict) or isinstance(v, BaseModel):
                return parse_obj(document_class, v)
            return cls(document_class=document_class)

    def to_dict(self):
        return {"collection": self.document_class.get_collection_name()}


if not IS_PYDANTIC_V2:
    ENCODERS_BY_TYPE[BackLink] = lambda o: o.to_dict()


class IndexModelField:
    def __init__(self, index: IndexModel):
        self.index = index
        self.name = index.document["name"]

        self.fields = tuple(sorted(self.index.document["key"]))
        self.options = tuple(
            sorted(
                (k, v)
                for k, v in self.index.document.items()
                if k not in ["key", "v"]
            )
        )

    def __eq__(self, other):
        return self.fields == other.fields and self.options == other.options

    def __repr__(self):
        return f"IndexModelField({self.name}, {self.fields}, {self.options})"

    @staticmethod
    def list_difference(
        left: List[IndexModelField], right: List[IndexModelField]
    ):
        result = []
        for index in left:
            if index not in right:
                result.append(index)
        return result

    @staticmethod
    def list_to_index_model(left: List[IndexModelField]):
        return [index.index for index in left]

    @classmethod
    def from_motor_index_information(cls, index_info: dict):
        result = []
        for name, details in index_info.items():
            fields = details["key"]
            if ("_id", 1) in fields:
                continue

            options = {k: v for k, v in details.items() if k != "key"}
            index_model = IndexModelField(
                IndexModel(fields, name=name, **options)
            )
            result.append(index_model)
        return result

    def same_fields(self, other: IndexModelField):
        return self.fields == other.fields

    @staticmethod
    def find_index_with_the_same_fields(
        indexes: List[IndexModelField], index: IndexModelField
    ):
        for i in indexes:
            if i.same_fields(index):
                return i
        return None

    @staticmethod
    def merge_indexes(
        left: List[IndexModelField], right: List[IndexModelField]
    ):
        left_dict = {index.fields: index for index in left}
        right_dict = {index.fields: index for index in right}
        left_dict.update(right_dict)
        return list(left_dict.values())

    if IS_PYDANTIC_V2:

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            def validate(v, _):
                if isinstance(v, IndexModel):
                    return IndexModelField(v)
                else:
                    return IndexModelField(IndexModel(v))

            return plain_validator(validate)

    else:

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, v):
            if isinstance(v, IndexModel):
                return IndexModelField(v)
            else:
                return IndexModelField(IndexModel(v))
