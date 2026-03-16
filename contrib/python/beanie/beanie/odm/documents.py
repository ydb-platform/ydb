import asyncio
import warnings
from datetime import datetime, timezone
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID, uuid4

from bson import DBRef, ObjectId
from lazy_model import LazyModel
from motor.motor_asyncio import AsyncIOMotorClientSession
from pydantic import (
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
)
from pydantic.class_validators import root_validator
from pydantic.main import BaseModel
from pymongo import InsertOne
from pymongo.errors import DuplicateKeyError
from pymongo.results import (
    DeleteResult,
    InsertManyResult,
)
from typing_extensions import Concatenate, ParamSpec, Self, TypeAlias

from beanie.exceptions import (
    CollectionWasNotInitialized,
    DocumentNotFound,
    DocumentWasNotSaved,
    NotSupported,
    ReplaceError,
    RevisionIdWasChanged,
)
from beanie.odm.actions import (
    ActionDirections,
    EventTypes,
    wrap_with_actions,
)
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.cache import LRUCache
from beanie.odm.enums import SortDirection
from beanie.odm.fields import (
    BackLink,
    DeleteRules,
    ExpressionField,
    Link,
    LinkInfo,
    LinkTypes,
    PydanticObjectId,
    WriteRules,
)
from beanie.odm.interfaces.aggregate import AggregateInterface
from beanie.odm.interfaces.detector import ModelType
from beanie.odm.interfaces.find import FindInterface
from beanie.odm.interfaces.getters import OtherGettersInterface
from beanie.odm.interfaces.inheritance import InheritanceInterface
from beanie.odm.interfaces.setters import SettersInterface
from beanie.odm.models import (
    InspectionError,
    InspectionResult,
    InspectionStatuses,
)
from beanie.odm.operators.find.comparison import In
from beanie.odm.operators.update.general import (
    CurrentDate,
    Inc,
    SetRevisionId,
    Unset,
)
from beanie.odm.operators.update.general import (
    Set as SetOperator,
)
from beanie.odm.queries.find import FindMany, FindOne
from beanie.odm.queries.update import UpdateMany, UpdateResponse
from beanie.odm.settings.document import DocumentSettings
from beanie.odm.utils.dump import get_dict, get_top_level_nones
from beanie.odm.utils.parsing import apply_changes, merge_models
from beanie.odm.utils.pydantic import (
    IS_PYDANTIC_V2,
    get_extra_field_info,
    get_field_type,
    get_model_dump,
    get_model_fields,
    parse_model,
    parse_object_as,
)
from beanie.odm.utils.self_validation import validate_self_before
from beanie.odm.utils.state import (
    previous_saved_state_needed,
    save_state_after,
    saved_state_needed,
)
from beanie.odm.utils.typing import extract_id_class

if IS_PYDANTIC_V2:
    from pydantic import model_validator

if TYPE_CHECKING:
    from beanie.odm.views import View

FindType = TypeVar("FindType", bound=Union["Document", "View"])
DocType = TypeVar("DocType", bound="Document")
P = ParamSpec("P")
R = TypeVar("R")
# can describe both sync and async, where R itself is a coroutine
AnyDocMethod: TypeAlias = Callable[Concatenate[DocType, P], R]
# describes only async
AsyncDocMethod: TypeAlias = Callable[
    Concatenate[DocType, P], Coroutine[Any, Any, R]
]
DocumentProjectionType = TypeVar("DocumentProjectionType", bound=BaseModel)


def json_schema_extra(schema: Dict[str, Any], model: Type["Document"]) -> None:
    # remove excluded fields from the json schema
    properties = schema.get("properties")
    if not properties:
        return
    for k, field in get_model_fields(model).items():
        k = field.alias or k
        if k not in properties:
            continue
        field_info = field if IS_PYDANTIC_V2 else field.field_info
        if field_info.exclude:
            del properties[k]


def document_alias_generator(s: str) -> str:
    if s == "id":
        return "_id"
    return s


class MergeStrategy(str, Enum):
    local = "local"
    remote = "remote"


class Document(
    LazyModel,
    SettersInterface,
    InheritanceInterface,
    FindInterface,
    AggregateInterface,
    OtherGettersInterface,
):
    """
    Document Mapping class.

    Fields:

    - `id` - MongoDB document ObjectID "_id" field.
    Mapped to the PydanticObjectId class
    """

    if IS_PYDANTIC_V2:
        model_config = ConfigDict(
            json_schema_extra=json_schema_extra,
            populate_by_name=True,
            alias_generator=document_alias_generator,
        )
    else:

        class Config:
            json_encoders = {ObjectId: str}
            allow_population_by_field_name = True
            fields = {"id": "_id"}
            schema_extra = staticmethod(json_schema_extra)

    id: Optional[PydanticObjectId] = Field(
        default=None, description="MongoDB document ObjectID"
    )

    # State
    revision_id: Optional[UUID] = Field(default=None, exclude=True)
    _saved_state: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _previous_saved_state: Optional[Dict[str, Any]] = PrivateAttr(default=None)

    # Relations
    _link_fields: ClassVar[Optional[Dict[str, LinkInfo]]] = None

    # Cache
    _cache: ClassVar[Optional[LRUCache]] = None

    # Settings
    _document_settings: ClassVar[Optional[DocumentSettings]] = None

    # Database
    _database_major_version: ClassVar[int] = 4

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Document, self).__init__(*args, **kwargs)
        self.get_motor_collection()

    @classmethod
    def _fill_back_refs(cls, values):
        if cls._link_fields:
            for field_name, link_info in cls._link_fields.items():
                if (
                    link_info.link_type
                    in [LinkTypes.BACK_DIRECT, LinkTypes.OPTIONAL_BACK_DIRECT]
                    and field_name not in values
                ):
                    values[field_name] = BackLink[link_info.document_class](
                        link_info.document_class
                    )
                if (
                    link_info.link_type
                    in [LinkTypes.BACK_LIST, LinkTypes.OPTIONAL_BACK_LIST]
                    and field_name not in values
                ):
                    values[field_name] = [
                        BackLink[link_info.document_class](
                            link_info.document_class
                        )
                    ]
        return values

    if IS_PYDANTIC_V2:

        @model_validator(mode="before")
        def fill_back_refs(cls, values):
            return cls._fill_back_refs(values)

    else:

        @root_validator(pre=True)
        def fill_back_refs(cls, values):
            return cls._fill_back_refs(values)

    @classmethod
    async def get(
        cls: Type["DocType"],
        document_id: Any,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Optional["DocType"]:
        """
        Get document by id, returns None if document does not exist

        :param document_id: PydanticObjectId - document id
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param ignore_cache: bool - ignore cache (if it is turned on)
        :param **pymongo_kwargs: pymongo native parameters for find operation
        :return: Union["Document", None]
        """
        if not isinstance(
            document_id,
            extract_id_class(get_field_type(get_model_fields(cls)["id"])),
        ):
            document_id = parse_object_as(
                get_field_type(get_model_fields(cls)["id"]), document_id
            )

        return await cls.find_one(
            {"_id": document_id},
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )

    async def sync(self, merge_strategy: MergeStrategy = MergeStrategy.remote):
        """
        Sync the document with the database

        :param merge_strategy: MergeStrategy - how to merge the document
        :return: None
        """
        if (
            merge_strategy == MergeStrategy.local
            and self.get_settings().use_state_management is False
        ):
            raise ValueError(
                "State management must be turned on to use local merge strategy"
            )
        if self.id is None:
            raise DocumentWasNotSaved
        document = await self.find_one({"_id": self.id})
        if document is None:
            raise DocumentNotFound

        if merge_strategy == MergeStrategy.local:
            original_changes = self.get_changes()
            new_state = document.get_saved_state()
            if new_state is None:
                raise DocumentWasNotSaved
            changes_to_apply = self._collect_updates(
                new_state, original_changes
            )
            merge_models(self, document)
            apply_changes(changes_to_apply, self)
        elif merge_strategy == MergeStrategy.remote:
            merge_models(self, document)
        else:
            raise ValueError("Invalid merge strategy")

    @wrap_with_actions(EventTypes.INSERT)
    @save_state_after
    @validate_self_before
    async def insert(
        self: Self,
        *,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        session: Optional[AsyncIOMotorClientSession] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Self:
        """
        Insert the document (self) to the collection
        :return: self
        """
        if self.get_settings().use_revision:
            self.revision_id = uuid4()
        if link_rule == WriteRules.WRITE:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.save(
                                link_rule=WriteRules.WRITE, session=session
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                    ]:
                        if isinstance(value, List):
                            await asyncio.gather(
                                *[
                                    obj.save(
                                        link_rule=WriteRules.WRITE,
                                        session=session,
                                    )
                                    for obj in value
                                    if isinstance(obj, Document)
                                ]
                            )
        result = await self.get_motor_collection().insert_one(
            get_dict(
                self, to_db=True, keep_nulls=self.get_settings().keep_nulls
            ),
            session=session,
        )
        new_id = result.inserted_id
        if not isinstance(
            new_id,
            extract_id_class(get_field_type(get_model_fields(self)["id"])),
        ):
            new_id = parse_object_as(
                get_field_type(get_model_fields(self)["id"]), new_id
            )
        self.id = new_id
        return self

    async def create(
        self: Self,
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> Self:
        """
        The same as self.insert()
        :return: self
        """
        return await self.insert(session=session)

    @classmethod
    async def insert_one(
        cls: Type[DocType],
        document: DocType,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional["BulkWriter"] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
    ) -> Optional[DocType]:
        """
        Insert one document to the collection
        :param document: Document - document to insert
        :param session: AsyncIOMotorClientSession - motor session
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param link_rule: InsertRules - hot to manage link fields
        :return: DocType
        """
        if not isinstance(document, cls):
            raise TypeError(
                "Inserting document must be of the original document class"
            )
        if bulk_writer is None:
            return await document.insert(link_rule=link_rule, session=session)
        else:
            if link_rule == WriteRules.WRITE:
                raise NotSupported(
                    "Cascade insert with bulk writing not supported"
                )
            bulk_writer.add_operation(
                Operation(
                    operation=InsertOne,
                    first_query=get_dict(
                        document,
                        to_db=True,
                        keep_nulls=document.get_settings().keep_nulls,
                    ),
                    object_class=type(document),
                )
            )
            return None

    @classmethod
    async def insert_many(
        cls: Type[DocType],
        documents: Iterable[DocType],
        session: Optional[AsyncIOMotorClientSession] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        **pymongo_kwargs: Any,
    ) -> InsertManyResult:
        """
        Insert many documents to the collection

        :param documents:  List["Document"] - documents to insert
        :param session: AsyncIOMotorClientSession - motor session
        :param link_rule: InsertRules - how to manage link fields
        :return: InsertManyResult
        """
        if link_rule == WriteRules.WRITE:
            raise NotSupported(
                "Cascade insert not supported for insert many method"
            )
        documents_list = [
            get_dict(
                document,
                to_db=True,
                keep_nulls=document.get_settings().keep_nulls,
            )
            for document in documents
        ]
        return await cls.get_motor_collection().insert_many(
            documents_list, session=session, **pymongo_kwargs
        )

    @wrap_with_actions(EventTypes.REPLACE)
    @save_state_after
    @validate_self_before
    async def replace(
        self: Self,
        ignore_revision: bool = False,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Self:
        """
        Fully update the document in the database

        :param session: Optional[AsyncIOMotorClientSession] - motor session.
        :param ignore_revision: bool - do force replace.
            Used when revision based protection is turned on.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :return: self
        """
        if self.id is None:
            raise ValueError("Document must have an id")

        if bulk_writer is not None and link_rule != WriteRules.DO_NOTHING:
            raise NotSupported

        if link_rule == WriteRules.WRITE:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.replace(
                                link_rule=link_rule,
                                bulk_writer=bulk_writer,
                                ignore_revision=ignore_revision,
                                session=session,
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            await asyncio.gather(
                                *[
                                    obj.replace(
                                        link_rule=link_rule,
                                        bulk_writer=bulk_writer,
                                        ignore_revision=ignore_revision,
                                        session=session,
                                    )
                                    for obj in value
                                    if isinstance(obj, Document)
                                ]
                            )

        use_revision_id = self.get_settings().use_revision
        find_query: Dict[str, Any] = {"_id": self.id}

        if use_revision_id and not ignore_revision:
            find_query["revision_id"] = self.revision_id
            self.revision_id = uuid4()
        try:
            await self.find_one(find_query).replace_one(
                self,
                session=session,
                bulk_writer=bulk_writer,
            )
        except DocumentNotFound:
            if use_revision_id and not ignore_revision:
                raise RevisionIdWasChanged
            else:
                raise DocumentNotFound
        return self

    @wrap_with_actions(EventTypes.SAVE)
    @save_state_after
    @validate_self_before
    async def save(
        self: Self,
        session: Optional[AsyncIOMotorClientSession] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        ignore_revision: bool = False,
        **kwargs: Any,
    ) -> Self:
        """
        Update an existing model in the database or
        insert it if it does not yet exist.

        :param session: Optional[AsyncIOMotorClientSession] - motor session.
        :param link_rule: WriteRules - rules how to deal with links on writing
        :param ignore_revision: bool - do force save.
        :return: self
        """
        if link_rule == WriteRules.WRITE:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.save(
                                link_rule=link_rule, session=session
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            await asyncio.gather(
                                *[
                                    obj.save(
                                        link_rule=link_rule, session=session
                                    )
                                    for obj in value
                                    if isinstance(obj, Document)
                                ]
                            )

        if self.get_settings().keep_nulls is False:
            return await self.update(
                SetOperator(
                    get_dict(
                        self,
                        to_db=True,
                        keep_nulls=self.get_settings().keep_nulls,
                    )
                ),
                Unset(get_top_level_nones(self)),
                session=session,
                ignore_revision=ignore_revision,
                upsert=True,
                **kwargs,
            )
        else:
            return await self.update(
                SetOperator(
                    get_dict(
                        self,
                        to_db=True,
                        keep_nulls=self.get_settings().keep_nulls,
                    )
                ),
                session=session,
                ignore_revision=ignore_revision,
                upsert=True,
                **kwargs,
            )

    @saved_state_needed
    @wrap_with_actions(EventTypes.SAVE_CHANGES)
    @validate_self_before
    async def save_changes(
        self: Self,
        ignore_revision: bool = False,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Optional[Self]:
        """
        Save changes.
        State management usage must be turned on

        :param ignore_revision: bool - ignore revision id, if revision is turned on
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :return: Optional[self]
        """
        if not self.is_changed:
            return None
        changes = self.get_changes()
        if self.get_settings().keep_nulls is False:
            return await self.update(
                SetOperator(changes),
                Unset(get_top_level_nones(self)),
                ignore_revision=ignore_revision,
                session=session,
                bulk_writer=bulk_writer,
            )
        else:
            return await self.set(
                changes,
                ignore_revision=ignore_revision,
                session=session,
                bulk_writer=bulk_writer,
            )

    @classmethod
    async def replace_many(
        cls: Type[DocType],
        documents: List[DocType],
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> None:
        """
        Replace list of documents

        :param documents: List["Document"]
        :param session: Optional[AsyncIOMotorClientSession] - motor session.
        :return: None
        """
        ids_list = [document.id for document in documents]
        if await cls.find(In(cls.id, ids_list)).count() != len(ids_list):
            raise ReplaceError(
                "Some of the documents are not exist in the collection"
            )
        async with BulkWriter(session=session) as bulk_writer:
            for document in documents:
                await document.replace(
                    bulk_writer=bulk_writer, session=session
                )

    @wrap_with_actions(EventTypes.UPDATE)
    @save_state_after
    async def update(
        self: Self,
        *args: Union[Dict[Any, Any], Mapping[Any, Any]],
        ignore_revision: bool = False,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        skip_sync: Optional[bool] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Partially update the document in the database

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: AsyncIOMotorClientSession - motor session.
        :param ignore_revision: bool - force update. Will update even if revision id is not the same, as stored
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        arguments: list[Any] = list(args)

        if skip_sync is not None:
            raise DeprecationWarning(
                "skip_sync parameter is not supported. The document get synced always using atomic operation."
            )
        use_revision_id = self.get_settings().use_revision

        if self.id is not None:
            find_query: Dict[str, Any] = {"_id": self.id}
        else:
            find_query = {"_id": PydanticObjectId()}

        if use_revision_id and not ignore_revision:
            find_query["revision_id"] = self.revision_id

        if use_revision_id:
            new_revision_id = uuid4()
            arguments.append(SetRevisionId(new_revision_id))
        try:
            result = await self.find_one(find_query).update(
                *arguments,
                session=session,
                response_type=UpdateResponse.NEW_DOCUMENT,
                bulk_writer=bulk_writer,
                **pymongo_kwargs,
            )
        except DuplicateKeyError:
            raise RevisionIdWasChanged
        if bulk_writer is None:
            if use_revision_id and not ignore_revision and result is None:
                raise RevisionIdWasChanged
            merge_models(self, result)
        return self

    @classmethod
    def update_all(
        cls,
        *args: Union[dict, Mapping],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateMany:
        """
        Partially update all the documents

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: AsyncIOMotorClientSession - motor session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param **pymongo_kwargs: pymongo native parameters for find operation
        :return: UpdateMany query
        """
        return cls.find_all().update_many(
            *args, session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    def set(
        self: Self,
        expression: Dict[Union[ExpressionField, str, Any], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_sync: Optional[bool] = None,
        **kwargs: Any,
    ) -> Coroutine[None, None, Self]:
        """
        Set values

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).set({Sample.one: 100})

        ```

        Uses [Set operator](operators/update.md#set)

        :param expression: Dict[Union[ExpressionField, str, Any], Any] - keys and
        values to set
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :param skip_sync: bool - skip doc syncing. Available for the direct instances only
        :return: self
        """
        return self.update(
            SetOperator(expression),
            session=session,
            bulk_writer=bulk_writer,
            skip_sync=skip_sync,
            **kwargs,
        )

    def current_date(
        self: Self,
        expression: Dict[Union[datetime, ExpressionField, str], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_sync: Optional[bool] = None,
        **kwargs: Any,
    ) -> Coroutine[None, None, Self]:
        """
        Set current date

        Uses [CurrentDate operator](operators/update.md#currentdate)

        :param expression: Dict[Union[datetime, ExpressionField, str], Any]
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :param skip_sync: bool - skip doc syncing. Available for the direct instances only
        :return: self
        """
        return self.update(
            CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            skip_sync=skip_sync,
            **kwargs,
        )

    def inc(
        self: Self,
        expression: Dict[Union[ExpressionField, float, int, str], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_sync: Optional[bool] = None,
        **kwargs: Any,
    ) -> Coroutine[None, None, Self]:
        """
        Increment

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).inc({Sample.one: 100})

        ```

        Uses [Inc operator](operators/update.md#inc)

        :param expression: Dict[Union[ExpressionField, float, int, str], Any]
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :param skip_sync: bool - skip doc syncing. Available for the direct instances only
        :return: self
        """
        return self.update(
            Inc(expression),
            session=session,
            bulk_writer=bulk_writer,
            skip_sync=skip_sync,
            **kwargs,
        )

    @wrap_with_actions(EventTypes.DELETE)
    async def delete(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: DeleteRules = DeleteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs: Any,
    ) -> Optional[DeleteResult]:
        """
        Delete the document

        :param session: Optional[AsyncIOMotorClientSession] - motor session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param link_rule: DeleteRules - rules for link fields
        :param **pymongo_kwargs: pymongo native parameters for delete operation
        :return: Optional[DeleteResult] - pymongo DeleteResult instance.
        """

        if link_rule == DeleteRules.DELETE_LINKS:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.delete(
                                link_rule=DeleteRules.DELETE_LINKS,
                                **pymongo_kwargs,
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            await asyncio.gather(
                                *[
                                    obj.delete(
                                        link_rule=DeleteRules.DELETE_LINKS,
                                        **pymongo_kwargs,
                                    )
                                    for obj in value
                                    if isinstance(obj, Document)
                                ]
                            )

        return await self.find_one({"_id": self.id}).delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    @classmethod
    async def delete_all(
        cls,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Optional[DeleteResult]:
        """
        Delete all the documents

        :param session: Optional[AsyncIOMotorClientSession] - motor session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param **pymongo_kwargs: pymongo native parameters for delete operation
        :return: Optional[DeleteResult] - pymongo DeleteResult instance.
        """
        return await cls.find_all().delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    # State management

    @classmethod
    def use_state_management(cls) -> bool:
        """
        Is state management turned on
        :return: bool
        """
        return cls.get_settings().use_state_management

    @classmethod
    def state_management_save_previous(cls) -> bool:
        """
        Should we save the previous state after a commit to database
        :return: bool
        """
        return cls.get_settings().state_management_save_previous

    @classmethod
    def state_management_replace_objects(cls) -> bool:
        """
        Should objects be replaced when using state management
        :return: bool
        """
        return cls.get_settings().state_management_replace_objects

    def _save_state(self) -> None:
        """
        Save current document state. Internal method
        :return: None
        """
        if self.use_state_management() and self.id is not None:
            if self.state_management_save_previous():
                self._previous_saved_state = self._saved_state

            self._saved_state = get_dict(
                self,
                to_db=True,
                keep_nulls=self.get_settings().keep_nulls,
                exclude={"revision_id"},
            )

    def get_saved_state(self) -> Optional[Dict[str, Any]]:
        """
        Saved state getter. It is protected property.
        :return: Optional[Dict[str, Any]] - saved state
        """
        return self._saved_state

    def get_previous_saved_state(self) -> Optional[Dict[str, Any]]:
        """
        Previous state getter. It is a protected property.
        :return: Optional[Dict[str, Any]] - previous state
        """
        return self._previous_saved_state

    @property
    @saved_state_needed
    def is_changed(self) -> bool:
        if self._saved_state == get_dict(
            self,
            to_db=True,
            keep_nulls=self.get_settings().keep_nulls,
            exclude={"revision_id"},
        ):
            return False
        return True

    @property
    @saved_state_needed
    @previous_saved_state_needed
    def has_changed(self) -> bool:
        if (
            self._previous_saved_state is None
            or self._previous_saved_state == self._saved_state
        ):
            return False
        return True

    def _collect_updates(
        self, old_dict: Dict[str, Any], new_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compares old_dict with new_dict and returns field paths that have been updated
        Args:
            old_dict: dict1
            new_dict: dict2

        Returns: dictionary with updates

        """
        updates = {}
        if old_dict.keys() - new_dict.keys():
            updates = new_dict
        else:
            for field_name, field_value in new_dict.items():
                if field_value != old_dict.get(field_name):
                    if not self.state_management_replace_objects() and (
                        isinstance(field_value, dict)
                        and isinstance(old_dict.get(field_name), dict)
                    ):
                        if old_dict.get(field_name) is None:
                            updates[field_name] = field_value
                        elif isinstance(field_value, dict) and isinstance(
                            old_dict.get(field_name), dict
                        ):
                            field_data = self._collect_updates(
                                old_dict.get(field_name),  # type: ignore
                                field_value,
                            )

                            for k, v in field_data.items():
                                updates[f"{field_name}.{k}"] = v
                    else:
                        updates[field_name] = field_value

        return updates

    @saved_state_needed
    def get_changes(self) -> Dict[str, Any]:
        return self._collect_updates(
            self._saved_state,  # type: ignore
            get_dict(
                self,
                to_db=True,
                keep_nulls=self.get_settings().keep_nulls,
                exclude={"revision_id"},
            ),
        )

    @saved_state_needed
    @previous_saved_state_needed
    def get_previous_changes(self) -> Dict[str, Any]:
        if self._previous_saved_state is None:
            return {}

        return self._collect_updates(
            self._previous_saved_state,
            self._saved_state,  # type: ignore
        )

    @saved_state_needed
    def rollback(self) -> None:
        if self.is_changed:
            for key, value in self._saved_state.items():  # type: ignore
                if key == "_id":
                    setattr(self, "id", value)
                else:
                    setattr(self, key, value)

    # Other

    @classmethod
    def get_settings(cls) -> DocumentSettings:
        """
        Get document settings, which was created on
        the initialization step

        :return: DocumentSettings class
        """
        if cls._document_settings is None:
            raise CollectionWasNotInitialized
        return cls._document_settings

    @classmethod
    async def inspect_collection(
        cls, session: Optional[AsyncIOMotorClientSession] = None
    ) -> InspectionResult:
        """
        Check, if documents, stored in the MongoDB collection
        are compatible with the Document schema

        :return: InspectionResult
        """
        inspection_result = InspectionResult()
        async for json_document in cls.get_motor_collection().find(
            {}, session=session
        ):
            try:
                parse_model(cls, json_document)
            except ValidationError as e:
                if inspection_result.status == InspectionStatuses.OK:
                    inspection_result.status = InspectionStatuses.FAIL
                inspection_result.errors.append(
                    InspectionError(
                        document_id=json_document["_id"], error=str(e)
                    )
                )
        return inspection_result

    @classmethod
    def _check_hidden_fields(cls):
        hidden_fields = [
            (name, field)
            for name, field in get_model_fields(cls).items()
            if get_extra_field_info(field, "hidden") is True
        ]
        if not hidden_fields:
            return
        warnings.warn(
            f"{cls.__name__}: 'hidden=True' is deprecated, please use 'exclude=True'",
            DeprecationWarning,
            stacklevel=2,
        )
        if IS_PYDANTIC_V2:
            for name, field in hidden_fields:
                field.exclude = True
                del field.json_schema_extra["hidden"]
            cls.model_rebuild(force=True)
        else:
            for name, field in hidden_fields:
                field.field_info.exclude = True
                del field.field_info.extra["hidden"]
                cls.__exclude_fields__[name] = True

    @wrap_with_actions(event_type=EventTypes.VALIDATE_ON_SAVE)
    async def validate_self(self, *args: Any, **kwargs: Any):
        # TODO: it can be sync, but needs some actions controller improvements
        if self.get_settings().validate_on_save:
            new_model = parse_model(self.__class__, get_model_dump(self))
            merge_models(self, new_model)

    def to_ref(self):
        if self.id is None:
            raise DocumentWasNotSaved("Can not create dbref without id")
        return DBRef(self.get_motor_collection().name, self.id)

    async def fetch_link(self, field: Union[str, Any]):
        ref_obj = getattr(self, field, None)
        if isinstance(ref_obj, Link):
            value = await ref_obj.fetch(fetch_links=True)
            setattr(self, field, value)
        if isinstance(ref_obj, list) and ref_obj:
            values = await Link.fetch_list(ref_obj, fetch_links=True)
            setattr(self, field, values)

    async def fetch_all_links(self):
        coros = []
        link_fields = self.get_link_fields()
        if link_fields is not None:
            for ref in link_fields.values():
                coros.append(self.fetch_link(ref.field_name))  # TODO lists
        await asyncio.gather(*coros)

    @classmethod
    def get_link_fields(cls) -> Optional[Dict[str, LinkInfo]]:
        return cls._link_fields

    @classmethod
    def get_model_type(cls) -> ModelType:
        return ModelType.Document

    @classmethod
    async def distinct(
        cls,
        key: str,
        filter: Optional[Mapping[str, Any]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        **kwargs: Any,
    ) -> list:
        return await cls.get_motor_collection().distinct(
            key, filter, session, **kwargs
        )

    @classmethod
    def link_from_id(cls, id: Any):
        ref = DBRef(id=id, collection=cls.get_collection_name())
        return Link(ref, document_class=cls)


class DocumentWithSoftDelete(Document):
    deleted_at: Optional[datetime] = None

    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    async def hard_delete(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: DeleteRules = DeleteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs: Any,
    ) -> Optional[DeleteResult]:
        return await super().delete(
            session=session,
            bulk_writer=bulk_writer,
            link_rule=link_rule,
            skip_actions=skip_actions,
            **pymongo_kwargs,
        )

    async def delete(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: DeleteRules = DeleteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs,
    ) -> Optional[DeleteResult]:
        self.deleted_at = datetime.now(tz=timezone.utc)
        await self.save()
        return None

    @classmethod
    def find_many_in_all(  # type: ignore
        cls: Type[FindType],
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type["DocumentProjectionType"]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[FindType], FindMany["DocumentProjectionType"]]:
        return cls._find_many_query_class(document_model=cls).find_many(
            *args,
            sort=sort,
            skip=skip,
            limit=limit,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )

    @classmethod
    def find_many(  # type: ignore
        cls: Type[FindType],
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type["DocumentProjectionType"]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[FindType], FindMany["DocumentProjectionType"]]:
        args = cls._add_class_id_filter(args, with_children) + (
            {"deleted_at": None},
        )
        return cls._find_many_query_class(document_model=cls).find_many(
            *args,
            sort=sort,
            skip=skip,
            limit=limit,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )

    @classmethod
    def find_one(  # type: ignore
        cls: Type[FindType],
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type["DocumentProjectionType"]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[FindOne[FindType], FindOne["DocumentProjectionType"]]:
        args = cls._add_class_id_filter(args, with_children) + (
            {"deleted_at": None},
        )
        return cls._find_one_query_class(document_model=cls).find_one(
            *args,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )
