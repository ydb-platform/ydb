import asyncio
import sys

from typing_extensions import Sequence, get_args, get_origin

from beanie.odm.utils.pydantic import (
    IS_PYDANTIC_V2,
    get_extra_field_info,
    get_model_fields,
    parse_model,
)
from beanie.odm.utils.typing import get_index_attributes

if sys.version_info >= (3, 10):
    from types import UnionType as TypesUnionType
else:
    TypesUnionType = ()

import importlib
import inspect
from typing import (  # type: ignore
    List,
    Optional,
    Type,
    Union,
    _GenericAlias,
)

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pymongo import IndexModel

from beanie.exceptions import Deprecation, MongoDBVersionError
from beanie.odm.actions import ActionRegistry
from beanie.odm.cache import LRUCache
from beanie.odm.documents import DocType, Document
from beanie.odm.fields import (
    BackLink,
    ExpressionField,
    Link,
    LinkInfo,
    LinkTypes,
)
from beanie.odm.interfaces.detector import ModelType
from beanie.odm.registry import DocsRegistry
from beanie.odm.settings.document import DocumentSettings, IndexModelField
from beanie.odm.settings.union_doc import UnionDocSettings
from beanie.odm.settings.view import ViewSettings
from beanie.odm.union_doc import UnionDoc, UnionDocType
from beanie.odm.views import View


class Output(BaseModel):
    class_name: str
    collection_name: str


class Initializer:
    def __init__(
        self,
        database: AsyncIOMotorDatabase = None,
        connection_string: Optional[str] = None,
        document_models: Optional[
            Sequence[
                Union[Type["DocType"], Type["UnionDocType"], Type["View"], str]
            ]
        ] = None,
        allow_index_dropping: bool = False,
        recreate_views: bool = False,
        multiprocessing_mode: bool = False,
        skip_indexes: bool = False,
    ):
        """
        Beanie initializer

        :param database: AsyncIOMotorDatabase - motor database instance
        :param connection_string: str - MongoDB connection string
        :param document_models: List[Union[Type[DocType], Type[UnionDocType], str]] - model classes
        or strings with dot separated paths
        :param allow_index_dropping: bool - if index dropping is allowed.
        Default False
        :param recreate_views: bool - if views should be recreated. Default False
        :param multiprocessing_mode: bool - if multiprocessing mode is on
        it will patch the motor client to use process's event loop.
        :param skip_indexes: bool - if you want to skip working with indexes. Default False
        :return: None
        """

        self.inited_classes: List[Type] = []
        self.allow_index_dropping = allow_index_dropping
        self.skip_indexes = skip_indexes
        self.recreate_views = recreate_views

        self.models_with_updated_forward_refs: List[Type[BaseModel]] = []

        if (connection_string is None and database is None) or (
            connection_string is not None and database is not None
        ):
            raise ValueError(
                "connection_string parameter or database parameter must be set"
            )

        if document_models is None:
            raise ValueError("document_models parameter must be set")
        if connection_string is not None:
            database = AsyncIOMotorClient(
                connection_string
            ).get_default_database()

        self.database: AsyncIOMotorDatabase = database

        if multiprocessing_mode:
            self.database.client.get_io_loop = asyncio.get_running_loop

        sort_order = {
            ModelType.UnionDoc: 0,
            ModelType.Document: 1,
            ModelType.View: 2,
        }

        self.document_models: List[
            Union[Type[DocType], Type[UnionDocType], Type[View]]
        ] = [
            self.get_model(model) if isinstance(model, str) else model
            for model in document_models
        ]

        self.fill_docs_registry()

        self.document_models.sort(
            key=lambda val: sort_order[val.get_model_type()]
        )

    def __await__(self):
        for model in self.document_models:
            yield from self.init_class(model).__await__()

    # General
    def fill_docs_registry(self):
        for model in self.document_models:
            module = inspect.getmodule(model)
            members = inspect.getmembers(module)
            for name, obj in members:
                if inspect.isclass(obj) and issubclass(obj, BaseModel):
                    DocsRegistry.register(name, obj)

    @staticmethod
    def get_model(dot_path: str) -> Type["DocType"]:
        """
        Get the model by the path in format bar.foo.Model

        :param dot_path: str - dot seprated path to the model
        :return: Type[DocType] - class of the model
        """
        module_name, class_name = None, None
        try:
            module_name, class_name = dot_path.rsplit(".", 1)
            return getattr(importlib.import_module(module_name), class_name)

        except ValueError:
            raise ValueError(
                f"'{dot_path}' doesn't have '.' path, eg. path.to.your.model.class"
            )

        except AttributeError:
            raise AttributeError(
                f"module '{module_name}' has no class called '{class_name}'"
            )

    def init_settings(
        self, cls: Union[Type[Document], Type[View], Type[UnionDoc]]
    ):
        """
        Init Settings

        :param cls: Union[Type[Document], Type[View], Type[UnionDoc]] - Class
        to init settings
        :return: None
        """
        settings_class = getattr(cls, "Settings", None)
        settings_vars = {}
        if settings_class is not None:
            # get all attributes of the Settings subclass (including inherited ones)
            # without magic dunder methods
            settings_vars = {
                attr: getattr(settings_class, attr)
                for attr in dir(settings_class)
                if not attr.startswith("__")
            }
        if issubclass(cls, Document):
            cls._document_settings = parse_model(
                DocumentSettings, settings_vars
            )
        if issubclass(cls, View):
            cls._settings = parse_model(ViewSettings, settings_vars)
        if issubclass(cls, UnionDoc):
            cls._settings = parse_model(UnionDocSettings, settings_vars)

    if not IS_PYDANTIC_V2:

        def update_forward_refs(self, cls: Type[BaseModel]):
            """
            Update forward refs

            :param cls: Type[BaseModel] - class to update forward refs
            :return: None
            """
            if cls not in self.models_with_updated_forward_refs:
                cls.update_forward_refs()
                self.models_with_updated_forward_refs.append(cls)

    # General. Relations

    def detect_link(
        self, field: FieldInfo, field_name: str
    ) -> Optional[LinkInfo]:
        """
        It detects link and returns LinkInfo if any found.

        :param field: ModelField
        :return: Optional[LinkInfo]
        """

        origin = get_origin(field.annotation)
        args = get_args(field.annotation)
        classes = [
            Link,
            BackLink,
        ]

        for cls in classes:
            # Check if annotation is one of the custom classes
            if (
                isinstance(field.annotation, _GenericAlias)
                and field.annotation.__origin__ is cls
            ):
                if cls is Link:
                    return LinkInfo(
                        field_name=field_name,
                        lookup_field_name=field_name,
                        document_class=DocsRegistry.evaluate_fr(args[0]),  # type: ignore
                        link_type=LinkTypes.DIRECT,
                    )
                if cls is BackLink:
                    return LinkInfo(
                        field_name=field_name,
                        lookup_field_name=get_extra_field_info(
                            field, "original_field"
                        ),  # type: ignore
                        document_class=DocsRegistry.evaluate_fr(args[0]),  # type: ignore
                        link_type=LinkTypes.BACK_DIRECT,
                    )

            # Check if annotation is List[custom class]
            elif (
                (origin is List or origin is list)
                and len(args) == 1
                and isinstance(args[0], _GenericAlias)
                and args[0].__origin__ is cls
            ):
                if cls is Link:
                    return LinkInfo(
                        field_name=field_name,
                        lookup_field_name=field_name,
                        document_class=DocsRegistry.evaluate_fr(
                            get_args(args[0])[0]
                        ),  # type: ignore
                        link_type=LinkTypes.LIST,
                    )
                if cls is BackLink:
                    return LinkInfo(
                        field_name=field_name,
                        lookup_field_name=get_extra_field_info(  # type: ignore
                            field, "original_field"
                        ),
                        document_class=DocsRegistry.evaluate_fr(
                            get_args(args[0])[0]
                        ),  # type: ignore
                        link_type=LinkTypes.BACK_LIST,
                    )

            # Check if annotation is Optional[custom class] or Optional[List[custom class]]
            elif (
                (origin is Union or origin is TypesUnionType)
                and len(args) == 2
                and type(None) in args
            ):
                if args[1] is type(None):
                    optional = args[0]
                else:
                    optional = args[1]
                optional_origin = get_origin(optional)
                optional_args = get_args(optional)

                if (
                    isinstance(optional, _GenericAlias)
                    and optional.__origin__ is cls
                ):
                    if cls is Link:
                        return LinkInfo(
                            field_name=field_name,
                            lookup_field_name=field_name,
                            document_class=DocsRegistry.evaluate_fr(
                                optional_args[0]
                            ),  # type: ignore
                            link_type=LinkTypes.OPTIONAL_DIRECT,
                        )
                    if cls is BackLink:
                        return LinkInfo(
                            field_name=field_name,
                            lookup_field_name=get_extra_field_info(
                                field, "original_field"
                            ),
                            document_class=DocsRegistry.evaluate_fr(
                                optional_args[0]
                            ),  # type: ignore
                            link_type=LinkTypes.OPTIONAL_BACK_DIRECT,
                        )

                elif (
                    (optional_origin is List or optional_origin is list)
                    and len(optional_args) == 1
                    and isinstance(optional_args[0], _GenericAlias)
                    and optional_args[0].__origin__ is cls
                ):
                    if cls is Link:
                        return LinkInfo(
                            field_name=field_name,
                            lookup_field_name=field_name,
                            document_class=DocsRegistry.evaluate_fr(
                                get_args(optional_args[0])[0]
                            ),  # type: ignore
                            link_type=LinkTypes.OPTIONAL_LIST,
                        )
                    if cls is BackLink:
                        return LinkInfo(
                            field_name=field_name,
                            lookup_field_name=get_extra_field_info(
                                field, "original_field"
                            ),
                            document_class=DocsRegistry.evaluate_fr(
                                get_args(optional_args[0])[0]
                            ),  # type: ignore
                            link_type=LinkTypes.OPTIONAL_BACK_LIST,
                        )
        return None

    def check_nested_links(self, link_info: LinkInfo, current_depth: int):
        if current_depth == 1:
            return
        for k, v in get_model_fields(link_info.document_class).items():
            nested_link_info = self.detect_link(v, k)
            if nested_link_info is None:
                continue

            if link_info.nested_links is None:
                link_info.nested_links = {}
            link_info.nested_links[k] = nested_link_info
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            self.check_nested_links(nested_link_info, current_depth=new_depth)

    # Document

    @staticmethod
    def set_default_class_vars(cls: Type[Document]):
        """
        Set default class variables.

        :param cls: Union[Type[Document], Type[View], Type[UnionDoc]] - Class
        to init settings
        :return:
        """
        cls._children = dict()
        cls._parent = None
        cls._inheritance_inited = False
        cls._class_id = None
        cls._link_fields = None

    @staticmethod
    def init_cache(cls) -> None:
        """
        Init model's cache
        :return: None
        """
        if cls.get_settings().use_cache:
            cls._cache = LRUCache(
                capacity=cls.get_settings().cache_capacity,
                expiration_time=cls.get_settings().cache_expiration_time,
            )

    def init_document_fields(self, cls) -> None:
        """
        Init class fields
        :return: None
        """

        if not IS_PYDANTIC_V2:
            self.update_forward_refs(cls)

        if cls._link_fields is None:
            cls._link_fields = {}
        for k, v in get_model_fields(cls).items():
            path = v.alias or k
            setattr(cls, k, ExpressionField(path))

            link_info = self.detect_link(v, k)
            depth_level = cls.get_settings().max_nesting_depths_per_field.get(
                k, None
            )
            if depth_level is None:
                depth_level = cls.get_settings().max_nesting_depth
            if link_info is not None:
                if depth_level > 0 or depth_level is None:
                    cls._link_fields[k] = link_info
                    self.check_nested_links(
                        link_info, current_depth=depth_level
                    )
                elif depth_level <= 0:
                    link_info.is_fetchable = False
                    cls._link_fields[k] = link_info

        cls._check_hidden_fields()

    @staticmethod
    def init_actions(cls):
        """
        Init event-based actions
        """
        ActionRegistry.clean_actions(cls)
        for attr in dir(cls):
            f = getattr(cls, attr)
            if inspect.isfunction(f):
                if hasattr(f, "has_action"):
                    ActionRegistry.add_action(
                        document_class=cls,
                        event_types=f.event_types,  # type: ignore
                        action_direction=f.action_direction,  # type: ignore
                        funct=f,
                    )

    async def init_document_collection(self, cls):
        """
        Init collection for the Document-based class
        :param cls:
        :return:
        """
        cls.set_database(self.database)

        document_settings = cls.get_settings()

        # register in the Union Doc

        if document_settings.union_doc is not None:
            name = cls.get_settings().name or cls.__name__
            document_settings.name = document_settings.union_doc.register_doc(
                name, cls
            )
            document_settings.union_doc_alias = name

        # set a name

        if not document_settings.name:
            document_settings.name = cls.__name__

        # check mongodb version fits
        if (
            document_settings.timeseries is not None
            and cls._database_major_version < 5
        ):
            raise MongoDBVersionError(
                "Timeseries are supported by MongoDB version 5 and higher"
            )

        # create motor collection
        if (
            document_settings.timeseries is not None
            and document_settings.name
            not in await self.database.list_collection_names(
                authorizedCollections=True, nameOnly=True
            )
        ):
            collection = await self.database.create_collection(
                **document_settings.timeseries.build_query(
                    document_settings.name
                )
            )
        else:
            collection = self.database[document_settings.name]

        cls.set_collection(collection)

    async def init_indexes(self, cls, allow_index_dropping: bool = False):
        """
        Async indexes initializer
        """
        collection = cls.get_motor_collection()
        document_settings = cls.get_settings()

        index_information = await collection.index_information()

        old_indexes = IndexModelField.from_motor_index_information(
            index_information
        )
        new_indexes = []

        # Indexed field wrapped with Indexed()
        indexed_fields = (
            (k, fvalue, get_index_attributes(fvalue))
            for k, fvalue in get_model_fields(cls).items()
        )
        found_indexes = [
            IndexModelField(
                IndexModel(
                    [
                        (
                            fvalue.alias or k,
                            indexed_attrs[0],
                        )
                    ],
                    **indexed_attrs[1],
                )
            )
            for k, fvalue, indexed_attrs in indexed_fields
            if indexed_attrs is not None
        ]

        if document_settings.merge_indexes:
            result: List[IndexModelField] = []
            for subclass in reversed(cls.mro()):
                if issubclass(subclass, Document) and not subclass == Document:
                    if (
                        subclass not in self.inited_classes
                        and not subclass == cls
                    ):
                        await self.init_class(subclass)
                    if subclass.get_settings().indexes:
                        result = IndexModelField.merge_indexes(
                            result, subclass.get_settings().indexes
                        )
            found_indexes = IndexModelField.merge_indexes(
                found_indexes, result
            )

        else:
            if document_settings.indexes:
                found_indexes = IndexModelField.merge_indexes(
                    found_indexes, document_settings.indexes
                )

        new_indexes += found_indexes

        # delete indexes
        # Only drop indexes if the user specifically allows for it
        if allow_index_dropping:
            for index in IndexModelField.list_difference(
                old_indexes, new_indexes
            ):
                await collection.drop_index(index.name)

        # create indices
        if found_indexes:
            new_indexes += await collection.create_indexes(
                IndexModelField.list_to_index_model(new_indexes)
            )

    async def init_document(self, cls: Type[Document]) -> Optional[Output]:
        """
        Init Document-based class

        :param cls:
        :return:
        """
        if cls is Document:
            return None

        # get db version
        build_info = await self.database.command({"buildInfo": 1})
        mongo_version = build_info["version"]
        cls._database_major_version = int(mongo_version.split(".")[0])
        if cls not in self.inited_classes:
            self.set_default_class_vars(cls)
            self.init_settings(cls)

            bases = [b for b in cls.__bases__ if issubclass(b, Document)]
            if len(bases) > 1:
                return None
            parent = bases[0]
            output = await self.init_document(parent)
            if cls.get_settings().is_root and (
                parent is Document or not parent.get_settings().is_root
            ):
                if cls.get_collection_name() is None:
                    cls.set_collection_name(cls.__name__)
                output = Output(
                    class_name=cls.__name__,
                    collection_name=cls.get_collection_name(),
                )
                cls._class_id = cls.__name__
                cls._inheritance_inited = True
            elif output is not None:
                output.class_name = f"{output.class_name}.{cls.__name__}"
                cls._class_id = output.class_name
                cls.set_collection_name(output.collection_name)
                parent.add_child(cls._class_id, cls)
                cls._parent = parent
                cls._inheritance_inited = True

            await self.init_document_collection(cls)
            if not self.skip_indexes:
                await self.init_indexes(cls, self.allow_index_dropping)
            self.init_document_fields(cls)
            self.init_cache(cls)
            self.init_actions(cls)

            self.inited_classes.append(cls)

            return output

        else:
            if cls._inheritance_inited is True:
                return Output(
                    class_name=cls._class_id,
                    collection_name=cls.get_collection_name(),
                )
            else:
                return None

    # Views

    def init_view_fields(self, cls) -> None:
        """
        Init class fields
        :return: None
        """

        if cls._link_fields is None:
            cls._link_fields = {}
        for k, v in get_model_fields(cls).items():
            path = v.alias or k
            setattr(cls, k, ExpressionField(path))
            link_info = self.detect_link(v, k)
            depth_level = cls.get_settings().max_nesting_depths_per_field.get(
                k, None
            )
            if depth_level is None:
                depth_level = cls.get_settings().max_nesting_depth
            if link_info is not None:
                if depth_level > 0:
                    cls._link_fields[k] = link_info
                    self.check_nested_links(
                        link_info, current_depth=depth_level
                    )
                elif depth_level <= 0:
                    link_info.is_fetchable = False
                    cls._link_fields[k] = link_info

    def init_view_collection(self, cls):
        """
        Init collection for View

        :param cls:
        :return:
        """
        view_settings = cls.get_settings()

        if view_settings.name is None:
            view_settings.name = cls.__name__

        if inspect.isclass(view_settings.source):
            view_settings.source = view_settings.source.get_collection_name()

        view_settings.motor_db = self.database
        view_settings.motor_collection = self.database[view_settings.name]

    async def init_view(self, cls: Type[View]):
        """
        Init View-based class

        :param cls:
        :return:
        """
        self.init_settings(cls)
        self.init_view_collection(cls)
        self.init_view_fields(cls)
        self.init_cache(cls)

        collection_names = await self.database.list_collection_names(
            authorizedCollections=True, nameOnly=True
        )
        if self.recreate_views or cls._settings.name not in collection_names:
            if cls._settings.name in collection_names:
                await cls.get_motor_collection().drop()

            await self.database.command(
                {
                    "create": cls.get_settings().name,
                    "viewOn": cls.get_settings().source,
                    "pipeline": cls.get_settings().pipeline,
                }
            )

    # Union Doc

    async def init_union_doc(self, cls: Type[UnionDoc]):
        """
        Init Union Doc based class

        :param cls:
        :return:
        """
        self.init_settings(cls)
        if cls._settings.name is None:
            cls._settings.name = cls.__name__

        cls._settings.motor_db = self.database
        cls._settings.motor_collection = self.database[cls._settings.name]
        cls._is_inited = True

    # Deprecations

    @staticmethod
    def check_deprecations(
        cls: Union[Type[Document], Type[View], Type[UnionDoc]],
    ):
        if hasattr(cls, "Collection"):
            raise Deprecation(
                "Collection inner class is not supported more. "
                "Please use Settings instead. "
                "https://beanie-odm.dev/tutorial/defining-a-document/#settings"
            )

    # Final

    async def init_class(
        self, cls: Union[Type[Document], Type[View], Type[UnionDoc]]
    ):
        """
        Init Document, View or UnionDoc based class.

        :param cls:
        :return:
        """
        self.check_deprecations(cls)

        if issubclass(cls, Document):
            await self.init_document(cls)

        if issubclass(cls, View):
            await self.init_view(cls)

        if issubclass(cls, UnionDoc):
            await self.init_union_doc(cls)

        if hasattr(cls, "custom_init"):
            await cls.custom_init()  # type: ignore


async def init_beanie(
    database: AsyncIOMotorDatabase = None,
    connection_string: Optional[str] = None,
    document_models: Optional[
        Sequence[Union[Type[Document], Type[UnionDoc], Type["View"], str]]
    ] = None,
    allow_index_dropping: bool = False,
    recreate_views: bool = False,
    multiprocessing_mode: bool = False,
    skip_indexes: bool = False,
):
    """
    Beanie initialization

    :param database: AsyncIOMotorDatabase - motor database instance
    :param connection_string: str - MongoDB connection string
    :param document_models: List[Union[Type[DocType], Type[UnionDocType], str]] - model classes
    or strings with dot separated paths
    :param allow_index_dropping: bool - if index dropping is allowed.
    Default False
    :param recreate_views: bool - if views should be recreated. Default False
    :param multiprocessing_mode: bool - if multiprocessing mode is on
        it will patch the motor client to use process's event loop. Default False
    :param skip_indexes: bool - if you want to skip working with the indexes.
        Default False
    :return: None
    """

    await Initializer(
        database=database,
        connection_string=connection_string,
        document_models=document_models,
        allow_index_dropping=allow_index_dropping,
        recreate_views=recreate_views,
        multiprocessing_mode=multiprocessing_mode,
        skip_indexes=skip_indexes,
    )
