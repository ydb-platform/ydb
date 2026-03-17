from __future__ import annotations

import enum
from collections.abc import Collection, Mapping
from dataclasses import is_dataclass
from datetime import date, datetime
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    ClassVar,
    Generic,
    Protocol,
    TypeVar,
    Union,
    cast,
)

from sqlalchemy.util.langhelpers import duck_type_collection

from polyfactory.exceptions import ConfigurationException, MissingDependencyException, ParameterException
from polyfactory.factories.base import BaseFactory
from polyfactory.factories.base import BuildContext as BaseBuildContext
from polyfactory.field_meta import Constraints, FieldMeta
from polyfactory.persistence import AsyncPersistenceProtocol, SyncPersistenceProtocol
from polyfactory.utils.types import Frozendict

try:
    from sqlalchemy import ARRAY, Column, Numeric, String, inspect, types
    from sqlalchemy.dialects import mssql, mysql, postgresql, sqlite
    from sqlalchemy.exc import NoInspectionAvailable
    from sqlalchemy.ext.associationproxy import AssociationProxy
    from sqlalchemy.orm import InstanceState, Mapper, RelationshipProperty
except ImportError as e:
    msg = "sqlalchemy is not installed"
    raise MissingDependencyException(msg) from e

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
    from sqlalchemy.orm import Session, scoped_session
    from sqlalchemy.sql.type_api import TypeEngine
    from typing_extensions import NotRequired, TypeGuard


T = TypeVar("T")


class SQLAlchemyBuildContext(BaseBuildContext):
    skip_computed_fields: bool


class SQLAlchemyConstraints(Constraints):
    computed: NotRequired[bool]


class SQLAlchemyPersistenceMethod(enum.Enum):
    FLUSH = "flush"
    COMMIT = "commit"


class SQLASyncPersistence(SyncPersistenceProtocol[T]):
    def __init__(
        self,
        session: Session,
        persistence_method: SQLAlchemyPersistenceMethod = SQLAlchemyPersistenceMethod.COMMIT,
    ) -> None:
        """Sync persistence handler for SQLAFactory."""
        self.session = session
        self.persistence_method = persistence_method

    def _flush_or_commit(self) -> None:
        if self.persistence_method == SQLAlchemyPersistenceMethod.FLUSH:
            self.session.flush()
        elif self.persistence_method == SQLAlchemyPersistenceMethod.COMMIT:
            self.session.commit()

    def save(self, data: T) -> T:
        self.session.add(data)
        self._flush_or_commit()
        return data

    def save_many(self, data: list[T]) -> list[T]:
        self.session.add_all(data)
        self._flush_or_commit()
        return data


class SQLAASyncPersistence(AsyncPersistenceProtocol[T]):
    def __init__(
        self,
        session: AsyncSession,
        persistence_method: SQLAlchemyPersistenceMethod = SQLAlchemyPersistenceMethod.COMMIT,
    ) -> None:
        """Async persistence handler for SQLAFactory."""
        self.session = session
        self.persistence_method = persistence_method

    async def _flush_or_commit(self, session: AsyncSession) -> None:
        if self.persistence_method == SQLAlchemyPersistenceMethod.FLUSH:
            await session.flush()
        elif self.persistence_method == SQLAlchemyPersistenceMethod.COMMIT:
            await session.commit()

    async def save(self, data: T) -> T:
        self.session.add(data)
        await self._flush_or_commit(self.session)
        await self.session.refresh(data)
        return data

    async def save_many(self, data: list[T]) -> list[T]:
        self.session.add_all(data)
        await self._flush_or_commit(self.session)
        for batch_item in data:
            await self.session.refresh(batch_item)
        return data


_T_co = TypeVar("_T_co", covariant=True)


class _SessionMaker(Protocol[_T_co]):
    @staticmethod
    def __call__() -> _T_co: ...


class SQLAlchemyFactory(Generic[T], BaseFactory[T]):
    """Base factory for SQLAlchemy models."""

    __is_base_factory__ = True

    __set_primary_key__: ClassVar[bool] = True
    """Configuration to consider primary key columns as a field or not."""
    __set_foreign_keys__: ClassVar[bool] = True
    """Configuration to consider columns with foreign keys as a field or not."""
    __set_relationships__: ClassVar[bool] = True
    """Configuration to consider relationships property as a model field or not."""
    __set_association_proxy__: ClassVar[bool] = True
    """Configuration to consider AssociationProxy property as a model field or not."""

    __session__: ClassVar[Session | _SessionMaker[Session] | scoped_session[Session] | None] = None
    __async_session__: ClassVar[
        AsyncSession | _SessionMaker[AsyncSession] | async_scoped_session[AsyncSession] | None
    ] = None
    __persistence_method__: ClassVar[SQLAlchemyPersistenceMethod] = SQLAlchemyPersistenceMethod.COMMIT
    """Configuration to use flush() or commit() for persistence."""

    __config_keys__ = (
        *BaseFactory.__config_keys__,
        "__set_primary_key__",
        "__set_foreign_keys__",
        "__set_relationships__",
        "__set_association_proxy__",
        "__persistence_method__",
    )

    @classmethod
    def _get_build_context(
        cls, build_context: BaseBuildContext | SQLAlchemyBuildContext | None
    ) -> SQLAlchemyBuildContext:
        build_context = cast("SQLAlchemyBuildContext", super()._get_build_context(build_context))
        if build_context.get("skip_computed_fields") is None:
            build_context["skip_computed_fields"] = False

        return build_context

    @classmethod
    def create_sync(cls, **kwargs: Any) -> T:
        build_context = cls._get_build_context(kwargs.get("_build_context"))
        build_context["skip_computed_fields"] = True
        kwargs["_build_context"] = build_context
        return super().create_sync(**kwargs)

    @classmethod
    async def create_async(cls, **kwargs: Any) -> T:
        build_context = cls._get_build_context(kwargs.get("_build_context"))
        build_context["skip_computed_fields"] = True
        kwargs["_build_context"] = build_context
        return await super().create_async(**kwargs)

    @classmethod
    def get_sqlalchemy_types(cls) -> dict[Any, Callable[[], Any]]:
        """Get mapping of types where column type should be used directly.

        For sqlalchemy dialect `JSON` type, accepted only basic types in pydict in case sqlalchemy process `JSON` raise serialize error.
        """
        return {
            types.TupleType: cls.__faker__.pytuple,
            mssql.JSON: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            mysql.YEAR: lambda: cls.__random__.randint(1901, 2155),
            mysql.JSON: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            postgresql.CIDR: lambda: cls.__faker__.ipv4(network=True),
            postgresql.DATERANGE: lambda: (cls.__faker__.past_date(), date.today()),  # noqa: DTZ011
            postgresql.INET: lambda: cls.__faker__.ipv4(network=False),
            postgresql.INT4RANGE: lambda: tuple(sorted([cls.__faker__.pyint(), cls.__faker__.pyint()])),
            postgresql.INT8RANGE: lambda: tuple(sorted([cls.__faker__.pyint(), cls.__faker__.pyint()])),
            postgresql.MACADDR: lambda: cls.__faker__.hexify(text="^^:^^:^^:^^:^^:^^", upper=True),
            postgresql.NUMRANGE: lambda: tuple(sorted([cls.__faker__.pyint(), cls.__faker__.pyint()])),
            postgresql.TSRANGE: lambda: (cls.__faker__.past_datetime(), datetime.now()),  # noqa: DTZ005
            postgresql.TSTZRANGE: lambda: (cls.__faker__.past_datetime(), datetime.now()),  # noqa: DTZ005
            postgresql.HSTORE: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            postgresql.JSON: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            postgresql.JSONB: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            sqlite.JSON: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
            types.JSON: lambda: cls.__faker__.pydict(value_types=(str, int, bool, float)),
        }

    @classmethod
    def get_sqlalchemy_constraints(cls) -> dict[type[TypeEngine], dict[str, str]]:
        """Get mapping of SQLA type engine to attribute to constraints key."""
        return {
            String: {
                "length": "max_length",
            },
            Numeric: {
                "precision": "max_digits",
                "scale": "decimal_places",
            },
        }

    @classmethod
    def get_provider_map(cls) -> dict[Any, Callable[[], Any]]:
        providers_map = super().get_provider_map()
        providers_map.update(cls.get_sqlalchemy_types())
        return providers_map

    @classmethod
    def is_supported_type(cls, value: Any) -> TypeGuard[type[T]]:
        try:
            inspected = inspect(value)
        except NoInspectionAvailable:
            return False
        return isinstance(inspected, (Mapper, InstanceState))

    @classmethod
    def should_set_field_value(cls, field_meta: FieldMeta, **kwargs: Any) -> bool:
        build_context = kwargs.get("_build_context", {})

        if field_meta.constraints:
            constraints = cast("SQLAlchemyConstraints", field_meta.constraints)
            if constraints.get("computed") and build_context.get("skip_computed_fields"):
                return False

        return super().should_set_field_value(field_meta, **kwargs)

    @classmethod
    def should_column_be_set(cls, column: Any) -> bool:
        if not isinstance(column, Column):
            return False

        if not cls.__set_primary_key__ and column.primary_key:
            return False

        if not cls.should_dataclass_init_field(column.name):
            return False

        return bool(cls.__set_foreign_keys__ or not column.foreign_keys)

    @classmethod
    def should_dataclass_init_field(cls, field_name: str) -> bool:
        if not is_dataclass(cls.__model__):
            return True

        dataclass_fields = cls.__model__.__dataclass_fields__
        try:
            return dataclass_fields[field_name].init
        except KeyError:
            return True

    @classmethod
    def _get_type_from_type_engine(cls, type_engine: TypeEngine) -> type:
        if type(type_engine) in cls.get_sqlalchemy_types():
            return type(type_engine)

        annotation: type
        try:
            annotation = type_engine.python_type
        except NotImplementedError:
            if not hasattr(type_engine, "impl"):
                msg = f"Unsupported type engine: {type_engine}.\nOverride get_sqlalchemy_types to support"
                raise ParameterException(msg) from None
            annotation = type_engine.impl.python_type  # pyright: ignore[reportAttributeAccessIssue]

        constraints: SQLAlchemyConstraints = {}
        for type_, constraint_fields in cls.get_sqlalchemy_constraints().items():
            if not isinstance(type_engine, type_):
                continue
            for sqlalchemy_field, constraint_field in constraint_fields.items():
                if (value := getattr(type_engine, sqlalchemy_field, None)) is not None:
                    constraints[constraint_field] = value  # type: ignore[literal-required]
        if constraints:
            annotation = Annotated[annotation, Frozendict(constraints)]  # type: ignore[assignment]

        return annotation

    @classmethod
    def get_type_from_column(cls, column: Column) -> type:
        annotation: type
        if isinstance(column.type, (ARRAY, postgresql.ARRAY)):
            item_type = cls._get_type_from_type_engine(column.type.item_type)
            annotation = list[item_type]  # type: ignore[valid-type]
        else:
            annotation = cls._get_type_from_type_engine(column.type)

        if column.nullable:
            annotation = Union[annotation, None]  # type: ignore[assignment]

        if column.computed:
            constraints: SQLAlchemyConstraints = {"computed": True}
            annotation = Annotated[annotation, Frozendict(constraints)]  # type: ignore[assignment]

        return annotation

    @classmethod
    def get_type_from_collection_class(
        cls,
        collection_class: type[Collection[Any]] | Callable[[], Collection[Any]],
        entity_class: Any,
    ) -> type[Any]:
        annotation: type[Any]

        if isinstance(collection_class, type):
            if issubclass(collection_class, Mapping):
                annotation = dict[Any, entity_class]
            else:
                if not (duck_typed_as := duck_type_collection(collection_class)):
                    msg = f"Cannot infer type from collection_class {collection_class}"
                    raise ConfigurationException(
                        msg,
                    )

                annotation = duck_typed_as[entity_class]  # pyright: ignore[reportIndexIssue]
        else:
            annotation = dict[Any, entity_class]

        return annotation

    @classmethod
    def _get_relationship_type(cls, relationship: RelationshipProperty[Any]) -> type:
        class_ = relationship.entity.class_
        annotation: type

        if relationship.uselist:
            collection_class = relationship.collection_class
            if collection_class is None:
                annotation = list[class_]  # type: ignore[valid-type]
            else:
                annotation = cls.get_type_from_collection_class(collection_class, class_)
        else:
            annotation = class_

        return annotation

    @classmethod
    def _get_association_proxy_type(cls, table: Mapper, proxy: AssociationProxy) -> type | None:
        target_collection = table.relationships.get(proxy.target_collection)
        if not target_collection:
            return None

        target_class = target_collection.entity.class_
        target_attr = getattr(target_class, proxy.value_attr)
        if not target_attr:
            return None

        class_ = target_attr.entity.class_
        return class_ if not target_collection.uselist else list[class_]  # type: ignore[valid-type]

    @classmethod
    def get_model_fields(cls) -> list[FieldMeta]:
        fields_meta: list[FieldMeta] = []

        table: Mapper = inspect(cls.__model__)  # type: ignore[assignment]
        fields_meta.extend(
            FieldMeta.from_type(
                annotation=cls.get_type_from_column(column),
                name=name,
            )
            for name, column in table.columns.items()
            if cls.should_column_be_set(column)
        )
        if cls.__set_relationships__:
            for name, relationship in table.relationships.items():
                if not cls.should_dataclass_init_field(name):
                    continue

                annotation = cls._get_relationship_type(relationship)
                fields_meta.append(
                    FieldMeta.from_type(
                        name=name,
                        annotation=annotation,
                    )
                )
        if cls.__set_association_proxy__:
            for name, attr in table.all_orm_descriptors.items():
                if isinstance(attr, AssociationProxy):
                    if not cls.should_dataclass_init_field(name):
                        continue

                    # Read-only proxies derive from the underlying relationship and shouldn't be set directly.
                    if not getattr(attr, "creator", None):
                        continue

                    if annotation := cls._get_association_proxy_type(table, attr):  # type: ignore[assignment]
                        fields_meta.append(
                            FieldMeta.from_type(
                                name=name,
                                annotation=annotation,
                            )
                        )

        return fields_meta

    @classmethod
    def _get_sync_persistence(cls) -> SyncPersistenceProtocol[T]:
        if cls.__session__ is not None:
            session = cls.__session__() if callable(cls.__session__) else cls.__session__
            return SQLASyncPersistence(session, persistence_method=cls.__persistence_method__)
        return super()._get_sync_persistence()

    @classmethod
    def _get_async_persistence(cls) -> AsyncPersistenceProtocol[T]:
        if cls.__async_session__ is not None:
            session = cls.__async_session__() if callable(cls.__async_session__) else cls.__async_session__
            return SQLAASyncPersistence(session, persistence_method=cls.__persistence_method__)
        return super()._get_async_persistence()
