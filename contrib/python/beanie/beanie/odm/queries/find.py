from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from motor.motor_asyncio import AsyncIOMotorClientSession
from pydantic import BaseModel
from pymongo import ReplaceOne
from pymongo.results import UpdateResult

from beanie.exceptions import DocumentNotFound
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.cache import LRUCache
from beanie.odm.enums import SortDirection
from beanie.odm.interfaces.aggregation_methods import AggregateMethods
from beanie.odm.interfaces.clone import CloneInterface
from beanie.odm.interfaces.session import SessionMethods
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.operators.find.logical import And
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.cursor import BaseCursorQuery
from beanie.odm.queries.delete import (
    DeleteMany,
    DeleteOne,
)
from beanie.odm.queries.update import (
    UpdateMany,
    UpdateOne,
    UpdateQuery,
    UpdateResponse,
)
from beanie.odm.utils.dump import get_dict
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.find import construct_lookup_queries, split_text_query
from beanie.odm.utils.parsing import parse_obj
from beanie.odm.utils.projection import get_projection
from beanie.odm.utils.relations import convert_ids

if TYPE_CHECKING:
    from beanie.odm.documents import DocType

FindQueryProjectionType = TypeVar("FindQueryProjectionType", bound=BaseModel)
FindQueryResultType = TypeVar("FindQueryResultType", bound=BaseModel)


class FindQuery(
    Generic[FindQueryResultType], UpdateMethods, SessionMethods, CloneInterface
):
    """
    Find Query base class
    """

    UpdateQueryType: Union[
        Type[UpdateQuery], Type[UpdateMany], Type[UpdateOne]
    ] = UpdateQuery
    DeleteQueryType: Union[Type[DeleteOne], Type[DeleteMany]] = DeleteMany
    AggregationQueryType = AggregationQuery

    def __init__(self, document_model: Type["DocType"]):
        self.document_model = document_model
        self.find_expressions: List[Mapping[str, Any]] = []
        self.projection_model: Type[FindQueryResultType] = cast(
            Type[FindQueryResultType], self.document_model
        )
        self.session = None
        self.encoders: Dict[Any, Callable[[Any], Any]] = {}
        self.ignore_cache: bool = False
        self.encoders = self.document_model.get_bson_encoders()
        self.fetch_links: bool = False
        self.pymongo_kwargs: Dict[str, Any] = {}
        self.lazy_parse = False
        self.nesting_depth: Optional[int] = None
        self.nesting_depths_per_field: Optional[Dict[str, int]] = None

    def prepare_find_expressions(self):
        if self.document_model.get_link_fields() is not None:
            for i, query in enumerate(self.find_expressions):
                self.find_expressions[i] = convert_ids(
                    query,
                    doc=self.document_model,  # type: ignore
                    fetch_links=self.fetch_links,
                )

    def get_filter_query(self) -> Mapping[str, Any]:
        """

        Returns: MongoDB filter query

        """
        self.prepare_find_expressions()
        if self.find_expressions:
            return Encoder(custom_encoders=self.encoders).encode(
                And(*self.find_expressions).query
            )
        else:
            return {}

    def delete(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Union[DeleteOne, DeleteMany]:
        """
        Provide search criteria to the Delete query

        :param session: Optional[AsyncIOMotorClientSession]
        :return: Union[DeleteOne, DeleteMany]
        """
        self.set_session(session=session)
        return self.DeleteQueryType(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        ).set_session(session=session)

    def project(self, projection_model):
        """
        Apply projection parameter
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :return: self
        """
        if projection_model is not None:
            self.projection_model = projection_model
        return self

    def get_projection_model(self) -> Type[FindQueryResultType]:
        return self.projection_model

    async def count(self) -> int:
        """
        Number of found documents
        :return: int
        """
        kwargs = {}
        if isinstance(self, FindMany):
            if self.limit_number:
                kwargs["limit"] = self.limit_number
            if self.skip_number:
                kwargs["skip"] = self.skip_number
        return (
            await self.document_model.get_motor_collection().count_documents(
                self.get_filter_query(), session=self.session, **kwargs
            )
        )

    async def exists(self) -> bool:
        """
        If find query will return anything

        :return: bool
        """
        return await self.count() > 0


class FindMany(
    FindQuery[FindQueryResultType],
    BaseCursorQuery[FindQueryResultType],
    AggregateMethods,
):
    """
    Find Many query class
    """

    UpdateQueryType = UpdateMany
    DeleteQueryType = DeleteMany

    def __init__(self, document_model: Type["DocType"]):
        super(FindMany, self).__init__(document_model=document_model)
        self.sort_expressions: List[Tuple[str, SortDirection]] = []
        self.skip_number: int = 0
        self.limit_number: int = 0

    @overload
    def find_many(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: None = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindMany[FindQueryResultType]": ...

    @overload
    def find_many(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindMany[FindQueryProjectionType]": ...

    def find_many(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[
        "FindMany[FindQueryResultType]", "FindMany[FindQueryProjectionType]"
    ]:
        """
        Find many documents by criteria

        :param args: *Mapping[str, Any] - search criteria
        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key
        or a list of (key, direction) pairs specifying the sort order
        for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: FindMany - query instance
        """
        self.find_expressions += args  # type: ignore # bool workaround
        self.skip(skip)
        self.limit(limit)
        self.sort(sort)
        self.project(projection_model)
        self.set_session(session=session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        self.nesting_depth = nesting_depth
        self.nesting_depths_per_field = nesting_depths_per_field
        if lazy_parse is True:
            self.lazy_parse = lazy_parse
        return self

    # TODO probably merge FindOne and FindMany to one class to avoid this
    #  code duplication

    @overload
    def project(
        self: "FindMany",
        projection_model: None,
    ) -> "FindMany[FindQueryResultType]": ...

    @overload
    def project(
        self: "FindMany",
        projection_model: Type[FindQueryProjectionType],
    ) -> "FindMany[FindQueryProjectionType]": ...

    def project(
        self: "FindMany",
        projection_model: Optional[Type[FindQueryProjectionType]],
    ) -> Union[
        "FindMany[FindQueryResultType]", "FindMany[FindQueryProjectionType]"
    ]:
        """
        Apply projection parameter

        :param projection_model: Optional[Type[BaseModel]] - projection model
        :return: self
        """
        super().project(projection_model)
        return self

    @overload
    def find(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: None = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindMany[FindQueryResultType]": ...

    @overload
    def find(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindMany[FindQueryProjectionType]": ...

    def find(
        self: "FindMany[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[
        "FindMany[FindQueryResultType]", "FindMany[FindQueryProjectionType]"
    ]:
        """
        The same as `find_many(...)`
        """
        return self.find_many(
            *args,
            skip=skip,
            limit=limit,
            sort=sort,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )

    def sort(
        self,
        *args: Optional[
            Union[
                str, Tuple[str, SortDirection], List[Tuple[str, SortDirection]]
            ]
        ],
    ) -> "FindMany[FindQueryResultType]":
        """
        Add sort parameters
        :param args: Union[str, Tuple[str, SortDirection],
        List[Tuple[str, SortDirection]]] - A key or a tuple (key, direction)
        or a list of (key, direction) pairs specifying
        the sort order for this query.
        :return: self
        """
        for arg in args:
            if arg is None:
                pass
            elif isinstance(arg, list):
                self.sort(*arg)
            elif isinstance(arg, tuple):
                self.sort_expressions.append(arg)
            elif isinstance(arg, str):
                if arg.startswith("+"):
                    self.sort_expressions.append(
                        (arg[1:], SortDirection.ASCENDING)
                    )
                elif arg.startswith("-"):
                    self.sort_expressions.append(
                        (arg[1:], SortDirection.DESCENDING)
                    )
                else:
                    self.sort_expressions.append(
                        (arg, SortDirection.ASCENDING)
                    )
            else:
                raise TypeError("Wrong argument type")
        return self

    def skip(self, n: Optional[int]) -> "FindMany[FindQueryResultType]":
        """
        Set skip parameter
        :param n: int
        :return: self
        """
        if n is not None:
            self.skip_number = n
        return self

    def limit(self, n: Optional[int]) -> "FindMany[FindQueryResultType]":
        """
        Set limit parameter
        :param n: int
        :return:
        """
        if n is not None:
            self.limit_number = n
        return self

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :return: UpdateMany query
        """
        self.set_session(session)
        return (
            self.UpdateQueryType(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .update(*args, bulk_writer=bulk_writer, **pymongo_kwargs)
            .set_session(session=self.session)
        )

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[AsyncIOMotorClientSession] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[AsyncIOMotorClientSession]
        :return: UpdateMany query
        """
        self.set_session(session)
        return (
            self.UpdateQueryType(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .upsert(
                *args,
                on_insert=on_insert,
                **pymongo_kwargs,
            )
            .set_session(session=self.session)
        )

    def update_many(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateMany:
        """
        Provide search criteria to the
        [UpdateMany](query.md#updatemany) query

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :return: [UpdateMany](query.md#updatemany) query
        """
        return cast(
            UpdateMany,
            self.update(
                *args,
                session=session,
                bulk_writer=bulk_writer,
                **pymongo_kwargs,
            ),
        )

    def delete_many(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> DeleteMany:
        """
        Provide search criteria to the [DeleteMany](query.md#deletemany) query

        :param session:
        :return: [DeleteMany](query.md#deletemany) query
        """
        # We need to cast here to tell mypy that we are sure about the type.
        # This is because delete may also return a DeleteOne type in general, and mypy can not be sure in this case
        # See https://mypy.readthedocs.io/en/stable/common_issues.html#narrowing-and-inner-functions
        return cast(
            DeleteMany,
            self.delete(
                session=session, bulk_writer=bulk_writer, **pymongo_kwargs
            ),
        )

    @overload
    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: None = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> AggregationQuery[Dict[str, Any]]: ...

    @overload
    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: Type[FindQueryProjectionType],
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> AggregationQuery[FindQueryProjectionType]: ...

    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[
        AggregationQuery[Dict[str, Any]],
        AggregationQuery[FindQueryProjectionType],
    ]:
        """
        Provide search criteria to the [AggregationQuery](query.md#aggregationquery)

        :param aggregation_pipeline: list - aggregation pipeline. MongoDB doc:
        <https://docs.mongodb.com/manual/core/aggregation-pipeline/>
        :param projection_model: Type[BaseModel] - Projection Model
        :param session: Optional[AsyncIOMotorClientSession] - PyMongo session
        :param ignore_cache: bool
        :return:[AggregationQuery](query.md#aggregationquery)
        """
        self.set_session(session=session)
        return self.AggregationQueryType(
            self.document_model,
            self.build_aggregation_pipeline(*aggregation_pipeline),
            find_query={},
            projection_model=projection_model,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        ).set_session(session=self.session)

    @property
    def _cache_key(self) -> str:
        return LRUCache.create_key(
            {
                "type": "FindMany",
                "filter": self.get_filter_query(),
                "sort": self.sort_expressions,
                "projection": get_projection(self.projection_model),
                "skip": self.skip_number,
                "limit": self.limit_number,
            }
        )

    def _get_cache(self):
        if (
            self.document_model.get_settings().use_cache
            and self.ignore_cache is False
        ):
            return self.document_model._cache.get(self._cache_key)  # type: ignore
        else:
            return None

    def _set_cache(self, data):
        if (
            self.document_model.get_settings().use_cache
            and self.ignore_cache is False
        ):
            return self.document_model._cache.set(self._cache_key, data)  # type: ignore

    def build_aggregation_pipeline(self, *extra_stages):
        if self.fetch_links:
            aggregation_pipeline: List[Dict[str, Any]] = (
                construct_lookup_queries(
                    self.document_model,
                    nesting_depth=self.nesting_depth,
                    nesting_depths_per_field=self.nesting_depths_per_field,
                )
            )
        else:
            aggregation_pipeline = []
        filter_query = self.get_filter_query()

        if filter_query:
            text_queries, non_text_queries = split_text_query(filter_query)

            if text_queries:
                aggregation_pipeline.insert(
                    0,
                    {
                        "$match": (
                            {"$and": text_queries}
                            if len(text_queries) > 1
                            else text_queries[0]
                        )
                    },
                )

            if non_text_queries:
                aggregation_pipeline.append(
                    {
                        "$match": (
                            {"$and": non_text_queries}
                            if len(non_text_queries) > 1
                            else non_text_queries[0]
                        )
                    }
                )

        if extra_stages:
            aggregation_pipeline.extend(extra_stages)
        sort_pipeline = {"$sort": {i[0]: i[1] for i in self.sort_expressions}}
        if sort_pipeline["$sort"]:
            aggregation_pipeline.append(sort_pipeline)
        if self.skip_number != 0:
            aggregation_pipeline.append({"$skip": self.skip_number})
        if self.limit_number != 0:
            aggregation_pipeline.append({"$limit": self.limit_number})
        return aggregation_pipeline

    @property
    def motor_cursor(self):
        if self.fetch_links:
            aggregation_pipeline: List[Dict[str, Any]] = (
                self.build_aggregation_pipeline()
            )

            projection = get_projection(self.projection_model)

            if projection is not None:
                aggregation_pipeline.append({"$project": projection})

            return self.document_model.get_motor_collection().aggregate(
                aggregation_pipeline,
                session=self.session,
                **self.pymongo_kwargs,
            )

        return self.document_model.get_motor_collection().find(
            filter=self.get_filter_query(),
            sort=self.sort_expressions,
            projection=get_projection(self.projection_model),
            skip=self.skip_number,
            limit=self.limit_number,
            session=self.session,
            **self.pymongo_kwargs,
        )

    async def first_or_none(self) -> Optional[FindQueryResultType]:
        """
        Returns the first found element or None if no elements were found
        """
        res = await self.limit(1).to_list()
        if not res:
            return None
        return res[0]

    async def count(self) -> int:
        """
        Number of found documents
        :return: int
        """
        if self.fetch_links:
            aggregation_pipeline: List[Dict[str, Any]] = (
                self.build_aggregation_pipeline()
            )

            aggregation_pipeline.append({"$count": "count"})

            result = (
                await self.document_model.get_motor_collection()
                .aggregate(
                    aggregation_pipeline,
                    session=self.session,
                    **self.pymongo_kwargs,
                )
                .to_list(length=1)
            )

            return result[0]["count"] if result else 0

        return await super(FindMany, self).count()


class FindOne(FindQuery[FindQueryResultType]):
    """
    Find One query class
    """

    UpdateQueryType = UpdateOne
    DeleteQueryType = DeleteOne

    @overload
    def project(
        self: "FindOne[FindQueryResultType]",
        projection_model: None = None,
    ) -> "FindOne[FindQueryResultType]": ...

    @overload
    def project(
        self: "FindOne[FindQueryResultType]",
        projection_model: Type[FindQueryProjectionType],
    ) -> "FindOne[FindQueryProjectionType]": ...

    # TODO probably merge FindOne and FindMany to one class to avoid this
    #  code duplication

    def project(
        self: "FindOne[FindQueryResultType]",
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
    ) -> Union[
        "FindOne[FindQueryResultType]", "FindOne[FindQueryProjectionType]"
    ]:
        """
        Apply projection parameter
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :return: self
        """
        super().project(projection_model)
        return self

    @overload
    def find_one(
        self: "FindOne[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: None = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindOne[FindQueryResultType]": ...

    @overload
    def find_one(
        self: "FindOne[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Type[FindQueryProjectionType],
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> "FindOne[FindQueryProjectionType]": ...

    def find_one(
        self: "FindOne[FindQueryResultType]",
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        nesting_depth: Optional[int] = None,
        nesting_depths_per_field: Optional[Dict[str, int]] = None,
        **pymongo_kwargs: Any,
    ) -> Union[
        "FindOne[FindQueryResultType]", "FindOne[FindQueryProjectionType]"
    ]:
        """
        Find one document by criteria

        :param args: *Mapping[str, Any] - search criteria
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: FindOne - query instance
        """
        self.find_expressions += args  # type: ignore # bool workaround
        self.project(projection_model)
        self.set_session(session=session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        self.nesting_depth = nesting_depth
        self.nesting_depths_per_field = nesting_depths_per_field
        return self

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateMany query
        """
        self.set_session(session)
        return (
            self.UpdateQueryType(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .update(
                *args,
                bulk_writer=bulk_writer,
                response_type=response_type,
                **pymongo_kwargs,
            )
            .set_session(session=self.session)
        )

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[AsyncIOMotorClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[AsyncIOMotorClientSession]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateMany query
        """
        self.set_session(session)
        return (
            self.UpdateQueryType(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .upsert(
                *args,
                on_insert=on_insert,
                response_type=response_type,
                **pymongo_kwargs,
            )
            .set_session(session=self.session)
        )

    def update_one(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateOne:
        """
        Create [UpdateOne](query.md#updateone) query using modifications and
        provide search criteria there
        :param args: *Mapping[str,Any] - the modifications to apply
        :param session: Optional[AsyncIOMotorClientSession] - PyMongo sessions
        :param response_type: Optional[UpdateResponse]
        :return: [UpdateOne](query.md#updateone) query
        """
        return cast(
            UpdateOne,
            self.update(
                *args,
                session=session,
                bulk_writer=bulk_writer,
                response_type=response_type,
                **pymongo_kwargs,
            ),
        )

    def delete_one(
        self,
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> DeleteOne:
        """
        Provide search criteria to the [DeleteOne](query.md#deleteone) query
        :param session: Optional[AsyncIOMotorClientSession] - PyMongo sessions
        :return: [DeleteOne](query.md#deleteone) query
        """
        # We need to cast here to tell mypy that we are sure about the type.
        # This is because delete may also return a DeleteOne type in general, and mypy can not be sure in this case
        # See https://mypy.readthedocs.io/en/stable/common_issues.html#narrowing-and-inner-functions
        return cast(
            DeleteOne,
            self.delete(
                session=session, bulk_writer=bulk_writer, **pymongo_kwargs
            ),
        )

    async def replace_one(
        self,
        document: "DocType",
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
    ) -> Optional[UpdateResult]:
        """
        Replace found document by provided
        :param document: Document - document, which will replace the found one
        :param session: Optional[AsyncIOMotorClientSession] - PyMongo session
        :param bulk_writer: Optional[BulkWriter] - Beanie bulk writer
        :return: UpdateResult
        """
        self.set_session(session=session)
        if bulk_writer is None:
            result: UpdateResult = (
                await self.document_model.get_motor_collection().replace_one(
                    self.get_filter_query(),
                    get_dict(
                        document,
                        to_db=True,
                        exclude={"_id"},
                        keep_nulls=document.get_settings().keep_nulls,
                    ),
                    session=self.session,
                )
            )

            if not result.raw_result["updatedExisting"]:
                raise DocumentNotFound
            return result
        else:
            bulk_writer.add_operation(
                Operation(
                    operation=ReplaceOne,
                    first_query=self.get_filter_query(),
                    second_query=get_dict(
                        document,
                        to_db=True,
                        exclude={"_id"},
                        keep_nulls=document.get_settings().keep_nulls,
                    ),
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None

    async def _find_one(self):
        if self.fetch_links:
            return await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                projection_model=self.projection_model,
                nesting_depth=self.nesting_depth,
                nesting_depths_per_field=self.nesting_depths_per_field,
                **self.pymongo_kwargs,
            ).first_or_none()
        return await self.document_model.get_motor_collection().find_one(
            filter=self.get_filter_query(),
            projection=get_projection(self.projection_model),
            session=self.session,
            **self.pymongo_kwargs,
        )

    def __await__(
        self,
    ) -> Generator[Coroutine, Any, Optional[FindQueryResultType]]:
        """
        Run the query
        :return: BaseModel
        """
        # projection = get_projection(self.projection_model)
        if (
            self.document_model.get_settings().use_cache
            and self.ignore_cache is False
        ):
            cache_key = LRUCache.create_key(
                "FindOne",
                self.get_filter_query(),
                self.projection_model,
                self.session,
                self.fetch_links,
            )
            document: Dict[str, Any] = self.document_model._cache.get(  # type: ignore
                cache_key
            )
            if document is None:
                document = yield from self._find_one().__await__()  # type: ignore
                self.document_model._cache.set(cache_key, document)  # type: ignore
        else:
            document = yield from self._find_one().__await__()  # type: ignore
        if document is None:
            return None
        if type(document) is self.projection_model:
            return cast(FindQueryResultType, document)
        return cast(
            FindQueryResultType, parse_obj(self.projection_model, document)
        )

    async def count(self) -> int:
        """
        Count the number of documents matching the query
        :return: int
        """
        if self.fetch_links:
            return await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                **self.pymongo_kwargs,
            ).count()
        return await super(FindOne, self).count()
