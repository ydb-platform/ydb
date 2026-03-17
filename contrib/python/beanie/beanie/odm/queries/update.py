from abc import abstractmethod
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)

from motor.motor_asyncio import AsyncIOMotorClientSession
from pymongo import ReturnDocument
from pymongo import UpdateMany as UpdateManyPyMongo
from pymongo import UpdateOne as UpdateOnePyMongo
from pymongo.results import InsertOneResult, UpdateResult

from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.interfaces.clone import CloneInterface
from beanie.odm.interfaces.session import SessionMethods
from beanie.odm.interfaces.update import (
    UpdateMethods,
)
from beanie.odm.operators.update import BaseUpdateOperator
from beanie.odm.operators.update.general import SetRevisionId
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import parse_obj

if TYPE_CHECKING:
    from beanie.odm.documents import DocType


class UpdateResponse(str, Enum):
    UPDATE_RESULT = "UPDATE_RESULT"  # PyMongo update result
    OLD_DOCUMENT = "OLD_DOCUMENT"  # Original document
    NEW_DOCUMENT = "NEW_DOCUMENT"  # Updated document


class UpdateQuery(UpdateMethods, SessionMethods, CloneInterface):
    """
    Update Query base class
    """

    def __init__(
        self,
        document_model: Type["DocType"],
        find_query: Mapping[str, Any],
    ):
        self.document_model = document_model
        self.find_query = find_query
        self.update_expressions: List[Mapping[str, Any]] = []
        self.session = None
        self.is_upsert = False
        self.upsert_insert_doc: Optional["DocType"] = None
        self.encoders: Dict[Any, Callable[[Any], Any]] = {}
        self.bulk_writer: Optional[BulkWriter] = None
        self.encoders = self.document_model.get_settings().bson_encoders
        self.pymongo_kwargs: Dict[str, Any] = {}

    @property
    def update_query(self) -> Dict[str, Any]:
        query: Union[Dict[str, Any], List[Dict[str, Any]], None] = None
        for expression in self.update_expressions:
            if isinstance(expression, BaseUpdateOperator):
                if query is None:
                    query = {}
                if isinstance(query, list):
                    raise TypeError("Wrong expression type")
                query.update(expression.query)
            elif isinstance(expression, dict):
                if query is None:
                    query = {}
                if isinstance(query, list):
                    raise TypeError("Wrong expression type")
                query.update(expression)
            elif isinstance(expression, SetRevisionId):
                if query is None:
                    query = {}
                if isinstance(query, list):
                    raise TypeError("Wrong expression type")
                set_query = query.get("$set", {})
                set_query.update(expression.query.get("$set", {}))
                query["$set"] = set_query
            elif isinstance(expression, list):
                if query is None:
                    query = []
                if isinstance(query, dict):
                    raise TypeError("Wrong expression type")
                query.extend(expression)
            else:
                raise TypeError("Wrong expression type")
        return Encoder(custom_encoders=self.encoders).encode(query)

    @abstractmethod
    async def _update(self) -> UpdateResult: ...


class UpdateMany(UpdateQuery):
    """
    Update Many query class
    """

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        self.set_session(session=session)
        self.update_expressions += args
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[AsyncIOMotorClientSession] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Provide modifications to the upsert query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[AsyncIOMotorClientSession]
        :param **pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        self.upsert_insert_doc = on_insert  # type: ignore
        self.update(*args, session=session, **pymongo_kwargs)
        return self

    def update_many(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Provide modifications to the update query

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        return self.update(
            *args, session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    async def _update(self):
        if self.bulk_writer is None:
            return (
                await self.document_model.get_motor_collection().update_many(
                    self.find_query,
                    self.update_query,
                    session=self.session,
                    **self.pymongo_kwargs,
                )
            )
        else:
            self.bulk_writer.add_operation(
                Operation(
                    operation=UpdateManyPyMongo,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )

    def __await__(
        self,
    ) -> Generator[
        Any, None, Union[UpdateResult, InsertOneResult, Optional["DocType"]]
    ]:
        """
        Run the query
        :return:
        """

        update_result = yield from self._update().__await__()
        if self.upsert_insert_doc is None:
            return update_result

        if update_result is not None and update_result.matched_count == 0:
            return (
                yield from self.document_model.insert_one(
                    document=self.upsert_insert_doc,
                    session=self.session,
                    bulk_writer=self.bulk_writer,
                ).__await__()
            )

        return update_result


class UpdateOne(UpdateQuery):
    """
    Update One query class
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super(UpdateOne, self).__init__(*args, **kwargs)
        self.response_type = UpdateResponse.UPDATE_RESULT

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: UpdateResponse
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        self.set_session(session=session)
        self.update_expressions += args
        if response_type is not None:
            self.response_type = response_type
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[AsyncIOMotorClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Provide modifications to the upsert query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[AsyncIOMotorClientSession]
        :param response_type: Optional[UpdateResponse]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        self.upsert_insert_doc = on_insert  # type: ignore
        self.update(
            *args,
            response_type=response_type,
            session=session,
            **pymongo_kwargs,
        )
        return self

    def update_one(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ):
        """
        Provide modifications to the update query. The same as `update()`

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[AsyncIOMotorClientSession]
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param response_type: Optional[UpdateResponse]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: UpdateMany query
        """
        return self.update(
            *args,
            session=session,
            bulk_writer=bulk_writer,
            response_type=response_type,
            **pymongo_kwargs,
        )

    async def _update(self):
        if not self.bulk_writer:
            if self.response_type == UpdateResponse.UPDATE_RESULT:
                return await self.document_model.get_motor_collection().update_one(
                    self.find_query,
                    self.update_query,
                    session=self.session,
                    **self.pymongo_kwargs,
                )
            else:
                result = await self.document_model.get_motor_collection().find_one_and_update(
                    self.find_query,
                    self.update_query,
                    session=self.session,
                    return_document=(
                        ReturnDocument.BEFORE
                        if self.response_type == UpdateResponse.OLD_DOCUMENT
                        else ReturnDocument.AFTER
                    ),
                    **self.pymongo_kwargs,
                )
                if result is not None:
                    result = parse_obj(self.document_model, result)
                return result
        else:
            self.bulk_writer.add_operation(
                Operation(
                    operation=UpdateOnePyMongo,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )

    def __await__(
        self,
    ) -> Generator[
        Any, None, Union[UpdateResult, InsertOneResult, Optional["DocType"]]
    ]:
        """
        Run the query
        :return:
        """
        update_result = yield from self._update().__await__()
        if self.upsert_insert_doc is None:
            return update_result

        if (
            self.response_type == UpdateResponse.UPDATE_RESULT
            and update_result is not None
            and update_result.matched_count == 0
        ) or (
            self.response_type != UpdateResponse.UPDATE_RESULT
            and update_result is None
        ):
            return (
                yield from self.document_model.insert_one(
                    document=self.upsert_insert_doc,
                    session=self.session,
                    bulk_writer=self.bulk_writer,
                ).__await__()
            )

        return update_result
