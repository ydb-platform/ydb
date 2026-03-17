from typing import TYPE_CHECKING, Any, Dict, Generator, Mapping, Optional, Type

from motor.motor_asyncio import AsyncIOMotorClientSession
from pymongo import DeleteMany as DeleteManyPyMongo
from pymongo import DeleteOne as DeleteOnePyMongo
from pymongo.results import DeleteResult

from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.interfaces.clone import CloneInterface
from beanie.odm.interfaces.session import SessionMethods

if TYPE_CHECKING:
    from beanie.odm.documents import DocType


class DeleteQuery(SessionMethods, CloneInterface):
    """
    Deletion Query
    """

    def __init__(
        self,
        document_model: Type["DocType"],
        find_query: Mapping[str, Any],
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        self.document_model = document_model
        self.find_query = find_query
        self.session: Optional[AsyncIOMotorClientSession] = None
        self.bulk_writer = bulk_writer
        self.pymongo_kwargs: Dict[str, Any] = pymongo_kwargs


class DeleteMany(DeleteQuery):
    def __await__(
        self,
    ) -> Generator[DeleteResult, None, Optional[DeleteResult]]:
        """
        Run the query
        :return:
        """
        if self.bulk_writer is None:
            return (
                yield from self.document_model.get_motor_collection()
                .delete_many(
                    self.find_query,
                    session=self.session,
                    **self.pymongo_kwargs,
                )
                .__await__()
            )
        else:
            self.bulk_writer.add_operation(
                Operation(
                    operation=DeleteManyPyMongo,
                    first_query=self.find_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None


class DeleteOne(DeleteQuery):
    def __await__(
        self,
    ) -> Generator[DeleteResult, None, Optional[DeleteResult]]:
        """
        Run the query
        :return:
        """
        if self.bulk_writer is None:
            return (
                yield from self.document_model.get_motor_collection()
                .delete_one(
                    self.find_query,
                    session=self.session,
                    **self.pymongo_kwargs,
                )
                .__await__()
            )
        else:
            self.bulk_writer.add_operation(
                Operation(
                    operation=DeleteOnePyMongo,
                    first_query=self.find_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None
