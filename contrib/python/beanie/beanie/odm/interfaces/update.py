from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Union

from motor.motor_asyncio import AsyncIOMotorClientSession

from beanie.odm.bulk import BulkWriter
from beanie.odm.fields import ExpressionField
from beanie.odm.operators.update.general import (
    CurrentDate,
    Inc,
    Set,
)


class UpdateMethods:
    """
    Update methods
    """

    @abstractmethod
    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs: Any,
    ):
        return self

    def set(
        self,
        expression: Dict[Union[ExpressionField, str, Any], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs: Any,
    ):
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
        :return: self
        """
        return self.update(
            Set(expression), session=session, bulk_writer=bulk_writer, **kwargs
        )

    def current_date(
        self,
        expression: Dict[Union[datetime, ExpressionField, str], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs: Any,
    ):
        """
        Set current date

        Uses [CurrentDate operator](operators/update.md#currentdate)

        :param expression: Dict[Union[datetime, ExpressionField, str], Any]
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            **kwargs,
        )

    def inc(
        self,
        expression: Dict[Union[ExpressionField, float, int, str], Any],
        session: Optional[AsyncIOMotorClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs: Any,
    ):
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
        :return: self
        """
        return self.update(
            Inc(expression), session=session, bulk_writer=bulk_writer, **kwargs
        )
