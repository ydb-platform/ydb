import typing
import datetime
from pydantic import BaseModel


class Reservation(BaseModel):
    phone: typing.Optional[typing.Union[str, str]] = None
    description: typing.Optional[typing.Union[bytes, str]] = None
    table_id: int
    dt: datetime.datetime


class Table(BaseModel):
    table_id: int = None
    description: typing.Optional[typing.Union[bytes, str]] = None
    cnt: int


class ReservationCreateRequest(BaseModel):
    dt: datetime.datetime
    cnt: int
    description: typing.Optional[typing.Union[bytes, str]] = None
    phone: typing.Optional[typing.Union[bytes, str]]


class ReservationCreateResponse(BaseModel):
    success: bool
    table_id: typing.Optional[int] = None


class ReservationCancelRequest(BaseModel):
    phone: typing.Optional[typing.Union[bytes, str]]
    dt: datetime.datetime


class ReservationCancelResponse(BaseModel):
    success: bool
