# -*- coding: utf-8 -*-
# type: ignore
from uuid import UUID

from sqlalchemy import BigInteger, Column, Index, Integer, LargeBinary, String, Text

try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Mapped
from sqlalchemy_utils.types.uuid import UUIDType


class Base:
    __allow_unmapped__ = True


Base = declarative_base(cls=Base)


class EventRecord(Base):
    __abstract__ = True
    originator_id: Column[UUID]
    originator_version: Column[int]
    topic: Column[str]
    state: Column[bytes]


class StoredEventRecord(EventRecord):
    __tablename__ = "stored_events"
    __abstract__ = True

    # Notification ID.
    id: Mapped[int] = Column(
        BigInteger().with_variant(Integer(), "sqlite"),
        primary_key=True,
        autoincrement=True,
    )

    # Originator ID (e.g. an entity or aggregate ID).
    originator_id: Mapped[UUID] = Column(UUIDType(), nullable=False)

    # Originator version of item in sequence.
    originator_version: Mapped[int] = Column(
        BigInteger().with_variant(Integer(), "sqlite"),
        nullable=False,
    )

    # Topic of the item (e.g. path to domain event class).
    topic: Mapped[str] = Column(Text(), nullable=False)

    # State of the item (serialized dict, possibly encrypted).
    state: Mapped[bytes] = Column(LargeBinary(), nullable=False)

    __table_args__ = (
        Index(
            "stored_aggregate_event_index",
            "originator_id",
            "originator_version",
            unique=True,
        ),
    )


class SnapshotRecord(EventRecord):
    __tablename__ = "snapshots"
    __abstract__ = True

    # Originator ID (e.g. an entity or aggregate ID).
    originator_id = Column(UUIDType(), primary_key=True)

    # Originator version of item in sequence.
    originator_version = Column(
        BigInteger().with_variant(Integer(), "sqlite"), primary_key=True
    )

    # Topic of the item (e.g. path to domain entity class).
    topic = Column(Text(), nullable=False)

    # State of the item (serialized dict, possibly encrypted).
    state = Column(LargeBinary(), nullable=False)


class NotificationTrackingRecord(Base):
    __tablename__ = "notification_tracking"
    __abstract__ = True

    # Application name.
    application_name: Mapped[str] = Column(String(length=32), primary_key=True)

    # Notification ID.
    notification_id: Mapped[int] = Column(
        BigInteger().with_variant(Integer(), "sqlite")
    )
