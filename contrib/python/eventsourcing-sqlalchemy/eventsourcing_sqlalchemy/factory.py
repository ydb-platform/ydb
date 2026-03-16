# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional, cast

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.utils import Environment, resolve_topic, strtobool
from sqlalchemy.orm import scoped_session

from eventsourcing_sqlalchemy.datastore import SQLAlchemyDatastore
from eventsourcing_sqlalchemy.recorders import (
    SQLAlchemyAggregateRecorder,
    SQLAlchemyApplicationRecorder,
    SQLAlchemyProcessRecorder,
    SQLAlchemyTrackingRecorder,
    TSQLAlchemyTrackingRecorder,
)


class SQLAlchemyFactory(InfrastructureFactory[SQLAlchemyTrackingRecorder]):
    SQLALCHEMY_URL = "SQLALCHEMY_URL"
    SQLALCHEMY_AUTOFLUSH = "SQLALCHEMY_AUTOFLUSH"
    SQLALCHEMY_CONNECTION_CREATOR_TOPIC = "SQLALCHEMY_CONNECTION_CREATOR_TOPIC"
    SQLALCHEMY_SCOPED_SESSION_TOPIC = "SQLALCHEMY_SCOPED_SESSION_TOPIC"
    SQLALCHEMY_SCHEMA = "SQLALCHEMY_SCHEMA"
    CREATE_TABLE = "CREATE_TABLE"

    datastore_class = SQLAlchemyDatastore
    aggregate_recorder_class = SQLAlchemyAggregateRecorder
    application_recorder_class = SQLAlchemyApplicationRecorder
    tracking_recorder_class = SQLAlchemyTrackingRecorder
    process_recorder_class = SQLAlchemyProcessRecorder

    def __init__(self, env: Environment):
        super().__init__(env)

        self._schema_name = self.env.get(self.SQLALCHEMY_SCHEMA)
        get_scoped_session_topic = self.env.get(self.SQLALCHEMY_SCOPED_SESSION_TOPIC)
        session: Optional[scoped_session] = None
        if get_scoped_session_topic:
            get_scoped_session = resolve_topic(get_scoped_session_topic)
            session = get_scoped_session()

        db_url = self.env.get(self.SQLALCHEMY_URL)
        autoflush = strtobool(self.env.get(self.SQLALCHEMY_AUTOFLUSH) or "True")

        # if db_url is None:
        #     raise EnvironmentError(
        #         "SQLAlchemy URL not found "
        #         "in environment with keys: "
        #         f"{', '.join(self.env.create_keys(self.SQLALCHEMY_URL))!r}"
        #     )

        kwargs = {}

        creator_topic = self.env.get(self.SQLALCHEMY_CONNECTION_CREATOR_TOPIC)
        if isinstance(creator_topic, str):
            kwargs["creator"] = resolve_topic(creator_topic)

        self.datastore = self.datastore_class(
            session=session, url=db_url, autoflush=autoflush, **kwargs
        )

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_" + purpose
        for_snapshots = purpose == "snapshots"
        recorder = self.aggregate_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            schema_name=self._schema_name,
            for_snapshots=for_snapshots,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        recorder = self.application_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            schema_name=self._schema_name,
        )
        if self.env_create_table() and recorder.datastore.engine is not None:
            recorder.create_table()
        return recorder

    def tracking_recorder(
        self, tracking_recorder_class: type[TSQLAlchemyTrackingRecorder] | None = None
    ) -> TSQLAlchemyTrackingRecorder:
        prefix = self.env.name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        if tracking_recorder_class is None:
            tracking_recorder_topic = self.env.get(self.TRACKING_RECORDER_TOPIC)
            if tracking_recorder_topic:
                tracking_recorder_class = resolve_topic(tracking_recorder_topic)
            else:
                tracking_recorder_class = cast(
                    "type[TSQLAlchemyTrackingRecorder]",
                    type(self).tracking_recorder_class,
                )
        assert tracking_recorder_class is not None
        assert issubclass(tracking_recorder_class, SQLAlchemyTrackingRecorder)
        recorder = tracking_recorder_class(
            datastore=self.datastore,
            tracking_table_name=tracking_table_name,
            schema_name=self._schema_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        prefix = self.env.name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        recorder = self.process_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            tracking_table_name=tracking_table_name,
            schema_name=self._schema_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def env_create_table(self) -> bool:
        default = "yes"
        return bool(strtobool(self.env.get(self.CREATE_TABLE) or default))


Factory = SQLAlchemyFactory
