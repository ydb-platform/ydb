from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Generic, cast

from eventsourcing.dcb.domain import (
    EnduringObject,
    InitialDecision,
    Perspective,
    Selector,
    TDecision,
    TGroup,
    TPerspective,
    TSlice,
)
from eventsourcing.dcb.persistence import (
    DCBEventStore,
    DCBInfrastructureFactory,
    DCBMapper,
    NotFoundError,
)
from eventsourcing.utils import Environment, EnvType, resolve_topic

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from typing_extensions import Self


class DCBApplication:
    name = "DCBApplication"
    env: Mapping[str, str] = {"PERSISTENCE_MODULE": "eventsourcing.dcb.popo"}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(self, env: EnvType | None = None):
        self.env = self.construct_env(self.name, env)
        self.factory = DCBInfrastructureFactory.construct(self.env)
        self.recorder = self.factory.dcb_recorder()
        if "MAPPER_TOPIC" in self.env:
            # Only need a mapper, event store, and repository
            # if we are using the higher-level abstractions.
            self.mapper = cast(
                DCBMapper[Any], resolve_topic(self.env["MAPPER_TOPIC"])()
            )
            assert isinstance(self.mapper, DCBMapper)
            self.events = DCBEventStore(self.mapper, self.recorder)
            self.repository = DCBRepository(self.events)

    def construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which application will be configured."""
        _env = dict(type(self).env)
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def do(self, s: TSlice) -> TSlice:
        """
        Advances and executes a slice, then saves new decisions.
        """
        if type(s).do_projection:
            s = self.repository.advance(s)
        s.execute()
        if s.new_decisions:
            self.repository.save(s)
        return s

    def close(self) -> None:
        self.factory.close()

    def __enter__(self) -> Self:
        self.factory.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
        self.factory.__exit__(exc_type, exc_val, exc_tb)


class DCBRepository(Generic[TDecision]):
    def __init__(self, eventstore: DCBEventStore[TDecision]):
        self.eventstore = eventstore

    def save(self, p: Perspective[TDecision]) -> int:
        return self.eventstore.append(
            events=p.collect_events(),
            cb=p.consistency_boundary(),
            after=p.last_known_position,
        )

    def get(
        self,
        enduring_object_id: str,
    ) -> EnduringObject[TDecision]:
        cb = [Selector(tags=[enduring_object_id])]
        events = self.eventstore.read(*cb)
        obj: EnduringObject[TDecision] | None = None
        for event in events:
            obj = event.decision.mutate(obj)
        if obj is None:
            raise NotFoundError
        obj.last_known_position = events.head
        return obj

    def get_many(
        self,
        *enduring_object_ids: str,
    ) -> list[EnduringObject[TDecision] | None]:
        cb = [
            Selector(tags=[enduring_object_id])
            for enduring_object_id in enduring_object_ids
        ]
        tagged_decisions = self.eventstore.read(cb)
        objs: dict[str, EnduringObject[TDecision] | None] = dict.fromkeys(
            enduring_object_ids
        )
        for tagged in tagged_decisions:
            for tag in tagged.tags:
                obj = objs.get(tag)
                if not isinstance(tagged.decision, InitialDecision) and not obj:
                    continue
                obj = tagged.decision.mutate(obj)
                objs[tag] = obj
        for obj in objs.values():
            if obj is not None:
                obj.last_known_position = tagged_decisions.head
        return list(objs.values())

    def get_group(self, cls: type[TGroup], *enduring_object_ids: str) -> TGroup:
        enduring_objects = self.get_many(*enduring_object_ids)
        perspective = cls(*enduring_objects)
        last_known_positions = [
            o.last_known_position
            for o in enduring_objects
            if o and o.last_known_position
        ]
        perspective.last_known_position = (
            max(last_known_positions) if last_known_positions else None
        )
        return perspective

    def advance(self, p: TPerspective) -> TPerspective:
        events = self.eventstore.read(
            cb=p.consistency_boundary(),
            after=p.last_known_position,
        )
        for event in events:
            event.decision.mutate(p)
        p.last_known_position = events.head
        return p
