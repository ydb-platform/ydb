from __future__ import annotations

from typing import Any, TypeVar

import msgspec

from eventsourcing.dcb import api, domain, persistence
from eventsourcing.utils import get_topic, resolve_topic


class Decision(msgspec.Struct, domain.Decision):
    def as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


TDecision = TypeVar("TDecision", bound=Decision)


class MessagePackMapper(persistence.DCBMapper[Decision]):
    def to_dcb_event(self, event: domain.Tagged[TDecision]) -> api.DCBEvent:
        return api.DCBEvent(
            type=get_topic(type(event.decision)),
            data=msgspec.msgpack.encode(event.decision),
            tags=event.tags,
        )

    def to_domain_event(self, event: api.DCBEvent) -> domain.Tagged[Decision]:
        return domain.Tagged(
            tags=event.tags,
            decision=msgspec.msgpack.decode(
                event.data,
                type=resolve_topic(event.type),
            ),
        )


class InitialDecision(Decision, domain.InitialDecision):
    originator_topic: str
