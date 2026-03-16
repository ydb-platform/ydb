# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ezdxf.document import Drawing


class Messenger:
    def __init__(self, doc: Drawing) -> None:
        self.doc = doc
        self.entitydb = doc.entitydb

    def broadcast(self, message_type: int, data: Any = None) -> None:
        """Broadcast a message to all entities in the entity database."""
        # Receiver can change the entity database while processing the message.
        for entity in list(self.entitydb.values()):
            entity.notify(message_type, data)
