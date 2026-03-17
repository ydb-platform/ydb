from __future__ import annotations

import json
from abc import ABC, abstractmethod
from base64 import b64decode, b64encode
from typing import TYPE_CHECKING, Generic
from uuid import UUID

from eventsourcing.application import NotificationLog, Section, TApplication
from eventsourcing.persistence import Notification

if TYPE_CHECKING:
    from collections.abc import Sequence


class NotificationLogInterface(ABC):
    """Abstract base class for obtaining serialised
    sections of a notification log.
    """

    @abstractmethod
    def get_log_section(self, section_id: str) -> str:
        """Returns a serialised :class:`~eventsourcing.application.Section`
        from a notification log.
        """

    @abstractmethod
    def get_notifications(
        self,
        start: int | None,
        limit: int,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> str:
        """Returns a serialised list of :class:`~eventsourcing.persistence.Notification`
        objects from a notification log.
        """


class NotificationLogJSONService(NotificationLogInterface, Generic[TApplication]):
    """Presents serialised sections of a notification log."""

    def __init__(self, app: TApplication):
        """Initialises service with given application."""
        self.app = app

    def get_log_section(self, section_id: str) -> str:
        """Returns JSON serialised :class:`~eventsourcing.application.Section`
        from a notification log.
        """
        section = self.app.notification_log[section_id]
        return json.dumps(
            {
                "id": section.id,
                "next_id": section.next_id,
                "items": [
                    {
                        "id": item.id,
                        "originator_id": (
                            item.originator_id.hex
                            if isinstance(item.originator_id, UUID)
                            else item.originator_id
                        ),
                        "originator_version": item.originator_version,
                        "topic": item.topic,
                        "state": b64encode(item.state).decode("utf8"),
                    }
                    for item in section.items
                ],
            }
        )

    def get_notifications(
        self,
        start: int | None,
        limit: int,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> str:
        notifications = self.app.notification_log.select(
            start=start,
            limit=limit,
            topics=topics,
            inclusive_of_start=inclusive_of_start,
        )
        return json.dumps(
            [
                {
                    "id": notification.id,
                    "originator_id": (
                        notification.originator_id.hex
                        if isinstance(notification.originator_id, UUID)
                        else notification.originator_id
                    ),
                    "originator_version": notification.originator_version,
                    "topic": notification.topic,
                    "state": b64encode(notification.state).decode("utf8"),
                }
                for notification in notifications
            ]
        )


class NotificationLogJSONClient(NotificationLog):
    """Presents deserialized sections of a notification log."""

    def __init__(self, interface: NotificationLogInterface):
        """Initialises log with a given interface."""
        self.interface = interface

    def __getitem__(self, section_id: str) -> Section:
        """Returns a :class:`Section` of
        :class:`~eventsourcing.persistence.Notification` objects
        from the notification log.
        """
        body = self.interface.get_log_section(section_id)
        section = json.loads(body)
        return Section(
            id=section["id"],
            next_id=section["next_id"],
            items=[
                Notification(
                    id=item["id"],
                    originator_id=UUID(item["originator_id"]),
                    originator_version=item["originator_version"],
                    topic=item["topic"],
                    state=b64decode(item["state"].encode("utf8")),
                )
                for item in section["items"]
            ],
        )

    def select(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> list[Notification]:
        """Returns a selection of
        :class:`~eventsourcing.persistence.Notification` objects
        from the notification log.
        """
        return [
            Notification(
                id=item["id"],
                originator_id=UUID(item["originator_id"]),
                originator_version=item["originator_version"],
                topic=item["topic"],
                state=b64decode(item["state"].encode("utf8")),
            )
            for item in json.loads(
                self.interface.get_notifications(
                    start=start,
                    limit=limit,
                    topics=topics,
                    inclusive_of_start=inclusive_of_start,
                )
            )
        ]
