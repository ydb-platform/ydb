# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import itertools
import typing as t

# ignore TC003 to make sphinx not completely drop the ball
from collections.abc import Sequence  # noqa: TC003
from copy import deepcopy
from dataclasses import dataclass

from .._api import (
    NotificationCategory,
    NotificationClassification,
    NotificationSeverity,
)
from .._exceptions import BoltProtocolError


if t.TYPE_CHECKING:
    import typing_extensions as te
    from typing_extensions import deprecated

    from .._addressing import Address
    from ..api import ServerInfo

    _T = t.TypeVar("_T")
else:
    from .._warnings import deprecated


class ResultSummary:
    """A summary of execution returned with a :class:`.Result` object."""

    #: A :class:`neo4j.ServerInfo` instance. Provides some basic information of
    #: the server where the result is obtained from.
    server: ServerInfo

    #: The database name where this summary is obtained from.
    database: str | None

    #: The query that was executed to produce this result.
    query: str | None

    #: Dictionary of parameters passed with the statement.
    parameters: dict[str, t.Any] | None

    #: A string that describes the type of query
    #: ``'r'`` = read-only, ``'rw'`` = read/write, ``'w'`` = write-only,
    #: ``'s'`` = schema.
    query_type: t.Literal["r", "rw", "w", "s"] | None

    #: A :class:`neo4j.SummaryCounters` instance. Counters for operations the
    #: query triggered.
    counters: SummaryCounters

    #: Dictionary that describes how the database will execute the query.
    plan: dict | None

    #: Dictionary that describes how the database executed the query.
    profile: dict | None

    #: The time it took for the server to have the result available.
    #: (milliseconds)
    result_available_after: int | None

    #: The time it took for the server to consume the result. (milliseconds)
    result_consumed_after: int | None

    #: see :attr:`.notifications`
    _notifications: list[dict] | None

    # cache for property ``summary_notifications``
    _summary_notifications: tuple[SummaryNotification, ...]

    # cache for property ``summary_notifications``
    _gql_status_objects: tuple[GqlStatusObject, ...]

    _had_key: bool
    _had_record: bool

    def __init__(
        self,
        address: Address,
        had_key: bool,
        had_record: bool,
        metadata: dict[str, t.Any],
    ) -> None:
        self._had_key = had_key
        self._had_record = had_record
        self.metadata = metadata
        self.server = metadata["server"]
        self.database = metadata.get("db")
        self.query = metadata.get("query")
        self.parameters = metadata.get("parameters")
        if "type" in metadata:
            self.query_type = metadata["type"]
            if self.query_type not in {"r", "w", "rw", "s"}:
                raise BoltProtocolError(
                    f"Unexpected query type {self.query_type!r} received from "
                    "server. Consider updating the driver.",
                    address,
                )
        self.query_type = metadata.get("type")
        self.plan = metadata.get("plan")
        self.profile = metadata.get("profile")
        self.counters = SummaryCounters(metadata.get("stats", {}))
        if self.server.protocol_version[0] < 3:
            self.result_available_after = metadata.get(
                "result_available_after"
            )
            self.result_consumed_after = metadata.get("result_consumed_after")
        else:
            self.result_available_after = metadata.get("t_first")
            self.result_consumed_after = metadata.get("t_last")

    @staticmethod
    def _notification_from_status(status: dict) -> dict:
        notification = {}
        for notification_key, status_key in (
            ("title", "title"),
            ("code", "neo4j_code"),
            ("description", "description"),
        ):
            if status_key in status:
                notification[notification_key] = status[status_key]

        if "diagnostic_record" in status:
            diagnostic_record = status["diagnostic_record"]
            if not isinstance(diagnostic_record, dict):
                diagnostic_record = {}

            for notification_key, diag_record_key in (
                ("severity", "_severity"),
                ("category", "_classification"),
                ("position", "_position"),
            ):
                if diag_record_key in diagnostic_record:
                    notification[notification_key] = diagnostic_record[
                        diag_record_key
                    ]

        return notification

    @property
    @deprecated(
        "ResultSummary.notifications is deprecated, "
        "use ResultSummary.gql_status_objects instead."
    )
    def notifications(self) -> list[dict] | None:
        """
        A list of Dictionaries containing notification information.

        Notifications provide extra information for a user executing a
        statement.
        They can be warnings about problematic queries or other valuable
        information that can be presented in a client.
        Unlike failures or errors, notifications do not affect the execution of
        a statement.

        .. seealso:: :attr:`.summary_notifications`

        .. deprecated:: 6.0
            Use :attr:`.gql_status_objects` instead.
        """
        return self._get_notifications()

    @notifications.setter
    @deprecated(
        "ResultSummary.notifications is deprecated, "
        "use ResultSummary.gql_status_objects instead."
    )
    def notifications(self, value: list[dict] | None) -> None:
        self._notifications = value

    def _get_notifications(self) -> list[dict] | None:
        if not hasattr(self, "_notifications"):
            self._set_notifications()
        return self._notifications

    def _set_notifications(self) -> None:
        if "notifications" in self.metadata:
            notifications = self.metadata["notifications"]
            if not isinstance(notifications, list):
                self._notifications = None
                return
            self._notifications = notifications
            return

        # polyfill notifications from GqlStatusObjects
        if "statuses" in self.metadata:
            statuses = self.metadata["statuses"]
            if not isinstance(statuses, list):
                self._notifications = None
                return
            notifications = []
            for status in statuses:
                if not (isinstance(status, dict) and "neo4j_code" in status):
                    # not a notification status
                    continue
                notification = self._notification_from_status(status)
                notifications.append(notification)
            self._notifications = notifications or None
            return

        self._notifications = None

    @property
    @deprecated(
        "ResultSummary.summary_notifications is deprecated, "
        "use ResultSummary.gql_status_objects instead."
    )
    def summary_notifications(self) -> Sequence[SummaryNotification]:
        """
        The same as ``notifications`` but in a parsed, structured form.

        Further, if connected to a gql-aware server, this property will be
        polyfilled from :attr:`gql_status_objects`.

        .. seealso:: :attr:`.notifications`, :class:`.SummaryNotification`

        .. versionadded:: 5.7

        .. deprecated:: 6.0
            Use :attr:`.gql_status_objects` instead.
        """
        if getattr(self, "_summary_notifications", None) is not None:
            return self._summary_notifications

        raw_notifications = self._get_notifications()
        if not isinstance(raw_notifications, list):
            self._summary_notifications = ()
            return self._summary_notifications
        self._summary_notifications = tuple(
            SummaryNotification._from_metadata(n) for n in raw_notifications
        )
        return self._summary_notifications

    @property
    def gql_status_objects(self) -> t.Sequence[GqlStatusObject]:
        """
        Get GqlStatusObjects that arose when executing the query.

        The sequence always contains at least 1 status representing the
        Success, No Data or Omitted Result.
        All other status are notifications like warnings about problematic
        queries or other valuable information that can be presented in a
        client.

        The GqlStatusObjects will be presented in the following order:

        * A "no data" (``02xxx``) has precedence over a warning.
        * A "warning" (``01xxx``) has precedence over a success.
        * A "success" (``00xxx``) has precedence over anything informational
          (``03xxx``).

        .. versionadded:: 5.22

        .. versionchanged:: 6.0 Stabilized from preview.
        """
        raw_status_objects = self.metadata.get("statuses")
        if isinstance(raw_status_objects, list):
            self._gql_status_objects = tuple(
                GqlStatusObject._from_status_metadata(s)
                for s in raw_status_objects
            )
            return self._gql_status_objects

        raw_notifications = self._get_notifications()
        notification_status_objects: t.Iterable[GqlStatusObject]
        if isinstance(raw_notifications, list):
            notification_status_objects = [
                GqlStatusObject._from_notification_metadata(n)
                for n in raw_notifications
            ]
        else:
            notification_status_objects = ()

        if self._had_record:
            # polyfill with a Success status
            result_status = GqlStatusObject._success()
        elif self._had_key:
            # polyfill with an Omitted Result status
            result_status = GqlStatusObject._no_data()
        else:
            # polyfill with a No Data status
            result_status = GqlStatusObject._omitted_result()

        notification_status_objects = itertools.chain(
            notification_status_objects, (result_status,)
        )

        def status_precedence(status: GqlStatusObject) -> int:
            if status.gql_status.startswith("02"):
                # no data
                return 3
            if status.gql_status.startswith("01"):
                # warning
                return 2
            if status.gql_status.startswith("00"):
                # success
                return 1
            if status.gql_status.startswith("03"):
                # informational
                return 0
            return -1

        notification_status_objects = sorted(
            notification_status_objects,
            key=status_precedence,
            reverse=True,
        )
        self._gql_status_objects = tuple(notification_status_objects)

        return self._gql_status_objects


_COUNTER_KEY_TO_ATTR_NAME = {
    "nodes-created": "nodes_created",
    "nodes-deleted": "nodes_deleted",
    "relationships-created": "relationships_created",
    "relationships-deleted": "relationships_deleted",
    "properties-set": "properties_set",
    "labels-added": "labels_added",
    "labels-removed": "labels_removed",
    "indexes-added": "indexes_added",
    "indexes-removed": "indexes_removed",
    "constraints-added": "constraints_added",
    "constraints-removed": "constraints_removed",
    "system-updates": "system_updates",
    "contains-updates": "_contains_updates",
    "contains-system-updates": "_contains_system_updates",
}

_COUNTER_ATTR_NAME_TO_KEY = {
    v: k for k, v in _COUNTER_KEY_TO_ATTR_NAME.items()
}


class SummaryCounters:
    """Contains counters for various operations that a query triggered."""

    #:
    nodes_created: int = 0

    #:
    nodes_deleted: int = 0

    #:
    relationships_created: int = 0

    #:
    relationships_deleted: int = 0

    #:
    properties_set: int = 0

    #:
    labels_added: int = 0

    #:
    labels_removed: int = 0

    #:
    indexes_added: int = 0

    #:
    indexes_removed: int = 0

    #:
    constraints_added: int = 0

    #:
    constraints_removed: int = 0

    #:
    system_updates: int = 0

    _contains_updates = None
    _contains_system_updates = None

    def __init__(self, statistics) -> None:
        for key, value in dict(statistics).items():
            attr_name = _COUNTER_KEY_TO_ATTR_NAME.get(key)
            if attr_name:
                setattr(self, attr_name, value)

    def __repr__(self) -> str:
        statistics = {
            _COUNTER_ATTR_NAME_TO_KEY[k]: v
            for k, v in vars(self).items()
            if k in _COUNTER_ATTR_NAME_TO_KEY
        }
        return f"{self.__class__.__name__}({statistics!r})"

    def __str__(self) -> str:
        attrs = []
        for k, v in vars(self).items():
            if k.startswith("_"):  # hide private attributes
                continue
            if hasattr(self.__class__, k) and getattr(self.__class__, k) == v:
                # hide default values
                continue
            attrs.append(f"{k}: {v}")
        attrs.append(f"contains_updates: {self.contains_updates}")
        attrs.append(
            f"contains_system_updates: {self.contains_system_updates}"
        )
        return f"SummaryCounters{{{', '.join(attrs)}}}"

    @property
    def contains_updates(self) -> bool:
        """
        Check if any counters tracking graph updates are greater than 0.

        True if any of the counters except for system_updates, are greater
        than 0. Otherwise, False.
        """
        if self._contains_updates is not None:
            return self._contains_updates
        return bool(
            self.nodes_created
            or self.nodes_deleted
            or self.relationships_created
            or self.relationships_deleted
            or self.properties_set
            or self.labels_added
            or self.labels_removed
            or self.indexes_added
            or self.indexes_removed
            or self.constraints_added
            or self.constraints_removed
        )

    @property
    def contains_system_updates(self) -> bool:
        """True if the system database was updated, otherwise False."""
        if self._contains_system_updates is not None:
            return self._contains_system_updates
        return self.system_updates > 0


@dataclass
class SummaryInputPosition:
    """
    Structured form of a gql status/notification position.

    .. seealso::
        :attr:`.GqlStatusObject.position`,
        :attr:`.SummaryNotification.position`,
        :data:`.SummaryNotificationPosition`

    .. versionadded:: 5.22
    """

    #: The line number of the notification. Line numbers start at 1.
    line: int
    #: The column number of the notification. Column numbers start at 1.
    column: int
    #: The character offset of the notification. Offsets start at 0.
    offset: int

    @classmethod
    def _from_metadata(cls, metadata: object) -> te.Self | None:
        if not isinstance(metadata, dict):
            return None
        line = metadata.get("line")
        if not isinstance(line, int) or isinstance(line, bool):
            return None
        column = metadata.get("column")
        if not isinstance(column, int) or isinstance(column, bool):
            return None
        offset = metadata.get("offset")
        if not isinstance(offset, int) or isinstance(offset, bool):
            return None
        return cls(line=line, column=column, offset=offset)

    def __str__(self) -> str:
        return (
            f"line: {self.line}, column: {self.column}, offset: {self.offset}"
        )

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"line={self.line!r}, "
            f"column={self.column!r}, "
            f"offset={self.offset!r}"
            ">"
        )


# Deprecated alias for :class:`.SummaryInputPosition`.
#
# .. versionadded:: 5.7
#
# .. versionchanged:: 5.22
#     Deprecated in favor of :class:`.SummaryInputPosition`.
SummaryNotificationPosition: te.TypeAlias = SummaryInputPosition


_SEVERITY_LOOKUP: dict[t.Any, NotificationSeverity] = {
    "WARNING": NotificationSeverity.WARNING,
    "INFORMATION": NotificationSeverity.INFORMATION,
}

_CATEGORY_LOOKUP: dict[t.Any, NotificationCategory] = {
    "HINT": NotificationCategory.HINT,
    "UNRECOGNIZED": NotificationCategory.UNRECOGNIZED,
    "UNSUPPORTED": NotificationCategory.UNSUPPORTED,
    "PERFORMANCE": NotificationCategory.PERFORMANCE,
    "DEPRECATION": NotificationCategory.DEPRECATION,
    "GENERIC": NotificationCategory.GENERIC,
    "SECURITY": NotificationCategory.SECURITY,
    "TOPOLOGY": NotificationCategory.TOPOLOGY,
    "SCHEMA": NotificationCategory.SCHEMA,
}

_CLASSIFICATION_LOOKUP: dict[t.Any, NotificationClassification] = {
    k: NotificationClassification(v) for k, v in _CATEGORY_LOOKUP.items()
}


if t.TYPE_CHECKING:

    class _SummaryNotificationKwargs(te.TypedDict, total=False):
        title: str
        code: str
        description: str
        severity_level: NotificationSeverity
        category: NotificationCategory
        raw_severity_level: str
        raw_category: str
        position: SummaryInputPosition | None


@dataclass
class SummaryNotification:
    """
    Structured form of a notification received from the server.

    .. seealso:: :attr:`.ResultSummary.summary_notifications`

    .. versionadded:: 5.7
    """

    title: str = ""
    code: str = ""
    description: str = ""
    severity_level: NotificationSeverity = NotificationSeverity.UNKNOWN
    category: NotificationCategory = NotificationCategory.UNKNOWN
    raw_severity_level: str = ""
    raw_category: str = ""
    position: SummaryNotificationPosition | None = None

    @classmethod
    def _from_metadata(cls, metadata: object) -> te.Self:
        if not isinstance(metadata, dict):
            return cls()
        kwargs: _SummaryNotificationKwargs = {
            "position": SummaryInputPosition._from_metadata(
                metadata.get("position")
            ),
        }
        str_keys: tuple[t.Literal["title", "code", "description"], ...] = (
            "title",
            "code",
            "description",
        )
        for key in str_keys:
            value = metadata.get(key)
            if isinstance(value, str):
                kwargs[key] = value
        severity = metadata.get("severity")
        if isinstance(severity, str):
            kwargs["raw_severity_level"] = severity
            kwargs["severity_level"] = _SEVERITY_LOOKUP.get(
                severity, NotificationSeverity.UNKNOWN
            )
        category = metadata.get("category")
        if isinstance(category, str):
            kwargs["raw_category"] = category
            kwargs["category"] = _CATEGORY_LOOKUP.get(
                category, NotificationCategory.UNKNOWN
            )
        return cls(**kwargs)

    def __str__(self) -> str:
        return (
            f"{{severity: {self.raw_severity_level}}} {{code: {self.code}}} "
            f"{{category: {self.raw_category}}} {{title: {self.title}}} "
            f"{{description: {self.description}}} "
            f"{{position: {self.position}}}"
        )


POLYFILL_DIAGNOSTIC_RECORD = (
    ("OPERATION", ""),
    ("OPERATION_CODE", "0"),
    ("CURRENT_SCHEMA", "/"),
)


_SUCCESS_STATUS_METADATA = {
    "gql_status": "00000",
    "status_description": "note: successful completion",
    "diagnostic_record": dict(POLYFILL_DIAGNOSTIC_RECORD),
}
_OMITTED_RESULT_STATUS_METADATA = {
    "gql_status": "00001",
    "status_description": "note: successful completion - omitted result",
    "diagnostic_record": dict(POLYFILL_DIAGNOSTIC_RECORD),
}
_NO_DATA_STATUS_METADATA = {
    "gql_status": "02000",
    "status_description": "note: no data",
    "diagnostic_record": dict(POLYFILL_DIAGNOSTIC_RECORD),
}


class GqlStatusObject:
    """
    Representation for GqlStatusObject found when executing a query.

    GqlStatusObjects are a superset of notifications, i.e., some but not all
    GqlStatusObjects are notifications.
    Notifications can be filtered server-side with
    driver config
    :ref:`driver-notifications-disabled-classifications-ref` and
    :ref:`driver-notifications-min-severity-ref` as well as
    session config
    :ref:`session-notifications-disabled-classifications-ref` and
    :ref:`session-notifications-min-severity-ref`.

    .. seealso:: :attr:`.ResultSummary.gql_status_objects`

    .. versionadded:: 5.22
    """

    # internal dictionaries, never handed to assure immutability
    _status_metadata: dict[str, t.Any]
    _status_diagnostic_record: dict[str, t.Any] | None = None

    _is_notification: bool
    _gql_status: str
    _status_description: str
    _position: SummaryInputPosition | None
    _raw_classification: str | None
    _classification: NotificationClassification
    _raw_severity: str | None
    _severity: NotificationSeverity
    _diagnostic_record: dict[str, t.Any]

    @classmethod
    def _success(cls) -> te.Self:
        obj = cls()
        obj._status_metadata = _SUCCESS_STATUS_METADATA
        return obj

    @classmethod
    def _omitted_result(cls) -> te.Self:
        obj = cls()
        obj._status_metadata = _OMITTED_RESULT_STATUS_METADATA
        return obj

    @classmethod
    def _no_data(cls) -> te.Self:
        obj = cls()
        obj._status_metadata = _NO_DATA_STATUS_METADATA
        return obj

    @classmethod
    def _from_status_metadata(cls, metadata: object) -> te.Self:
        obj = cls()
        if isinstance(metadata, dict):
            obj._status_metadata = metadata
        else:
            obj._status_metadata = {}
        return obj

    @classmethod
    def _from_notification_metadata(cls, metadata: object) -> te.Self:
        obj = cls()
        if not isinstance(metadata, dict):
            metadata = {}
        description = metadata.get("description")
        neo4j_code = metadata.get("neo4j_code")
        if not isinstance(neo4j_code, str):
            neo4j_code = ""
        title = metadata.get("title")
        if not isinstance(title, str):
            title = ""
        position = SummaryInputPosition._from_metadata(
            metadata.get("position")
        )
        classification = metadata.get("category")
        if not isinstance(classification, str):
            classification = None
        severity = metadata.get("severity")
        if not isinstance(severity, str):
            severity = None

        if severity == "WARNING":
            gql_status = "01N42"
            if not isinstance(description, str) or not description:
                description = "warn: unknown warning"
        else:
            # for "INFORMATION" or if severity is missing
            gql_status = "03N42"
            if not isinstance(description, str) or not description:
                description = "info: unknown notification"

        diagnostic_record = dict(POLYFILL_DIAGNOSTIC_RECORD)
        if "category" in metadata:
            diagnostic_record["_classification"] = metadata["category"]
        if "severity" in metadata:
            diagnostic_record["_severity"] = metadata["severity"]
        if "position" in metadata:
            diagnostic_record["_position"] = metadata["position"]

        obj._status_metadata = {
            "gql_status": gql_status,
            "status_description": description,
            "neo4j_code": neo4j_code,
            "title": title,
            "diagnostic_record": diagnostic_record,
        }
        obj._gql_status = gql_status
        obj._status_description = description
        obj._position = position
        obj._raw_classification = classification
        obj._raw_severity = severity
        obj._is_notification = True
        return obj

    def __str__(self) -> str:
        return self.status_description

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"gql_status={self.gql_status!r}, "
            f"status_description={self.status_description!r}, "
            f"position={self.position!r}, "
            f"raw_classification={self.raw_classification!r}, "
            f"classification={self.classification!r}, "
            f"raw_severity={self.raw_severity!r}, "
            f"severity={self.severity!r}, "
            f"diagnostic_record={self.diagnostic_record!r}"
            ">"
        )

    @property
    def is_notification(self) -> bool:
        """
        Whether this GqlStatusObject is a notification.

        Only some GqlStatusObjects are notifications.
        The definition of notification is vendor-specific.
        Notifications are those GqlStatusObjects that provide additional
        information and can be filtered out via
        :ref:`driver-notifications-disabled-classifications-ref` and
        :ref:`driver-notifications-min-severity-ref` as well as.

        The fields :attr:`.position`,
        :attr:`.raw_classification`, :attr:`.classification`,
        :attr:`.raw_severity`, and :attr:`.severity` are only meaningful
        for notifications.
        """
        if hasattr(self, "_is_notification"):
            return self._is_notification

        neo4j_code = self._status_metadata.get("neo4j_code")
        self._is_notification = bool(
            isinstance(neo4j_code, str) and neo4j_code
        )
        return self._is_notification

    @classmethod
    def _extract_str_field(
        cls,
        data: dict[str, t.Any],
        key: str,
        default: _T = "",  # type: ignore[assignment]
    ) -> str | _T:
        value = data.get(key)
        if isinstance(value, str):
            return value
        else:
            return default

    @property
    def gql_status(self) -> str:
        """
        The GQLSTATUS.

        The following GQLSTATUS codes denote codes that the driver will use
        for polyfilling (when connected to an old, non-GQL-aware server).
        Further, they may be used by servers during the transition-phase to
        GQLSTATUS-awareness.

         * ``01N42`` (warning - unknown warning)
         * ``02N42`` (no data - unknown subcondition)
         * ``03N42`` (informational - unknown notification)
         * ``05N42`` (general processing exception - unknown error)

        .. note::
            This means these codes are not guaranteed to be stable and may
            change in future versions of the driver or the server.
        """
        if hasattr(self, "_gql_status"):
            return self._gql_status

        self._gql_status = self._extract_str_field(
            self._status_metadata, "gql_status"
        )
        return self._gql_status

    @property
    def status_description(self) -> str:
        """A description of the status."""
        if hasattr(self, "_status_description"):
            return self._status_description

        self._status_description = self._extract_str_field(
            self._status_metadata, "status_description"
        )
        return self._status_description

    def _get_status_diagnostic_record(self) -> dict[str, t.Any]:
        if self._status_diagnostic_record is not None:
            return self._status_diagnostic_record

        self._status_diagnostic_record = self._status_metadata.get(
            "diagnostic_record", {}
        )
        if not isinstance(self._status_diagnostic_record, dict):
            self._status_diagnostic_record = {}
        return self._status_diagnostic_record

    @property
    def position(self) -> SummaryInputPosition | None:
        """
        The position of the input that caused the status (if applicable).

        This is vendor-specific information.

        Only notifications (see :attr:`.is_notification`) have a meaningful
        position.

        The value is :data:`None` if the server's data was missing or could not
        be interpreted.
        """
        if hasattr(self, "_position"):
            return self._position

        diag_record = self._get_status_diagnostic_record()
        self._position = SummaryInputPosition._from_metadata(
            diag_record.get("_position")
        )
        return self._position

    @property
    def raw_classification(self) -> str | None:
        """
        The raw (``str``) classification of the status.

        This is a vendor-specific classification that can be used to filter
        notifications.

        Only notifications (see :attr:`.is_notification`) have a meaningful
        classification.
        """
        if hasattr(self, "_raw_classification"):
            return self._raw_classification

        diag_record = self._get_status_diagnostic_record()
        self._raw_classification = self._extract_str_field(
            diag_record, "_classification", None
        )
        return self._raw_classification

    @property
    def classification(self) -> NotificationClassification:
        """
        Parsed version of :attr:`.raw_classification`.

        Only notifications (see :attr:`.is_notification`) have a meaningful
        classification.
        """
        if hasattr(self, "_classification"):
            return self._classification

        self._classification = _CLASSIFICATION_LOOKUP.get(
            self.raw_classification, NotificationClassification.UNKNOWN
        )
        return self._classification

    @property
    def raw_severity(self) -> str | None:
        """
        The raw (``str``) severity of the status.

        This is a vendor-specific severity that can be used to filter
        notifications.

        Only notifications (see :attr:`.is_notification`) have a meaningful
        severity.
        """
        if hasattr(self, "_raw_severity"):
            return self._raw_severity

        diag_record = self._get_status_diagnostic_record()
        self._raw_severity = self._extract_str_field(
            diag_record, "_severity", None
        )
        return self._raw_severity

    @property
    def severity(self) -> NotificationSeverity:
        """
        Parsed version of :attr:`.raw_severity`.

        Only notifications (see :attr:`.is_notification`) have a meaningful
        severity.
        """
        if hasattr(self, "_severity"):
            return self._severity

        self._severity = _SEVERITY_LOOKUP.get(
            self.raw_severity, NotificationSeverity.UNKNOWN
        )
        return self._severity

    @property
    def diagnostic_record(self) -> dict[str, t.Any]:
        """Further information about the GQLSTATUS for diagnostic purposes."""
        if hasattr(self, "_diagnostic_record"):
            return self._diagnostic_record

        self._diagnostic_record = deepcopy(
            self._get_status_diagnostic_record()
        )
        return self._diagnostic_record
