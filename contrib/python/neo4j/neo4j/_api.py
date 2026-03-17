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

from enum import Enum
from urllib.parse import (
    parse_qs,
    urlparse,
)

from . import (
    _typing as t,
    api,
)
from .exceptions import ConfigurationError


__all__ = [
    "DRIVER_BOLT",
    "DRIVER_NEO4J",
    "SECURITY_TYPE_NOT_SECURE",
    "SECURITY_TYPE_SECURE",
    "SECURITY_TYPE_SELF_SIGNED_CERTIFICATE",
    "NotificationCategory",
    "NotificationClassification",
    "NotificationDisabledCategory",
    "NotificationDisabledClassification",
    "NotificationMinimumSeverity",
    "NotificationSeverity",
    "RoutingControl",
    "TelemetryAPI",
    "check_access_mode",
    "parse_neo4j_uri",
    "parse_routing_context",
]


DRIVER_BOLT: t.Final[str] = "DRIVER_BOLT"
DRIVER_NEO4J: t.Final[str] = "DRIVER_NEO4J"

SECURITY_TYPE_NOT_SECURE: t.Final[str] = "SECURITY_TYPE_NOT_SECURE"
SECURITY_TYPE_SELF_SIGNED_CERTIFICATE: t.Final[str] = (
    "SECURITY_TYPE_SELF_SIGNED_CERTIFICATE"
)
SECURITY_TYPE_SECURE: t.Final[str] = "SECURITY_TYPE_SECURE"


def parse_neo4j_uri(uri):
    parsed = urlparse(uri)

    if parsed.username:
        raise ConfigurationError("Username is not supported in the URI")

    if parsed.password:
        raise ConfigurationError("Password is not supported in the URI")

    if parsed.scheme == api.URI_SCHEME_BOLT_ROUTING:
        raise ConfigurationError(
            f"Uri scheme {parsed.scheme!r} has been renamed. "
            f"Use {api.URI_SCHEME_NEO4J!r}"
        )
    elif parsed.scheme == api.URI_SCHEME_BOLT:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_NOT_SECURE
    elif parsed.scheme == api.URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_SELF_SIGNED_CERTIFICATE
    elif parsed.scheme == api.URI_SCHEME_BOLT_SECURE:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_SECURE
    elif parsed.scheme == api.URI_SCHEME_NEO4J:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_NOT_SECURE
    elif parsed.scheme == api.URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_SELF_SIGNED_CERTIFICATE
    elif parsed.scheme == api.URI_SCHEME_NEO4J_SECURE:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_SECURE
    else:
        supported_schemes = [
            api.URI_SCHEME_BOLT,
            api.URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
            api.URI_SCHEME_BOLT_SECURE,
            api.URI_SCHEME_NEO4J,
            api.URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
            api.URI_SCHEME_NEO4J_SECURE,
        ]
        raise ConfigurationError(
            f"URI scheme {parsed.scheme!r} is not supported. "
            f"Supported URI schemes are {supported_schemes}. "
            "Examples: bolt://host[:port] or "
            "neo4j://host[:port][?routing_context]"
        )

    return driver_type, security_type, parsed


def check_access_mode(access_mode):
    if access_mode not in {api.READ_ACCESS, api.WRITE_ACCESS}:
        raise ValueError(
            f"Unsupported access mode {access_mode}, must be one of "
            f"'{api.READ_ACCESS}' or '{api.WRITE_ACCESS}'."
        )

    return access_mode


def parse_routing_context(query):
    """
    Parse the query portion of a URI.

    Generates a routing context dictionary.
    """
    if not query:
        return {}

    context = {}
    parameters = parse_qs(query, True)
    for key in parameters:
        value_list = parameters[key]
        if len(value_list) != 1:
            raise ConfigurationError(
                f"Duplicated query parameters with key '{key}', value "
                f"'{value_list}' found in query string '{query}'"
            )
        value = value_list[0]
        if not value:
            raise ConfigurationError(
                f"Invalid parameters:'{key}={value}' in query string "
                f"'{query}'."
            )
        context[key] = value

    return context


class NotificationMinimumSeverity(str, Enum):
    """
    Filter notifications returned by the server by minimum severity.

    For GQL-aware servers, notifications are a subset of GqlStatusObjects.
    See also :attr:`.GqlStatusObject.is_notification`.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.NotificationMinimumSeverity` value
    will also accept a string::

        >>> NotificationMinimumSeverity.OFF == "OFF"
        True
        >>> NotificationMinimumSeverity.WARNING == "WARNING"
        True
        >>> NotificationMinimumSeverity.INFORMATION == "INFORMATION"
        True

    .. seealso::
        driver config :ref:`driver-notifications-min-severity-ref`,
        session config :ref:`session-notifications-min-severity-ref`

    .. versionadded:: 5.7
    """

    OFF = "OFF"
    WARNING = "WARNING"
    INFORMATION = "INFORMATION"


if t.TYPE_CHECKING:
    T_NotificationMinimumSeverity = (
        NotificationMinimumSeverity
        | t.Literal[
            "OFF",
            "WARNING",
            "INFORMATION",
        ]
    )
    __all__.append("T_NotificationMinimumSeverity")


class NotificationSeverity(str, Enum):
    """
    Server-side notification severity.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> NotificationSeverity.WARNING == "WARNING"
        True
        >>> NotificationSeverity.INFORMATION == "INFORMATION"
        True
        >>> NotificationSeverity.UNKNOWN == "UNKNOWN"
        True

    Example::

        import logging

        from neo4j import NotificationSeverity


        log = logging.getLogger(__name__)

        ...

        summary = session.run("RETURN 1").consume()

        for notification in summary.summary_notifications:
            severity = notification.severity_level
            if severity == NotificationSeverity.WARNING:
                # or severity == "WARNING"
                log.warning("%r", notification)
            elif severity == NotificationSeverity.INFORMATION:
                # or severity == "INFORMATION"
                log.info("%r", notification)
            else:
                # assert severity == NotificationSeverity.UNKNOWN
                # or severity == "UNKNOWN"
                log.debug("%r", notification)

    .. seealso:: :attr:`.SummaryNotification.severity_level`

    .. versionadded:: 5.7
    """

    WARNING = "WARNING"
    INFORMATION = "INFORMATION"
    #: Used when the server provides a Severity which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver.
    UNKNOWN = "UNKNOWN"


class NotificationDisabledCategory(str, Enum):
    """
    Filter notifications returned by the server by category.

    For GQL-aware servers, notifications are a subset of GqlStatusObjects.
    See also :attr:`.GqlStatusObject.is_notification`.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.NotificationDisabledCategory` value
    will also accept a string::

        >>> NotificationDisabledCategory.UNRECOGNIZED == "UNRECOGNIZED"
        True
        >>> NotificationDisabledCategory.PERFORMANCE == "PERFORMANCE"
        True
        >>> NotificationDisabledCategory.DEPRECATION == "DEPRECATION"
        True

    .. seealso::
        driver config :ref:`driver-notifications-disabled-categories-ref`,
        session config :ref:`session-notifications-disabled-categories-ref`

    .. versionadded:: 5.7

    .. versionchanged:: 5.14
        Added categories :attr:`.SECURITY` and :attr:`.TOPOLOGY`.

    .. versionchanged:: 5.24
        Added category :attr:`.SCHEMA`.

    .. deprecated:: 6.0
        Use :class:`.NotificationDisabledClassification` instead.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    #: Requires server version 5.13 or newer.
    TOPOLOGY = "TOPOLOGY"
    #: Requires server version 5.17 or newer.
    SCHEMA = "SCHEMA"


class NotificationDisabledClassification(str, Enum):
    """
    Identical to :class:`.NotificationDisabledCategory`.

    This alternative is provided for a consistent naming with
    :attr:`.GqlStatusObject.classification`.

    .. seealso::
        driver config
        :ref:`driver-notifications-disabled-classifications-ref`,
        session config
        :ref:`session-notifications-disabled-classifications-ref`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    #: Requires server version 5.13 or newer.
    TOPOLOGY = "TOPOLOGY"
    #: Requires server version 5.17 or newer.
    SCHEMA = "SCHEMA"


if t.TYPE_CHECKING:
    T_NotificationDisabledClassification = (
        NotificationDisabledCategory
        | NotificationDisabledClassification
        | t.Literal[
            "HINT",
            "UNRECOGNIZED",
            "UNSUPPORTED",
            "PERFORMANCE",
            "DEPRECATION",
            "GENERIC",
            "SECURITY",
            "TOPOLOGY",
            "SCHEMA",
        ]
    )
    __all__.append("T_NotificationDisabledClassification")


class NotificationCategory(str, Enum):
    """
    Server-side notification category.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> NotificationCategory.DEPRECATION == "DEPRECATION"
        True
        >>> NotificationCategory.GENERIC == "GENERIC"
        True
        >>> NotificationCategory.UNKNOWN == "UNKNOWN"
        True

    .. seealso:: :attr:`.SummaryNotification.category`

    .. versionadded:: 5.7

    .. versionchanged:: 5.14
        Added categories :attr:`.SECURITY` and :attr:`.TOPOLOGY`.

    .. versionchanged:: 5.24
        Added category :attr:`.SCHEMA`.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    TOPOLOGY = "TOPOLOGY"
    SCHEMA = "SCHEMA"
    #: Used when the server provides a Category which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before notification categories were introduced.
    UNKNOWN = "UNKNOWN"


class NotificationClassification(str, Enum):
    """
    Identical to :class:`.NotificationCategory`.

    This alternative is provided for a consistent naming with
    :attr:`.GqlStatusObject.classification`.

    .. seealso:: :attr:`.GqlStatusObject.classification`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    TOPOLOGY = "TOPOLOGY"
    SCHEMA = "SCHEMA"
    #: Used when the server provides a Category which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before notification categories were introduced.
    UNKNOWN = "UNKNOWN"


class RoutingControl(str, Enum):
    """
    Selection which cluster members to route a query connect to.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.RoutingControl` value will also
    accept a string::

        >>> RoutingControl.READ == "r"
        True
        >>> RoutingControl.WRITE == "w"
        True

    .. seealso::
        :meth:`.AsyncDriver.execute_query`, :meth:`.Driver.execute_query`

    .. versionadded:: 5.5

    .. versionchanged:: 5.8

        * Renamed ``READERS`` to ``READ`` and ``WRITERS`` to ``WRITE``.
        * Stabilized from experimental.
    """

    READ = "r"
    WRITE = "w"


class TelemetryAPI(int, Enum):
    TX_FUNC = 0
    TX = 1
    AUTO_COMMIT = 2
    DRIVER = 3


if t.TYPE_CHECKING:
    T_RoutingControl = RoutingControl | t.Literal["r", "w"]
    __all__.append("T_RoutingControl")
