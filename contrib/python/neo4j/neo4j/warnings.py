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

from . import _typing as _t
from ._debug import NotificationPrinter


if _t.TYPE_CHECKING:
    from ._work.summary import GqlStatusObject


__all__ = [
    "Neo4jDeprecationWarning",
    "Neo4jWarning",
    "PreviewWarning",
]


class PreviewWarning(Warning):
    """
    A driver feature in preview has been used.

    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. versionadded:: 5.8

    .. versionchanged:: 6.0
        Moved from ``neo4j.PreviewWarning`` to
        ``neo4j.warnings.PreviewWarning``.
    """


class Neo4jWarning(Warning):
    """
    Warning emitted for notifications sent by the server.

    Which notifications trigger a warning can be controlled by a
    configuration option: :ref:`driver-warn-notification-severity-ref`

    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    :param notification: The notification that triggered the warning.
    :param query: The query for which the notification was sent.
        If provided, it will be used for a more detailed warning message.

    .. versionadded:: 5.21

    .. versionchanged:: 6.0
        * :attr:`.notification` is now of type :class:`.GqlStatusObject`
          instead of :class:`.SummaryNotification`.
        * Stabilized from preview.

    .. seealso:: :ref:`development-environment-ref`
    """

    #: The notification that triggered the warning.
    notification: GqlStatusObject

    def __init__(
        self,
        notification: GqlStatusObject,
        query: str | None = None,
    ) -> None:
        msg = str(NotificationPrinter(notification, query))
        super().__init__(msg)
        self.notification = notification


class Neo4jDeprecationWarning(Neo4jWarning, DeprecationWarning):
    """
    Warning emitted for deprecation notifications sent by the server.

    .. note::

        This warning is a subclass of :class:`DeprecationWarning`.
        This means that Python will not show this warning by default.

    :param notification: The notification that triggered the warning.

    .. versionadded:: 5.21

    .. versionchanged:: 6.0 Stabilized from preview.
    """
