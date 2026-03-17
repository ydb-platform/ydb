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


from . import _typing as _t
from ._api import (
    NotificationCategory as _NotificationCategory,
    NotificationDisabledCategory as _NotificationDisabledCategory,
    NotificationDisabledClassification,
    NotificationMinimumSeverity,
    NotificationSeverity,
    RoutingControl,
)
from ._async.driver import (
    AsyncBoltDriver,
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
)
from ._async.work import (
    AsyncManagedTransaction,
    AsyncResult,
    AsyncSession,
    AsyncTransaction,
)
from ._conf import (
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from ._data import Record
from ._meta import (
    get_user_agent,
    version as __version__,
)
from ._sync.driver import (
    BoltDriver,
    Driver,
    GraphDatabase,
    Neo4jDriver,
)
from ._sync.work import (
    ManagedTransaction,
    Result,
    Session,
    Transaction,
)
from ._warnings import (
    deprecation_warn as _deprecation_warn,
    PreviewWarning as _PreviewWarning,
)
from ._work import (
    EagerResult,
    GqlStatusObject,
    NotificationClassification,
    Query,
    ResultSummary,
    SummaryCounters,
    SummaryInputPosition,
    SummaryNotification as _SummaryNotification,
    unit_of_work,
)


if _t.TYPE_CHECKING:
    from ._api import (
        NotificationCategory,
        NotificationDisabledCategory,
    )
    from ._work import (
        SummaryNotification,
    )
    from ._warnings import PreviewWarning

from ._addressing import (
    Address,
    IPv4Address,
    IPv6Address,
)
from .api import (
    Auth,  # TODO: Validate naming for Auth compared to other drivers.
)
from .api import (
    AuthToken,
    basic_auth,
    bearer_auth,
    Bookmarks,
    custom_auth,
    DEFAULT_DATABASE,
    kerberos_auth,
    READ_ACCESS,
    ServerInfo,
    SYSTEM_DATABASE,
    WRITE_ACCESS,
)


__all__ = [
    "DEFAULT_DATABASE",
    "READ_ACCESS",
    "SYSTEM_DATABASE",
    "WRITE_ACCESS",
    "Address",
    "AsyncBoltDriver",
    "AsyncDriver",
    "AsyncGraphDatabase",
    "AsyncManagedTransaction",
    "AsyncNeo4jDriver",
    "AsyncResult",
    "AsyncSession",
    "AsyncTransaction",
    "Auth",
    "AuthToken",
    "BoltDriver",
    "Bookmarks",
    "Driver",
    "EagerResult",
    "GqlStatusObject",
    "GraphDatabase",
    "IPv4Address",
    "IPv6Address",
    "ManagedTransaction",
    "Neo4jDriver",
    "NotificationCategory",
    "NotificationClassification",
    "NotificationDisabledCategory",
    "NotificationDisabledClassification",
    "NotificationMinimumSeverity",
    "NotificationSeverity",
    "PreviewWarning",
    "Query",
    "Record",
    "Result",
    "ResultSummary",
    "RoutingControl",
    "ServerInfo",
    "Session",
    "SummaryCounters",
    "SummaryInputPosition",
    "SummaryNotification",
    "Transaction",
    "TrustAll",
    "TrustCustomCAs",
    "TrustSystemCAs",
    "__version__",
    "basic_auth",
    "bearer_auth",
    "custom_auth",
    "get_user_agent",
    "kerberos_auth",
    "unit_of_work",
]


def __getattr__(name) -> _t.Any:
    # TODO: 7.0 - consider removing this
    if name == "SummaryNotification":
        _deprecation_warn(
            "SummaryNotification and related APIs are deprecated. "
            "Use GqlStatusObjects and related APIs instead.",
            stack_level=2,
        )
        return _SummaryNotification
    if name == "NotificationCategory":
        _deprecation_warn(
            "NotificationCategory is deprecated. "
            "Use NotificationClassification instead.",
            stack_level=2,
        )
        return _NotificationCategory
    if name == "NotificationDisabledCategory":
        _deprecation_warn(
            "NotificationDisabledCategory is deprecated. "
            "Use NotificationDisabledClassification instead.",
            stack_level=2,
        )
        return _NotificationDisabledCategory
    # TODO: 7.0 - remove this
    if name == "PreviewWarning":
        _deprecation_warn(
            f"Importing {name} from `neo4j` is deprecated and will be removed"
            f"in a future version. Import it from `neo4j.warnings` instead.",
            stack_level=2,
        )
        return _PreviewWarning
    raise AttributeError(f"module {__name__} has no attribute {name}")


def __dir__() -> list[str]:
    return __all__
