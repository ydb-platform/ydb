# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

CASSANDRA_CONSISTENCY_LEVEL: Final = "cassandra.consistency.level"
"""
The consistency level of the query. Based on consistency values from [CQL](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html).
"""

CASSANDRA_COORDINATOR_DC: Final = "cassandra.coordinator.dc"
"""
The data center of the coordinating node for a query.
"""

CASSANDRA_COORDINATOR_ID: Final = "cassandra.coordinator.id"
"""
The ID of the coordinating node for a query.
"""

CASSANDRA_PAGE_SIZE: Final = "cassandra.page.size"
"""
The fetch size used for paging, i.e. how many rows will be returned at once.
"""

CASSANDRA_QUERY_IDEMPOTENT: Final = "cassandra.query.idempotent"
"""
Whether or not the query is idempotent.
"""

CASSANDRA_SPECULATIVE_EXECUTION_COUNT: Final = (
    "cassandra.speculative_execution.count"
)
"""
The number of times a query was speculatively executed. Not set or `0` if the query was not executed speculatively.
"""


class CassandraConsistencyLevelValues(Enum):
    ALL = "all"
    """All."""
    EACH_QUORUM = "each_quorum"
    """Each Quorum."""
    QUORUM = "quorum"
    """Quorum."""
    LOCAL_QUORUM = "local_quorum"
    """Local Quorum."""
    ONE = "one"
    """One."""
    TWO = "two"
    """Two."""
    THREE = "three"
    """Three."""
    LOCAL_ONE = "local_one"
    """Local One."""
    ANY = "any"
    """Any."""
    SERIAL = "serial"
    """Serial."""
    LOCAL_SERIAL = "local_serial"
    """Local Serial."""
