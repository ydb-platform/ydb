# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

GRAPHQL_DOCUMENT: Final = "graphql.document"
"""
The GraphQL document being executed.
Note: If instrumentation can reliably identify and redact sensitive information it SHOULD do it.
"""

GRAPHQL_OPERATION_NAME: Final = "graphql.operation.name"
"""
The name of the operation being executed.
"""

GRAPHQL_OPERATION_TYPE: Final = "graphql.operation.type"
"""
The type of the operation being executed.
"""


class GraphqlOperationTypeValues(Enum):
    QUERY = "query"
    """GraphQL query."""
    MUTATION = "mutation"
    """GraphQL mutation."""
    SUBSCRIPTION = "subscription"
    """GraphQL subscription."""
