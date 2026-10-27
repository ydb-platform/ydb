# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

DB_COLLECTION_NAME: Final = "db.collection.name"
"""
The name of a collection (table, container) within the database.
Note: It is RECOMMENDED to capture the value as provided by the application
without attempting to do any case normalization.

The collection name SHOULD NOT be extracted from `db.query.text`,
when the database system supports query text with multiple collections
in non-batch operations.

For batch operations, if the individual operations are known to have the same
collection name then that collection name SHOULD be used.
"""

DB_NAMESPACE: Final = "db.namespace"
"""
The name of the database, fully qualified within the server address and port.
Note: If a database system has multiple namespace components, they SHOULD be concatenated from the most general to the most specific namespace component, using `|` as a separator between the components. Any missing components (and their associated separators) SHOULD be omitted.
Semantic conventions for individual database systems SHOULD document what `db.namespace` means in the context of that system.
It is RECOMMENDED to capture the value as provided by the application without attempting to do any case normalization.
"""

DB_OPERATION_BATCH_SIZE: Final = "db.operation.batch.size"
"""
The number of database operations included in a batch operation.
Note: Except for empty batch requests described below, a batch operation contains two
or more database operations explicitly submitted as separate operations in a single
client call, protocol message, or database command.

Requests to batch APIs that contain only one operation SHOULD be modeled as single
operations, not as batch operations.

A database call is not a batch operation solely because one operation accepts
multiple operands, such as keys, rows, documents, points, or other data elements,
including Redis [`MGET`](https://redis.io/docs/latest/commands/mget/) with
multiple keys.

In batch APIs that execute the same parameterized operation with parameter sets,
each parameter set represents one database operation for determining whether the
request is a batch operation. Requests with only one parameter set SHOULD be modeled
as single operations, not as batch operations.

`db.operation.batch.size` SHOULD be set to the number of operations in the batch.
It SHOULD NOT be set for non-batch operations.

A request to execute a batch operation with no operations SHOULD also be treated
as a batch operation, and `db.operation.batch.size` SHOULD be set to `0`.
"""

DB_OPERATION_NAME: Final = "db.operation.name"
"""
The name of the operation or command being executed.
Note: It is RECOMMENDED to capture the value as provided by the application
without attempting to do any case normalization.

The operation name SHOULD NOT be extracted from `db.query.text`,
when the database system supports query text with multiple operations
in non-batch operations.

If spaces can occur in the operation name, multiple consecutive spaces
SHOULD be normalized to a single space.

For batch operations, if the individual operations are known to have the same operation name
then that operation name SHOULD be used prepended by `BATCH `,
otherwise `db.operation.name` SHOULD be `BATCH` or some other database
system specific term if more applicable.
"""

DB_QUERY_SUMMARY: Final = "db.query.summary"
"""
Low cardinality summary of a database query.
Note: The query summary describes a class of database queries and is useful
as a grouping key, especially when analyzing telemetry for database
calls involving complex queries.

Summary may be available to the instrumentation through
instrumentation hooks or other means. If it is not available, instrumentations
that support query parsing SHOULD generate a summary following
[Generating query summary](/docs/db/database-spans.md#generating-a-summary-of-the-query)
section.

For batch operations, if the individual operations are known to have the same query summary
then that query summary SHOULD be used prepended by `BATCH `,
otherwise `db.query.summary` SHOULD be `BATCH` or some other database
system specific term if more applicable.
"""

DB_QUERY_TEXT: Final = "db.query.text"
"""
The database query being executed.
Note: For sanitization see [Sanitization of `db.query.text`](/docs/db/database-spans.md#sanitization-of-dbquerytext).
For batch operations, if the individual operations are known to have the same query text then that query text SHOULD be used, otherwise all of the individual query texts SHOULD be concatenated with separator `; ` or some other database system specific separator if more applicable.
Parameterized query text SHOULD NOT be sanitized. Even though parameterized query text can potentially have sensitive data, by using a parameterized query the user is giving a strong signal that any sensitive data will be passed as parameter values, and the benefit to observability of capturing the static part of the query text by default outweighs the risk.
"""

DB_RESPONSE_STATUS_CODE: Final = "db.response.status_code"
"""
Database response status code.
Note: The status code returned by the database. Usually it represents an error code, but may also represent partial success, warning, or differentiate between various types of successful outcomes.
Semantic conventions for individual database systems SHOULD document what `db.response.status_code` means in the context of that system.
"""

DB_STORED_PROCEDURE_NAME: Final = "db.stored_procedure.name"
"""
The name of a stored procedure within the database.
Note: It is RECOMMENDED to capture the value as provided by the application
without attempting to do any case normalization.

For batch operations, if the individual operations are known to have the same
stored procedure name then that stored procedure name SHOULD be used.
"""

DB_SYSTEM_NAME: Final = "db.system.name"
"""
The database management system (DBMS) product as identified by the client instrumentation.
Note: The actual DBMS may differ from the one identified by the client. For example, when using PostgreSQL client libraries to connect to a CockroachDB, the `db.system.name` is set to `postgresql` based on the instrumentation's best knowledge.
"""


class DbSystemNameValues(Enum):
    MARIADB = "mariadb"
    """[MariaDB](https://mariadb.org/)."""
    MICROSOFT_SQL_SERVER = "microsoft.sql_server"
    """[Microsoft SQL Server](https://www.microsoft.com/sql-server)."""
    MYSQL = "mysql"
    """[MySQL](https://www.mysql.com/)."""
    POSTGRESQL = "postgresql"
    """[PostgreSQL](https://www.postgresql.org/)."""
