# KILL SESSION

`KILL SESSION` forcibly terminates a [query execution session](../../../concepts/query_execution/execution_process.md#sessions) in the current database. Use it to stop a query that causes unwanted load or close an unnecessary long-lived session.

The statement runs through Query Service and can terminate both Query Service and Table Service sessions, including sessions on another node. It does not terminate topic read or write sessions or Coordination Service sessions.

## Syntax {#syntax}

To terminate one session, enclose its identifier in [backticks](lexer.md#keywords-and-ids):

```yql
KILL SESSION `<session_id>`;
```

Where `<session_id>` is a value from the `SessionId` column of the [`.sys/query_sessions`](../../../dev/system-views.md#query-sessions) system view.

For programmatic calls, pass the identifier as a separate `Utf8` parameter declared with [`DECLARE`](declare.md):

```yql
DECLARE $session_id AS Utf8;
KILL SESSION $session_id;
```

Pass the value of `$session_id` through the SDK or another client separately from the query text. The parameter must contain the full ID of an existing session; `NULL` and an empty string are invalid. Do not use backticks around the parameter.

The current version accepts one ID per statement. ID lists and subqueries that select IDs are not supported.

## Example: find and terminate a session {#examples}

First, find sessions whose queries have been running for at least 30 seconds. The following [`SELECT`](select/index.md) uses [`CurrentUtcTimestamp`](../builtins/basic.md#current-utc) and an [`Interval` literal](../types/primitive.md):

```yql
SELECT SessionId, UserSID, QueryStartAt, Query
FROM `.sys/query_sessions`
WHERE State = "EXECUTING"
  AND QueryStartAt <= CurrentUtcTimestamp() - Interval("PT30S")
ORDER BY QueryStartAt
LIMIT 20;
```

Choose the required `SessionId` from the results and submit a separate query. For example:

```yql
KILL SESSION `ydb://session/3?node_id=52910&id=MWFmNGYwYTAtYzJkN2RhOWEtZmFkMDhlMTUtZjU0ZDE0OTA%3D`;
```

Replace the example ID with a session identifier from your database.

## Permissions {#permissions}

The statement checks [access rights](grant.md#permissions-list) on the root of the current database:

- A user with `CONNECT` (`ydb.database.connect`) can terminate their own sessions. Ownership requires the caller's non-empty [SID](../../../security/authorization.md#sid) to match the session creator's non-empty SID. Sharing a user group does not establish ownership.
- To terminate other users' sessions, the caller also needs `UPDATE ROW` (`ydb.granular.update_row`) on the database root. This permission on a table or subdirectory is insufficient.
- Cluster administrators, and database administrators when [`enable_database_admin`](../../../reference/configuration/feature_flags.md) is enabled, can terminate any session in that database.

For example, the following [`GRANT`](grant.md) allows `session_operator` to terminate any session in `/Root/my_database`:

```yql
GRANT CONNECT, UPDATE ROW ON `/Root/my_database` TO session_operator;
```

Permissions can be granted directly to a user or through a group. Permission sets that include `UPDATE ROW` also qualify, provided the caller has `CONNECT`.

## Result and limitations {#behavior}

The statement returns no result set. `SUCCESS` means that the session has been removed after its cleanup has finished, including rollback of open transactions.

If the query running on the session can be cancelled, its client receives `CANCELLED` and a message identifying `KILL SESSION`. The message includes the initiator's SID when it is known. When [handling errors](../../../reference/ydb-sdk/error_handling.md), do not automatically restart a query that was administratively cancelled.

{% note warning %}

Terminating a session does not undo committed changes. A write that has reached a stage where cancellation is no longer possible may finish successfully. A DDL operation that has already been accepted for execution may also continue after the session is closed.

{% endnote %}

`KILL SESSION` runs outside user transactions because closing another session cannot be rolled back together with data changes. In the SDK, use a mode without an explicit transaction, such as `TTxControl::NoTx()` in the C++ Query Client. The legacy `TableService/ExecuteSchemeQuery` interface does not execute this statement.

The statement cannot terminate its own execution session. Use another session to submit `KILL SESSION`.

Submit `KILL SESSION` as a separate query, without combining it with data queries.

If the wait times out or the connection is lost, session removal may already have started and may continue. Check whether the session still exists; another attempt to terminate a session that has already been removed returns an error.

## Errors {#errors}

On failure, the statement returns a status and a message explaining the cause. The main cases are:

| Status | Cause |
| --- | --- |
| `BAD_REQUEST` | Malformed ID or invalid parameter value. Parameter type errors may be detected earlier, during query compilation. |
| `UNAUTHORIZED` | Insufficient permissions. For a caller without permission to terminate other users' sessions, a missing session and denied access produce the same message: `Session not found or access denied`. |
| `PRECONDITION_FAILED` | The session was not found and the caller is allowed to terminate any session; an attempt to terminate the statement's own session; execution inside a user transaction. |
| `GENERIC_ERROR` | Unsupported combination of `KILL SESSION` with data queries. |
| `TIMEOUT` | The deadline for waiting for the operation has expired. |
| `UNAVAILABLE` | The session owner node could not be contacted or the request could not be delivered. |
| `UNSUPPORTED` | The feature is disabled by its feature flag. |
