# DML operations audit

DML operations audit logging allows to monitor user access to the data.

{% note info "" %}

DML stands for [Data Manipulation Language](https://en.wikipedia.org/wiki/Data_manipulation_language) &mdash; subset of SQL-data statements for modifying or accessing data stored in the tables.

{% endnote %}

## Record attributes

| __Attribute__ | __Description__ |
|:----|:----|
| `component` | Logging component name, always `grpc-proxy`.
| `remote_address` | IP address of the client who sent the request.
| `subject` | User SID (account name) of the user on whose behalf the operation is performed.
| `database` | Path of the database in which the operation is performed.
| `operation` | Request type.
| `start_time` | Operation start time.
| `end_time` | Operation end time.
| `status` | General operation status: `SUCCESS` or `ERROR`.
| `detailed_status` | Internal operation status.

## Operation specific attributes

|  __Type__[**](*popup-note) | __Attribute__ | __Description__ |
|:----|:----|:----|
| _PrepareDataQuery_ | `query_text` | Query text.
|| `prepared_query_id` | Identifier of the prepared query.
| _BeginTransaction<br>CommitTransaction<br>RollbackTransaction_ | `tx_id` | Unique transaction identifier.
| _ExecuteDataQuery_ | `query_text` | Query text.<br>Either query_text or prepared_query_id is present in the record.
|| `prepared_query_id` | Identifier of the previously prepared query.<br>Either query_text or prepared_query_id is present in the record.
|| `tx_id` | Unique transaction identifier.
|| `begin_tx` | Request for implicit transaction creation at query start.
|| `commit_tx` | Request for implicit transaction commit upon query completion.
| _ExecuteYql<br>ExecuteScript_ | `query_text` | Query text.
| _ExecuteQuery_ | `query_text` | Query text.
|| `tx_id` | Unique transaction identifier.
|| `begin_tx` | Request for implicit transaction start.
|| `commit_tx` | Request for implicit transaction commit upon query completion.
| _BulkUpsert_ | `table` | Path of the modified table.
|| `row_count` | Number of added and updated rows.

## Configuration

The audit logging must be [enabled](audit-log.md#enabling-audit-log) on the cluster level.

DML operations logging require additional configuration at the database level. It is not possible to enable DML logging globally or only for a specific table.

Each database has two settings:
- `EnableDmlAudit` &mdash; a flag for enabling logging (default value: false)
- `ExpectedSubjects` &mdash; a list of user SIDs, actions from which are expected (and thus are of no interest) and should not be included in the audit log (default value: empty)

These settings are part of the database schema:
- They are set and changed by DDL operations for creating and modifying the database.
- The current state of the settings is shown in the response to the [describe](../reference/ydb-cli/commands/scheme-describe.md) database command.
    <br>For example:
    ```yaml
    PathDescription:
        DomainDescription:
            AuditSettings:
                EnableDmlAudit: true
                ExpectedSubjects:
                    0: "user2@ad"
                    1: "user1"
    ```

### How to enable

Example command to enable DML operation audit logging on the database `/root/db`:

```shell
$ cat >enable-dml-audit.txt <<END
ModifyScheme {
    OperationType: ESchemeOpAlterExtSubDomain
    WorkingDir: "/root"
    SubDomain {
        Name: "db"
        AuditSettings {
            EnableDmlAudit: true
            ExpectedSubjects: ["user2@ad", "user1"]
        }
    }
}
END
$ ./ydbd --server=HOST database schema exec enable-dml-audit.txt
```

Changes to the settings are made in the same way.

{% note info "" %}

`EnableDmlAudit` and `ExpectedSubjects` can be changed independently of each other.

The new value of `ExpectedSubjects` completely replaces the old one.
`ExpectedSubjects: [""]` clears the list.

{% endnote %}

## Things to know

- Logging occurs at the {{ ydb-short-name }} component `gRPC Proxy`.

- DML operation logging consumes both CPU resources and disk space, which might become an issue when there's a high rate of such queries. `EnableDmlAudit` and `ExpectedSubjects` at the database level help regulate the volume of audited events.

- Request details are captured before query text gets parsed, so detailed information about the query (expression type or list of affected tables) is unavailable.

- As a consequence, `SELECT` are also getting logged, along with `UPDATE`, `INSERT`, `DELETE` etc.

- As a consequence, query text is getting logged as is, with all literal values. Use [query parameters](../reference/ydb-sdk/parameterized_queries.md) to avoid getting literals into the log.

- The log only captures requests that have passed [authentication](../deploy/configuration/config#auth) and authorization checks against the database. Anonymous requests can not be logged (can't pass `ExpectedSubjects` filter).

- Each request is logged as a single audit record. `start_time` and `end_time` mark start and end times of the operation. Log record is made at the time when operation is completed (successful or not).

- In `query_text`, multiline query text is collapsed into a single line with the removal of extra whitespace characters. The resulting string is then truncated to 1024 bytes.

- Only user SIDs will work in the `ExpectedSubjects`, group SIDs will not.

## Log record examples

```json
{
    "component": "grpc-proxy",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "ExecuteDataQueryRequest",
    "start_time": "2023-11-03T20:40:53.897285Z",
    "query_text": "--!syntax_v1 PRAGMA TablePathPrefix(\"/root/db/.sys_health\"); $values = ( SELECT t1.id AS id, t1.value AS t1_value, t2.value AS t2_value FROM table1 AS t1 INNER JOIN table2 AS t2 ON t1.id == t2.id ); UPSERT INTO table1 SELECT id, t1_value + 1 as value FROM $values; UPSERT INTO table2 SELECT id, t2_value + 1 as value FROM $values; SELECT COUNT(*) FROM $values;",
    "begin_tx": "1",
    "commit_tx": "1",
    "tx_id": "{none}",
    "end_time": "2023-11-03T20:40:53.950970Z",
    "status": "SUCCESS",
    "detailed_status": "SUCCESS"
}
```

[*popup-note]: The suffix `Request` is omitted for readability.<br>For example, `ExecuteDataQuery` here stands for `ExecuteDataQueryRequest` in the log.
