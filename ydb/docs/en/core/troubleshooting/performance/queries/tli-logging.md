# TLI Logging

[Transaction lock invalidation](../../../concepts/glossary.md#tli) (`TLI`) logging lets you identify which query had its locks broken **(the victim)** and which query broke them **(the breaker)**.

## Enabling Logging

To get detailed [logs](../../../devops/observability/logging.md) about lock conflicts, set the `INFO` level (numeric value `6`) for the `TLI` component.

Add the following to the cluster configuration:

```yaml
log_config:
  entry:
    - component: "TLI"
      level: 6  # INFO
```

The `log_config` parameter [supports dynamic updates](../../../devops/configuration-management/configuration-v1/dynamic-config.md) without node restarts.

Once enabled, the server writes logs on every broken lock, and `VictimQuerySpanId` begins appearing in SDK error messages.

{% note info %}

The base log size for a single `TLI` event is 10–20 KB. The actual volume depends on the size of the text of the logged queries (the breaker and the victim) and the transactions of which they are part, since all this data is recorded in the log.
If conflicts are infrequent, the logging overhead is negligible. At high `TLI` rates, the total log volume can be significant.

To exclude tables with expected conflicts from `TLI` diagnostics, use the [`tli_config.ignored_table_regexes`](../../../reference/configuration/tli_config.md) parameter.

{% endnote %}

## Log Structure

When `TLI` occurs, the server writes log entries from each component (`DataShard` or `SessionActor`) for each side of the conflict (victim or breaker). The number of entries depends on the number of queries in the transactions.

**Example scenario:**

Two sessions are established to the DBMS: victim and breaker.

| Time | QuerySpanId | Role | Query |
|:------|:------------|:-----|:-------|
| T1 | `1111111111111111` | Victim | `SELECT * FROM Orders WHERE OrderId = 42` (acquires lock) |
| T2 | `2222222222222222` | Breaker | `UPDATE Orders SET Status = 'done' WHERE OrderId = 42` (breaks lock) |
| T3 | `3333333333333333` | Victim | `UPDATE Orders SET Amount = 100 WHERE OrderId = 42` (commit fails) |

{% note info %}

In the log content examples below, the order of individual fields within entries may differ.

{% endnote %}

**Breaker log (DataShard):**

```text
component=DataShard tabletId=<tablet-id> message="Write transaction broke other locks" breakerQuerySpanId=2222222222222222 victimQuerySpanId=1111111111111111
```

**Breaker log (SessionActor):**

```text
component=SessionActor message="Query had broken other locks" breakerTxSpanId=2222222222222222 querySpanId=2222222222222222 queryText="UPDATE Orders SET Status = 'done' WHERE OrderId = 42"
```

**Victim log (DataShard):**

```text
component=DataShard tabletId=<tablet-id> message="Write transaction was a victim of broken locks" victimQuerySpanId=1111111111111111
```

**Victim log (SessionActor):**

In this example, SessionActor writes two entries — one for each query of the victim transaction:

```text
component=SessionActor message="Query was a victim of broken locks" victimTxSpanId=1111111111111111 querySpanId=1111111111111111 queryText="SELECT * FROM Orders WHERE OrderId = 42"

component=SessionActor message="Query was a victim of broken locks" victimTxSpanId=1111111111111111 querySpanId=3333333333333333 queryText="UPDATE Orders SET Amount = 100 WHERE OrderId = 42"
```

## Log Fields

| Field | Description | Where it appears |
|:-----|:---------|:----------------|
| `component` | Source of the entry: `DataShard` or `SessionActor` | All `TLI` logs |
| `message` | Event type (see examples above) | All `TLI` logs |
| `tabletId` | Identifier of the DataShard tablet | DataShard logs |
| `victimQuerySpanId` | Identifier of the query whose locks were broken | Victim DataShard logs, breaker DataShard logs |
| `breakerQuerySpanId` | Identifier of the query that broke the locks | Breaker DataShard logs |
| `victimTxSpanId` | Identifier of the victim query whose locks were broken. Matches victimQuerySpanId in DataShard logs. | Victim SessionActor logs |
| `breakerTxSpanId` | Identifier of the breaker query that broke the locks. Matches breakerQuerySpanId in DataShard logs. | Breaker SessionActor logs |
| `querySpanId` | Identifier of a specific query in the SessionActor entry | SessionActor logs |
| `queryText` | SQL of a specific query in the SessionActor entry | SessionActor logs |

## Log Analysis

Using the `VictimQuerySpanId` from the SDK error message, you can find all related events:

1. **Find the victim query ID**: in the victim log, the `victimQuerySpanId` field contains the ID of the `SELECT` query that acquired the broken locks.

2. **Find the victim query text**: in the victim log, there is an entry whose `victimTxSpanId` matches `querySpanId`. The `queryText` field of this entry contains the text of the `SELECT` query that acquired the broken locks.

3. **Find the breaker query ID**: in the breaker DataShard log, there is an entry whose `victimQuerySpanId` field contains the same value (the victim query ID). From this entry, take the value of `breakerQuerySpanId`, which is the breaker query ID.

4. **Find the breaker query text**: in the breaker log, there is an entry whose `breakerTxSpanId` contains the breaker query ID and at the same time matches `querySpanId`. The `queryText` field of this entry contains the text of the breaker query.

5. **Get full transaction context**: all SessionActor entries with the same `victimTxSpanId` contain information about the queries of the victim transaction. Similarly, all SessionActor entries with the same `breakerTxSpanId` contain information about the queries of the breaker transaction.

## find_tli_chain Utility

For automatic `TLI` log analysis, the {{ ydb-short-name }} repository includes the [`find_tli_chain.py`](https://github.com/ydb-platform/ydb/tree/main/ydb/tools/tli_analysis) utility.

This utility takes the following parameters:

- `VictimQuerySpanId` - identifier of the broken query from the SDK error message;
- `LogFile` - name of the log file for log analysis.

It is assumed that `TLI` logging was previously enabled in the [configuration](../../../reference/configuration/log_config.md), log files were collected from database nodes and merged into one file.

```bash
python3 find_tli_chain.py <VictimQuerySpanId> <LogFile>
```

**Example:**

```bash
python3 find_tli_chain.py 1111111111111111 ydb.log
```

```text
================================================
  TLI Chain
================================================

VictimQuerySpanId: 1111111111111111

VictimQueryText: SELECT * FROM Orders WHERE OrderId = 42

BreakerQuerySpanId: 2222222222222222

BreakerQueryText: UPDATE Orders SET Status = 'done' WHERE OrderId = 42

================================================
  VictimTx
================================================

SELECT * FROM Orders WHERE OrderId = 42

UPDATE Orders SET Amount = 100 WHERE OrderId = 42

================================================
  BreakerTx
================================================

UPDATE Orders SET Status = 'done' WHERE OrderId = 42
```

The utility outputs:

- **TLI Chain** — `VictimQuerySpanId`, `VictimQueryText`, `BreakerQuerySpanId`, `BreakerQueryText`;
- **VictimTx** — all queries of the victim transaction;
- **BreakerTx** — all queries of the breaker transaction.

Additional parameters:

| Parameter | Description |
|:---------|:---------|
| `--window-sec N` | Time window for searching around the event (default: 10 s) |
| `--no-color` | Disable colored output |

{% note info %}

The utility correctly handles unsorted and mixed log files: filtering is based on timestamp values, not line positions.

{% endnote %}
