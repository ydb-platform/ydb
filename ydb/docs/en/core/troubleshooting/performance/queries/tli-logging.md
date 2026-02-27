# TLI logging

[Transaction lock invalidation](../../../concepts/glossary.md#tli) (TLI) logging lets you identify which query had its locks broken (the **victim**) and which query broke them (the **breaker**).

## Enabling logging

To get detailed logs about lock conflicts, set the `INFO` level (numeric value `6`) for the `TLI` component.

Add the following to the cluster configuration:

```yaml
log_config:
  entry:
    - component: "TLI"
      level: 6  # INFO
```

The `log_config` parameter [supports dynamic updates](../../../devops/configuration-management/configuration-v1/dynamic-config.md) without node restarts.

Once enabled, the server writes logs on every lock conflict, and `VictimQuerySpanId` begins appearing in SDK error messages.

{% note info %}

Each TLI event generates approximately 10–20 KB of logs. If conflicts are infrequent, the logging overhead is negligible. At high TLI rates, monitor the total log volume.

To exclude tables with expected conflicts from TLI diagnostics, use the [`tli_config.ignored_table_regexes`](../../../reference/configuration/tli_config.md) parameter.

{% endnote %}

## Log structure

When TLI occurs, the server writes four entries — one from each component for each side of the conflict.

**Example scenario:**

| Time | QuerySpanId | Role | Query |
|:-----|:------------|:-----|:------|
| T1 | `1111111111111111` | Victim | `SELECT * FROM Orders WHERE OrderId = 42` (acquires lock) |
| T2 | `2222222222222222` | Breaker | `UPDATE Orders SET Status = 'done' WHERE OrderId = 42` (breaks lock) |
| T3 | `3333333333333333` | Victim | `UPDATE Orders SET Amount = 100 WHERE OrderId = 42` (commit fails) |

**Breaker log (DataShard):**

```text
Component: DataShard, TabletId: <tablet-id>,
BreakerQuerySpanId: 2222222222222222, VictimQuerySpanIds: [1111111111111111],
Message: Write transaction broke other locks
```

**Breaker log (SessionActor):**

```text
Component: SessionActor, Message: Query had broken other locks,
BreakerQuerySpanId: 2222222222222222,
BreakerQueryText: UPDATE Orders SET Status = 'done' WHERE OrderId = 42,
BreakerQueryTexts: [QuerySpanId=2222222222222222 QueryText=UPDATE Orders SET Status = 'done' WHERE OrderId = 42]
```

**Victim log (DataShard):**

```text
Component: DataShard, TabletId: <tablet-id>,
VictimQuerySpanId: 1111111111111111, CurrentQuerySpanId: 3333333333333333,
Message: Write transaction was a victim of broken locks
```

**Victim log (SessionActor):**

```text
Component: SessionActor, Message: Query was a victim of broken locks,
VictimQuerySpanId: 1111111111111111, CurrentQuerySpanId: 3333333333333333,
VictimQueryText: SELECT * FROM Orders WHERE OrderId = 42,
VictimQueryTexts: [QuerySpanId=1111111111111111 QueryText=SELECT * FROM Orders WHERE OrderId = 42 |
                   QuerySpanId=3333333333333333 QueryText=UPDATE Orders SET Amount = 100 WHERE OrderId = 42]
```

## Log fields

| Field | Description | Where it appears |
|:------|:------------|:-----------------|
| `VictimQuerySpanId` | Identifier of the query whose locks were broken | Victim logs |
| `BreakerQuerySpanId` | Identifier of the query that broke the locks | Breaker logs |
| `CurrentQuerySpanId` | Identifier of the query at the time of the error (may differ from the victim) | Victim logs |
| `VictimQuerySpanIds` | Array of identifiers of all victim queries | Breaker DataShard logs |
| `VictimQueryText` | SQL of the victim query that acquired the locks | Victim SessionActor logs |
| `BreakerQueryText` | SQL of the breaker query | Breaker SessionActor logs |
| `VictimQueryTexts` | All queries of the victim transaction | Victim SessionActor logs |
| `BreakerQueryTexts` | All queries of the breaker transaction | Breaker SessionActor logs |

## Step-by-step log correlation

Using the `VictimQuerySpanId` from the SDK error message, you can find all related events:

1. **Find the victim query**: search for `VictimQuerySpanId: <value from the error>` in the logs — this shows which SELECT acquired the broken locks, with the query text in the `VictimQueryText` field.

2. **Find the breaker query**: the breaker DataShard log's `VictimQuerySpanIds` field contains the same value. From this log, take `BreakerQuerySpanId` and find the breaker's SessionActor log — it contains `BreakerQueryText` with the full query text.

3. **Get full transaction context**: `VictimQueryTexts` and `BreakerQueryTexts` contain all queries of the respective transactions in execution order.

## find_tli_chain utility

For automatic TLI log correlation, the {{ ydb-short-name }} repository includes the [`find_tli_chain.py`](https://github.com/ydb-platform/ydb/tree/main/ydb/tools/tli_analysis) utility.

It takes a `VictimQuerySpanId` from the error message and a log file, finds all related entries, and displays them in a convenient format:

```bash
python3 find_tli_chain.py <VictimQuerySpanId> <logfile>
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
- **VictimTx** — all queries of the victim transaction in execution order;
- **BreakerTx** — all queries of the breaker transaction.

Additional options:

| Option | Description |
|:-------|:------------|
| `--window-sec N` | Time window for searching around the event (default: 10 s) |
| `--no-color` | Disable colored output |

{% note info %}

The utility correctly handles unsorted and mixed log files: filtering is based on timestamp values, not line positions.

{% endnote %}
