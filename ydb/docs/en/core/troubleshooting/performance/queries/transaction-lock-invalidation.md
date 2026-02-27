# Transaction lock invalidation

**[Transaction lock invalidation](../../../concepts/glossary.md#tli)** (TLI) occurs when one transaction (the breaker) writes data and breaks the optimistic locks of another transaction (the victim). The victim detects this at commit time and receives a `transaction locks invalidated` error. The transaction must be retried. Frequent retries degrade application performance.

{% note info %}

The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

{% endnote %}

## Preventing conflicts

- **Shorten transaction duration.** The longer a transaction holds locks, the higher the likelihood of a conflict. Where possible, avoid [interactive transactions](../../../concepts/glossary.md#interactive-transaction): the best approach is a single YQL query with `BEGIN;` and `COMMIT;` to read, modify, and commit data. If interactive transactions are necessary, execute `COMMIT` in the last query.

- **Reduce data overlap between transactions.** The fewer rows a transaction reads, the fewer locks it holds and the lower the chance of a conflict. Avoid reading unnecessary data. If multiple transactions compete for the same rows, reconsider your data model: for example, instead of a single row with a total balance, use a hundred rows and compute the balance as a sum.

- **Use read-only transaction modes.** Transactions in [`Snapshot Read-Only`](../../../concepts/transactions.md#modes) mode read data from a consistent snapshot and do not acquire optimistic locks — such a transaction can never become a TLI victim. If a transaction does not modify data, explicitly set this mode in the SDK.


## Diagnostics

### Monitoring in Grafana

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

### Diagnostics via TLI logs

When a transaction fails with a TLI error, the error message contains the victim query identifier:

```text
Transaction locks invalidated. ... VictimQuerySpanId: 1111111111111111.
```

Using this `VictimQuerySpanId`, you can find the full conflict context in the server logs: which query acquired the locks and which one broke them. For details on enabling logging, log entry format, event correlation, and the `find_tli_chain` utility for automated log analysis, see [{#T}](tli-logging.md).

### Analysis via system view

To identify queries with the most conflicts, use the [`.sys/query_metrics_one_minute`](../../../dev/system-views.md#query-metrics) system view:

```sql
SELECT QueryText, LocksBrokenAsBreaker, LocksBrokenAsVictim
FROM `.sys/query_metrics_one_minute`
WHERE LocksBrokenAsBreaker > 0 OR LocksBrokenAsVictim > 0
ORDER BY LocksBrokenAsBreaker + LocksBrokenAsVictim DESC;
```

| Column | Description |
|:-------|:------------|
| `LocksBrokenAsBreaker` | How many times this query broke other transactions' locks |
| `LocksBrokenAsVictim` | How many times this query's locks were broken |

Queries with a high `LocksBrokenAsBreaker` are breakers: they cause other transactions to roll back. Queries with a high `LocksBrokenAsVictim` are victims.
