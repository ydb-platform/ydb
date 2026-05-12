# Transaction lock invalidation

{{ ydb-short-name }} uses [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to find conflicts with other transactions being executed. If the locks check during the commit phase reveals conflicting modifications, the committing transaction rolls back and must be restarted. In this case, {{ ydb-short-name }} returns a **transaction locks invalidated** error. Restarting a significant share of transactions can degrade your application's performance.

{% note info %}

The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

{% endnote %}


## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

## Recommendations

Consider the following recommendations:

- The longer a transaction lasts, the higher the likelihood of encountering a **transaction locks invalidated** error.

    If possible, avoid [interactive transactions](../../../concepts/glossary.md#interactive-transaction). A better approach is to use a single YQL query with `begin;` and `commit;` to select data, update data, and commit the transaction.

<<<<<<< HEAD
    If you do need interactive transactions, perform `commit` in the last query in the transaction.
=======
### Analysis via system views

The following system views are available for analyzing lock conflicts:

#### Query-level analysis
>>>>>>> 4ed6bd91bf6 (Docs: Add more sys_view about broken locks (#39965))

- Analyze the range of primary keys where conflicting modifications occur, and try to change the application logic to reduce the number of conflicts.

<<<<<<< HEAD
    For example, if a single row with a total balance value is frequently updated, split this row into a hundred rows and calculate the total balance as a sum of these rows. This will drastically reduce the number of **transaction locks invalidated** errors.
=======
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

#### Partition-level analysis

To analyze broken locks at the table partition level, use the following system views:

* [`.sys/partition_stats`](../../../dev/system-views.md#partitions) — current partition statistics, contains the cumulative `LocksBroken` field
* [`.sys/top_partitions_by_tli_one_minute`](../../../dev/system-views.md#top-tli-partitions) — top 10 partitions with non-zero broken locks for one-minute intervals
* [`.sys/top_partitions_by_tli_one_hour`](../../../dev/system-views.md#top-tli-partitions) — top 10 partitions with non-zero broken locks for one-hour intervals

Example query to find partitions with the most broken locks:

```sql
SELECT
    Path,
    SUM(LocksBroken) as TotalLocksBroken
FROM `.sys/partition_stats`
GROUP BY Path
ORDER BY TotalLocksBroken DESC
LIMIT 10;
```

Example query to view the history of broken locks by partition:

```sql
SELECT
    IntervalEnd,
    LocksBroken,
    Path
FROM `.sys/top_partitions_by_tli_one_hour`
WHERE IntervalEnd BETWEEN Timestamp("2000-01-01T00:00:00Z") AND Timestamp("2099-12-31T00:00:00Z")
ORDER BY IntervalEnd DESC, LocksBroken DESC
LIMIT 100;
```
>>>>>>> 4ed6bd91bf6 (Docs: Add more sys_view about broken locks (#39965))
