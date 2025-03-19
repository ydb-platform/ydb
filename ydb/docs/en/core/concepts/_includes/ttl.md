# Time to Live (TTL) and eviction to external storage

This section describes how the TTL mechanism works and what its limits are.

## How it works {#how-it-works}

The table's TTL is a sequence of storage tiers. Each tier contains an expression (TTL expression) and an action. When the expression is triggered, that tier is assigned to the row. When a tier is assigned to a row, the specified action is automatically performed: moving the row to external storage or deleting it. External storage is represented by the [external data source](../datamodel/external_data_source.md) object.

{{ ydb-short-name }} allows you to specify a column (TTL column) whose values are used in TTL expressions. The expression is triggered when the specified number of seconds has passed since the time recorded in the TTL column. For rows with `NULL` value in TTL column, the expression is not triggered.

The timestamp for deleting a table item is determined by the formula:

```text
eviction_time = valueof(ttl_column) + evict_after_seconds
```

{% note info %}

TTL doesn't guarantee that the item will be deleted exactly at `eviction_time`, it might happen later. If it's important to exclude logically obsolete but not yet physically deleted items from the selection, use query-level filtering.

{% endnote %}

Data is deleted by the *Background Removal Operation* (*BRO*), consisting of two stages:

1. Checking the values in the TTL column.
2. Deleting expired data.

The *BRO* has the following properties:

* The concurrency unit is a [table partition](../datamodel/table.md#partitioning).
* For tables with [secondary indexes](../secondary_indexes.md), the delete stage is a [distributed transaction](../transactions.md#distributed-tx).

## Guarantees {#guarantees}

* For the same partition *BRO* is run at the intervals set in the TTL settings. The default run interval is 1 hour, the minimum allowable value is 15 minutes.
* Data consistency is guaranteed. The TTL column value is re-checked during the delete stage. This means that if the TTL column value is updated between stages 1 and 2 (for example, with `UPDATE`) and ceases to meet the delete criteria, the row will not be deleted.

## Limitations {#restrictions}

* The TTL column must be of one of the following types:

  * `Date`.
  * `Datetime`.
  * `Timestamp`.
  * `Uint32`.
  * `Uint64`.
  * `DyNumber`.

* The value in the TTL column with a numeric type (`Uint32`, `Uint64`, or `DyNumber`) is interpreted as a [Unix time]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unix_time){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Unix-время){% endif %} value. The following units are supported (set in the TTL settings):

  * Seconds.
  * Milliseconds.
  * Microseconds.
  * Nanoseconds.

* You can't specify multiple TTL columns.
* You can't delete the TTL column. However, if this is required, you should first [disable TTL](../../yql/reference/recipes/ttl.md#disable) for the table.
* Only {{ objstorage-name }} is supported as external storage.
* The delete action can only be specified for the last tier.

## Setup {#setting}

Currently, you can manage TTL settings using:

* [YQL](../../yql/reference/recipes/ttl.md).
* [{{ ydb-short-name }} console client](../../recipes/ydb-cli/ttl.md).
* {{ ydb-short-name }} {% if oss %}C++, {% endif %}Go and Python [SDK](../../recipes/ydb-sdk/ttl.md).
