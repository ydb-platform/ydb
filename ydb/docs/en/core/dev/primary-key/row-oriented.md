# Selecting a primary key for maximum performance

The way columns are selected for a table's primary key defines YDB's ability to scale load and improve performance.

General recommendations for choosing a primary key:

* Avoid situations where the main load falls on one [partition](../../concepts/datamodel/table.md#partitioning) of a table. The more evenly load is distributed across partitions, the better the performance.
* Reduce the number of partitions that can be affected in a single request. Moreover, if the request affects no more than one partition, it is performed using a special simplified protocol. This significantly increases the speed and saves the resources.

All {{ ydb-short-name }} tables are sorted by primary key in ascending order. In a table with a monotonically increasing primary key, this will result in new data being added at the end of a table. As {{ ydb-short-name }} splits table data into partitions based on key ranges, inserts are always processed by the same server that is responsible for the "last" partition. Concentrating the load on a single server results in slow data uploading and inefficient use of a distributed system.

You get the same behaviour if the monotonically increasing value is the **only** primary-key column or the **first component of a composite primary key**: partition ranges are driven by the key prefix, so monotonicity at the beginning of the key produces the same hot-spot pattern. Auto-increment types (`SERIAL`), when used as the sole or leading component of `PRIMARY KEY`, behave in the same way.

As an example, let's take logging of user events to a table with the `( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )` schema.

The values in the `timestamp` column increase monotonically resulting in all new records being added at the end of a table, and the final partition, which is responsible for this range of keys, handles all the table inserts. This makes scaling insert loads impossible and performance will be limited by the single process servicing this partition and won't increase as new servers are added to a cluster.

{{ ydb-short-name }} supports further automatic partition splitting upon a threshold size or load being reached. However, in this situation, once it splits off, the new partition will again begin handling all the inserts, and the situation will recur.

## Techniques that let you evenly distribute load across table partitions {#balance-shard-load}

### Changing the sequence of key components {#key-order}

Writing data to a table with the `( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )` schema results in an uneven load on table partitions due to a monotonically increasing primary key. Changing the sequence of key components so that the monotonically increasing part isn't the first component can help distribute the load more evenly. If you redefine a table's primary key as `PRIMARY KEY (userid, timestamp)`, the DB writes will distribute more evenly across the partitions provided there is a sufficient number of users generating events.

### Using a hash of key column values as a primary key {#key-hash}

To obtain a more even distribution of operations across a table's partitions and reduce the size of internal data structures, make the primary key "prefix" (initial part) values more varied. To do this, make the primary key include the value of a hash of the entire primary key or a part of the primary key.

For instance, the schema of this table with the schema `( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )` might be made to include an additional field computed as a hash: `userhash = HASH(userid)`. This would change the table schema as follows:

```yql
( userhash, userid, timestamp, userevent, PRIMARY KEY (userhash, userid, timestamp) )
```

If you select the hash function properly, rows will be distributed fairly evenly throughout the entire key space, which will result in a more even load on the system. At the same time, the fact that the key includes `userid, timestamp` after `userhash` keeps the data local and sorted by time for a specific user.

The `userhash` field in the example above must be computed by the application and specified explicitly both for inserting new records into the table and for data access by primary key.

Another common special case of the same idea is monotonically increasing identifiers (`SERIAL`, a sequence, or a sequential numeric id)—not only the timestamp from the log example above.

#### Monotonically increasing identifier (SERIAL, sequence, auto increment) {#monotonic-serial}

Don't use a monotonically increasing value as the sole primary-key column or as the **first** component of a composite primary key. Typical examples are an auto-increment id (`SERIAL`), a value from a sequence, or a sequential numeric id.

Row-oriented tables in {{ ydb-short-name }} are partitioned by ranges of the primary key, so rows with a monotonic key keep landing in the last partition and create a hot spot: most write load concentrates on a small set of partitions while others are underused.

The same issue applies to a composite key when the **first** component increases monotonically—for example, `PRIMARY KEY (order_id, customer_id)`.

{{ ydb-short-name }} supports auto-increment types (`SERIAL`), but using them as the only or leading primary-key column isn't recommended for insert-heavy workloads.

Prefer a random identifier (such as UUID) or a composite business key without a monotonic prefix whenever possible. See [UUID as a primary key](#uuid-primary-key) for guidance on UUID layout. If you must keep a monotonically increasing id in the key, use a composite primary key where the **first** column is a hash of the id and the **second** is the id itself. That tends to yield **more even** distribution of rows across partitions while preserving uniqueness (a hash does not guarantee a perfectly balanced split).

{% cut "Poor example" %}

```yql
CREATE TABLE orders (
    order_id Uint64,
    customer_id Uint64,
    amount Double,
    PRIMARY KEY (order_id)
);

INSERT INTO orders (order_id, customer_id, amount) VALUES
    (1001, 10, 999.99),
    (1002, 11, 1499.00),
    (1003, 12, 499.50);

SELECT *
FROM orders
WHERE order_id = 1001;
```

{% endcut %}

{% cut "Good example" %}

```yql
CREATE TABLE orders (
    order_hash Uint64,
    order_id Uint64,
    customer_id Uint64,
    amount Double,
    PRIMARY KEY (order_hash, order_id)
);

INSERT INTO orders (order_hash, order_id, customer_id, amount) VALUES
    (Digest::NumericHash(1001), 1001, 10, 999.99),
    (Digest::NumericHash(1002), 1002, 11, 1499.00),
    (Digest::NumericHash(1003), 1003, 12, 499.50);

SELECT *
FROM orders
WHERE order_hash = Digest::NumericHash(1001)
    AND order_id = 1001;
```

{% endcut %}

Compute `order_hash` on write and on keyed reads—the same rule as for `userhash` in the example above.

#### UUID as a primary key {#uuid-primary-key}

A `Uuid` primary key is a common alternative to monotonically increasing numeric identifiers: it spreads inserts across partitions and does not require a central sequence. The effective outcome depends on how the 128 bits are arranged.

{{ ydb-short-name }} stores `Uuid` values as 16 raw bytes in [Microsoft GUID mixed-endian layout](https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding). Primary keys are compared in this byte order, not as RFC 9562 network-byte-order values and not by the canonical GUID string from `CAST(Uuid AS Text)`. As a result, a standard UUID version 7 generated externally, or by [`Uuid::newV7`](../../yql/reference/udf/list/uuid.md#newv7), does not sort chronologically in a row-oriented table or `ORDER BY` statement, even though the UUIDv7 specification embeds a timestamp. Use [`Uuid::newChrono`](../../yql/reference/udf/list/uuid.md#newchrono) when chronological key sort order in {{ ydb-short-name }} matters.

[`RandomUuid()`](../../yql/reference/builtins/basic.md#random) (UUID version 4) gives uniformly random keys. That avoids hot spots on the last partition, but new keys are scattered across the full key space. Related rows and index entries rarely sit in adjacent ranges, which increases cross-partition work for time-bounded scans and secondary index maintenance, and decreases the efficiency of data caching.

The [`Uuid` module](../../yql/reference/udf/list/uuid.md) provides generators tuned for {{ ydb-short-name }}'s byte-level key order:

* **`Uuid::newSharded`** — default choice for insert-heavy tables. Each key draws a random 10-bit prefix (1024 value buckets) and embeds the current time at second granularity. Writes spread across partitions; rows created at similar times stay relatively close in key space.
* **`Uuid::newChrono`** — chronological ordering in the stored bytes. Prefer when index keys should follow creation time closely.
* **`Uuid::newShardedPrefix` / `Uuid::newChronoPrefix`** — fix the prefix for a batch of rows (for example, all rows in one transaction). Those keys usually map to a single partition, which reduces cross-partition overhead for that operation. Use a fresh random prefix per transaction for overall table balance.
* **`Uuid::newV7` / `Uuid::newV7At`** — standard [RFC 9562 UUID version 7](https://datatracker.ietf.org/doc/html/rfc9562). Use when you need interoperability with external tools or using function [`Uuid::extractTs`](../../yql/reference/udf/list/uuid.md#extract-ts).

{% cut "Example: events table with a sharded UUID key" %}

```yql
CREATE TABLE events (
    id Uuid,
    payload Utf8,
    PRIMARY KEY (id)
);

-- Single-row inserts: random prefix per row
INSERT INTO events (id, payload)
VALUES (Uuid::newSharded(), "page view");

-- Multi-row transaction: shared prefix
$prefix = RandomNumber(1);

INSERT INTO events (id, payload)
SELECT
    Uuid::newShardedPrefix($prefix, TableRow()) AS id,
    payload
FROM AS_TABLE($rows);
```

{% endcut %}

The same principles apply to `Uuid` columns in [global secondary indexes](../../concepts/glossary.md#secondary-index): key layout affects how index partitions are split and how range scans perform.

### Reducing the number of partitions affected by a single query {#decrease-shards}

Let's assume that the main scenario for working with table data is to read all events by a specific `userid`. Then, when you use the `( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )` table schema, each read affects all the partitions of the table. Moreover, each partition is fully scanned, since the rows related to a specific `userid` are located in an order that isn't known in advance. Changing the sequence of `( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )` key components causes all rows related to a specific `userid` to follow each other. This row distribution will be useful for reading data by `userid` and will reduce load.

## NULL value in a key column {#key-null}

In {{ ydb-short-name }}, all columns, including key ones, may contain a NULL value. Using NULL as values in key columns isn't recommended. According to the SQL standard (ISO/IEC&nbsp;9075), you can't compare NULL with other values. Therefore, the use of concise SQL statements with simple comparison operators may lead, for example, to skipping rows containing NULL during filtering.

## Row size limit {#limit-string}

To achieve high performance, we don't recommend writing rows larger than 8 MB and key columns larger than 2 KB to the DB.
