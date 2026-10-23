# Uuid

The `Uuid` module provides primary key generators for {{ ydb-short-name }} tables. Unlike [`RandomUuid()`](../../builtins/basic.md#random), which returns a uniformly random [UUID version 4](https://datatracker.ietf.org/doc/html/rfc4122#section-4.4), these functions assemble 128-bit values with a deliberate bit layout so that key order and partition spread suit {{ ydb-short-name }}'s partitioning.

All functions return a value of type `Uuid` in {{ ydb-short-name }}'s internal 16-byte representation (Microsoft GUID / mixed-endian layout). This is the same byte order used when comparing primary keys. Generators that target key-friendly layout write bytes directly in this format instead of using the RFC network-byte-order representation.

When you cast a generated `Uuid` to `Text`, you get a canonical GUID text representation. For key-friendly generators (`newV8RowKey`, `newV8ColumnKey`, and `newV8RowGroup`), the string produced by this conversion does not reflect how the timestamp and prefix are embedded in the stored bytes.

Optional dependency arguments `[T1, ...]` work like [`RandomUuid()`](../../builtins/basic.md#random): they control when the function is evaluated per row, not the generated value.

For general recommendations on using `Uuid` as a primary key, see [UUID as a primary key](../../../../dev/primary-key/row-oriented.md#uuid-primary-key).

## Key-friendly generators {#key-friendly}

These functions produce UUID version 8 values (implementation-specific per [RFC 9562](https://datatracker.ietf.org/doc/html/rfc9562)) optimized for {{ ydb-short-name }} key sorting. Sort order is defined by comparing the stored 16 bytes, not by the canonical GUID string.

### `Uuid::newV8RowKey` {#newv8rowkey}

Generates a key for a [row-oriented table](../../../../concepts/datamodel/table.md#row-oriented-tables). Each call leaves the high bits (12-bit prefix, 2<sup>12</sup> ≈ 4096 value buckets) random, embeds the current Unix time at second granularity in the following bit field (modulo 2<sup>31</sup>), and fills the remaining bits with randomness. This spreads write load across partitions while keeping rows created at similar times relatively close in key space within each prefix bucket.

* `Uuid::newV8RowKey([T1[, T2, ...]]) -> Uuid`

### `Uuid::newV8ColumnKey` {#newv8columnkey}

Generates a key for a [column-oriented table](../../../../concepts/datamodel/table.md#column-oriented-tables). Sort order follows creation time at second granularity (modulo 2<sup>31</sup>), then random suffix bits. Rows inserted close together in time tend to land in adjacent key ranges. Column-oriented tables use hash partitioning, so a separate partition prefix is not needed.

* `Uuid::newV8ColumnKey([T1[, T2, ...]]) -> Uuid`

### `Uuid::newV8RowGroup` {#newv8rowgroup}

Returns a list of `COUNT` keys in the same row-table layout as `newV8RowKey`, all sharing a common prefix `PFX`. Rows with a shared prefix usually map to a single partition — useful for multi-row transactions and batch inserts.

The first argument is either:

* `Uint64` — the low 12 bits are used as the prefix;
* `Uuid` — the high 12 bits of the source value's MSB (YDB internal layout) are used as the prefix.

`COUNT` is `Uint64`. The maximum value is 1 000 000. Optional dependency arguments may follow.

* `Uuid::newV8RowGroup(Uint64, Uint64[, T1[, T2, ...]]) -> List<Uuid>`
* `Uuid::newV8RowGroup(Uuid, Uint64[, T1[, T2, ...]]) -> List<Uuid>`

### Choosing a key-friendly generator {#key-friendly-choice}

| Goal | Function |
| --- | --- |
| Even partition spread for single-row inserts into a row-oriented table, with some time locality and efficient data caching | `Uuid::newV8RowKey` |
| Chronological order in the primary key of a column-oriented table; time-range locality by key | `Uuid::newV8ColumnKey` |
| Reduce the number of partitions affected when writing many rows in one transaction (row-oriented table) | `Uuid::newV8RowGroup` with the same prefix |
| Unstructured random IDs without sort semantics | `Uuid::newV4` or [`RandomUuid()`](../../builtins/basic.md#random) |

## Standard UUID variants {#standard}

### `Uuid::newV4` {#newv4}

Uniformly random UUID version 4 (analogue of [`RandomUuid()`](../../builtins/basic.md#random) in the `Uuid` module). No sort or partition-spread semantics — for unstructured identifiers.

* `Uuid::newV4([T1[, T2, ...]]) -> Uuid`

### RFC UUID version 7 {#rfc-v7}

`Uuid::newV7` and `Uuid::newV7At` generate standard [RFC 9562 UUID version 7](https://datatracker.ietf.org/doc/html/rfc9562) values: a 48-bit Unix timestamp in milliseconds in the leading bits, then a random suffix. The result is stored in {{ ydb-short-name }}'s internal `Uuid` representation. Use these when you need interoperability with RFC v7 tools or to extract the embedded timestamp.

Because UUIDv7 follows the RFC byte layout, its sort order in {{ ydb-short-name }} does not match chronological order in a row-oriented table or in an `ORDER BY` clause. For table performance in {{ ydb-short-name }}, prefer `Uuid::newV8RowKey` or `Uuid::newV8ColumnKey` in primary keys.

### `Uuid::newV7` {#newv7}

Generates a v7 UUID from the current timestamp.

* `Uuid::newV7([T1[, T2, ...]]) -> Uuid`

### `Uuid::newV7At` {#newv7at}

Generates a v7 UUID from an explicit timestamp. Accepts `Timestamp` or `Timestamp64`.

* `Uuid::newV7At(Timestamp{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`
* `Uuid::newV7At(Timestamp64{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`

{% note info %}

The source timestamp passed to `Uuid::newV7At` may be specified with microsecond precision. UUIDv7 values encode the timestamp with millisecond precision. When generating a UUIDv7 value, the timestamp is truncated to millisecond precision.

{% endnote %}

### `Uuid::extractTs` and `Uuid::extractTs64` {#extract-ts}

Extract the timestamp embedded in a v7 UUID. Returns `NULL` if the argument is not UUIDv7 (for example, a key from `newV8RowKey` or `newV8ColumnKey`).

* `Uuid::extractTs(Uuid{Flags:AutoMap}) -> Timestamp?`
* `Uuid::extractTs64(Uuid{Flags:AutoMap}) -> Timestamp64?`

## Examples {#examples}

Single-row insert with row-table keys (default: random prefix per row):

```yql
INSERT INTO events (id, payload)
SELECT
    Uuid::newV8RowKey(TableRow()) AS id,
    payload
FROM AS_TABLE($rows);
```

Multi-row batch with a shared prefix (rows typically land in one partition):

```yql
$prefix = RandomNumber(1);
$ids = Uuid::newV8RowGroup($prefix, 3ul);

INSERT INTO events (id, payload)
VALUES
    (Unwrap($ids[0]), "a"),
    (Unwrap($ids[1]), "b"),
    (Unwrap($ids[2]), "c");
```

Chronological primary key for a column-oriented table:

```yql
INSERT INTO audit_log (id, message)
VALUES (Uuid::newV8ColumnKey(), "user signed in");
```
