# Uuid

The `Uuid` module provides primary key generators for {{ ydb-short-name }} tables. Unlike [`RandomUuid()`](../../builtins/basic.md#random), which returns a uniformly random [UUID version 4](https://datatracker.ietf.org/doc/html/rfc4122#section-4.4), these functions assemble 128-bit values with a deliberate bit layout so that key order and partition spread suit {{ ydb-short-name }}'s partitioning.

All functions return a value of type `Uuid` in {{ ydb-short-name }}'s internal 16-byte representation (Microsoft GUID / mixed-endian layout). This is the same byte order used when comparing primary keys. Generators that target key-friendly layout write bytes directly in this format instead of using the RFC network-byte-order representation.

When you cast a generated `Uuid` to `Text`, you get a canonical GUID text representation. For key-friendly generators (`newChrono`, `newSharded`, and their `Prefix` variants), the string produced by this conversion does not reflect how the timestamp and prefix are embedded in the stored bytes.

For general recommendations on using `Uuid` as a primary key, see [UUID as a primary key](../../../dev/primary-key/row-oriented.md#uuid-primary-key).

## Key-friendly generators {#key-friendly}

These functions produce UUID version 8 values (implementation-specific per [RFC 9562](https://datatracker.ietf.org/doc/html/rfc9562)) optimized for {{ ydb-short-name }} key sorting. Sort order is defined by comparing the stored 16 bytes, not by the canonical GUID string.

### `Uuid::newChrono` {#newchrono}

Generates a key whose sort order follows creation time at millisecond precision, then random suffix bits. Rows inserted close together in time tend to land in adjacent key ranges, which improves locality for time-bounded scans and index maintenance compared to fully random keys.

Optional dependency arguments work like [`RandomUuid()`](../../builtins/basic.md#random): they control when the function is evaluated per row, not the generated value.

* `Uuid::newChrono([T1[, T2, ...]]) -> Uuid`

### `Uuid::newChronoPrefix` {#newchronoprefix}

Same chronological layout as `newChrono`, but the high bits of the key (default prefix size is 10 bits) are taken from the first argument instead of being random. Use this to pin a shared prefix for several rows in one transaction or batch so they map to a single partition.

The first argument is either:

* `Uint64` — the low 10 bits are used as the prefix;
* `Uuid` — the high 10 bits of the source value's MSB are used as the prefix.

Optional dependency arguments may follow.

* `Uuid::newChronoPrefix(Uint64{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`
* `Uuid::newChronoPrefix(Uuid{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`

### `Uuid::newSharded` {#newsharded}

Generates a key that balances partition spread and time locality. Each call leaves the high bits (prefix) random (by default, 2<sup>10</sup> ≈ 1024 value buckets), embeds the current Unix time at second granularity in the following bit field, and fills the remaining bits with randomness. This spreads write load across partitions while keeping rows created at similar times relatively close in key space.

Optional dependency arguments work like [`RandomUuid()`](../../builtins/basic.md#random).

* `Uuid::newSharded([T1[, T2, ...]]) -> Uuid`

### `Uuid::newShardedPrefix` {#newshardedprefix}

Same sharded layout as `newSharded`, but the prefix is fixed from the first argument. Rows that share a prefix usually belong to one partition, which simplifies multi-row transactions; the embedded timestamp still groups nearby writes inside the prefix bucket.

The first argument is either `Uint64` or `Uuid` (same rules as `newChronoPrefix`). Optional dependency arguments may follow.

* `Uuid::newShardedPrefix(Uint64{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`
* `Uuid::newShardedPrefix(Uuid{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`

### Choosing between `Uuid::newChrono` and `Uuid::newSharded` {#key-friendly-choice}

| Goal | Function |
| --- | --- |
| Maximum chronological order in the primary key; time-range scans by key | `Uuid::newChrono` |
| Even partition spread for single-row inserts with some time locality; efficient data caching | `Uuid::newSharded` |
| Reduce the number of partitions affected when writing many rows in one transaction | `Uuid::newChronoPrefix` or `Uuid::newShardedPrefix` with the same prefix |
| Unstructured random IDs without sort semantics | [`RandomUuid()`](../../builtins/basic.md#random) |

## RFC UUID version 7 {#rfc-v7}

`Uuid::newV7` and `Uuid::newV7At` generate standard [RFC 9562 UUID version 7](https://datatracker.ietf.org/doc/html/rfc9562) values: a 48-bit Unix timestamp in milliseconds in the leading bits, then a random suffix. The result is stored in {{ ydb-short-name }}'s internal `Uuid` representation. Use these when you need interoperability with RFC v7 tools or to extract the embedded timestamp.

Because UUIDv7 follows the RFC byte layout, its sort order in {{ ydb-short-name }} does not match chronological order. For table performance in {{ ydb-short-name }}, prefer `Uuid::newChrono` or `Uuid::newSharded` in primary keys.

### `Uuid::newV7` {#newv7}

Generates a v7 UUID from the current timestamp.

* `Uuid::newV7([T1[, T2, ...]]) -> Uuid`

### `Uuid::newV7At` {#newv7at}

Generates a v7 UUID from an explicit timestamp. Accepts `Timestamp` or `Timestamp64`.

* `Uuid::newV7At(Timestamp{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`
* `Uuid::newV7At(Timestamp64{Flags:AutoMap}[, T1[, T2, ...]]) -> Uuid`

### `Uuid::extractTs` and `Uuid::extractTs64` {#extract-ts}

Extract the timestamp embedded in a v7 UUID. Returns `NULL` if the argument is not UUIDv7 (for example, a key from `newChrono` or `newSharded`).

* `Uuid::extractTs(Uuid{Flags:AutoMap}) -> Timestamp?`
* `Uuid::extractTs64(Uuid{Flags:AutoMap}) -> Timestamp64?`

## Examples {#examples}

Single-row insert with sharded keys (default: random prefix per row):

```yql
INSERT INTO events (id, payload)
SELECT
    Uuid::newSharded(TableRow()) AS id,
    payload
FROM AS_TABLE($rows);
```

Multi-row transaction with a shared prefix (rows typically land in one partition):

```yql
$prefix = RandomNumber(1);

INSERT INTO events (id, payload)
SELECT
    Uuid::newShardedPrefix($prefix, TableRow()) AS id,
    payload
FROM AS_TABLE($rows);
```

Chronological primary key:

```yql
INSERT INTO audit_log (id, message)
VALUES (Uuid::newChrono(), "user signed in");
```
