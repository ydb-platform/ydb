# Line storage policies

Existing `TRawLineFrontend<T>`, `TOnChangeLineFrontend<T>` and
`TGroupLineFrontend<TDescriptor>` keep their fixed-size format by default.
An optional second template argument selects compressed storage:

```cpp
using TCounter = TRawLineFrontend<ui64,
    TCompressedLineStorage<100'000,
        TIntegerEncoding<ELineValueEncoding::UnsignedDelta>>>;

using TCpu = TOnChangeLineFrontend<double,
    TCompressedLineStorage<100'000, TDecimalEncoding<2>>>;

// Descriptor fields are double CPU followed by ui64 events_total.
using TLoad = TGroupLineFrontend<TLoadDescriptor,
    TCompressedLineStorage<100'000,
        TDecimalEncoding<2>,
        TIntegerEncoding<ELineValueEncoding::UnsignedDelta>>>;
```

Creation and `Append` calls are unchanged. Typed readers return the original
producer types, with decimal values rounded to the configured precision.
A single value codec can be used for all fields in a homogeneous static group.
Mixed groups must specify one codec per descriptor field, in schema order.

The timestamp step is a compile-time number of microseconds. Zero preserves
exact cycle timestamps. A nonzero step quantizes absolute timestamps down to
a cycle grid; each error is less than one step and does not accumulate between
records. The step is converted once using the process clock frequency.
It does not change the collection frequency.

The first timestamp in each chunk is an absolute 62-bit timestamp stored in
8 bytes. Later timestamps use deltas in step units: 1/2/4 bytes carry
6/14/30 bits. Backward timestamps and larger differences use an absolute
8-byte timestamp. Negative timestamps or timestamps exceeding 62 bits fail
without publishing a record.

Each field starts with an absolute value in every chunk. Subsequent integer
values can use unsigned deltas (monotonic counters) or ZigZag signed deltas
(gauges). Decimal values use scaled signed integers; their scale and delta
policy are fixed at compile time. Large deltas, counter resets and absolute
mode use a tag byte plus 8 value bytes, preserving the complete 64-bit range.
Non-finite or out-of-range decimal inputs fail before writing anything.
`on_change` compares the encoded, rounded value and retains its existing
range-boundary interpolation semantics.

Chunks have a versioned header and a writer-only cache of the last encoded
values. Readers reconstruct values from the captured record prefix and never
read this mutable cache. Every chunk is independently decodable, including
after older chunks are evicted. A failed write leaves the payload and cache
unchanged. Group records are always published atomically as complete samples.

Dynamic schemas and partial group updates are separate from these storage
policies; the existing static group still requires all its fields on Append.
Harmonizer groups opt into a 100 ms timestamp step and two decimal places.
Quota and state share partial on-change storage. Other producers retain their
default storage unless they explicitly select a policy.

## Runtime participant groups

`TDynamicGroupLine::Create(system, name, fields, mode)` registers an immutable
runtime schema of 1–128 participants. Each field owns its name, labels and
value kind (unsigned/signed integer, decimal(2), or boolean). The schema is
retained by snapshots, independently of writer and backend lifetime.

`Append(span<TLineNumericValue>)` supplies the complete current state in schema
order. Modes are `All` (periodic full samples), `OnChangeAll` (full samples only
on changes), and `OnChangePartial` (only changed id/value pairs). Both on-change
modes compare encoded values. Failed writes never advance the writer cache.
Append is allocation-free; the schema must fit the configured chunk size.

Every chunk begins with all participant values and an absolute timestamp.
Later records use timestamp/value deltas; partial records store ordered ids.
Snapshot readers ignore the mutable header cache, reconstruct the complete
state and preserve on-change range boundaries. Runtime schemas export through
`ReadNumericRange`; static typed groups keep their existing typed read API.

Harmonizer production lines: `harmonizer.pools.cpu`,
`harmonizer.pools.state`, `harmonizer.global`.
Pool labels belong to participants; pools share physical storage. Harmonizer state uses partial on-change records.
