---
name: ydb-inmemory-metrics
description: Use when adding producers or reading snapshots, JSON histories, and embedded charts for YDB in-memory metrics.
---

# Add and read in-memory metrics

Paths below are relative to the repository root. This skill covers the in-memory
registry, not dynamic counters exported to Solomon or Prometheus.

## Source map

| Contract | Source |
|---|---|
| Producer interface and lookup | `ydb/library/actors/core/subsystems/metric_system.h` |
| Snapshot requests and reply event | `ydb/library/actors/core/subsystems/inmemory_metrics.h` |
| Ownership, pending registration, retention | `ydb/library/actors/metrics/README.md` |
| Labels and registry configuration | `ydb/library/actors/metrics/line_types.h` |
| Numeric variants and frontend reader callbacks | `ydb/library/actors/metrics/line_read.h` |
| Raw, on-change, static and dynamic groups | `ydb/library/actors/metrics/lines/` |
| Compression and group update modes | `ydb/library/actors/metrics/lines/README.md` |
| Allowlist and registry setup | `ydb/core/driver_lib/run/kikimr_services_initializers.cpp` |
| JSON request parameters | `ydb/core/subsystems/inmemory_metrics_monitoring/subsystem.cpp` |
| JSON history serialization | `ydb/core/subsystems/inmemory_metrics_monitoring/viewer.cpp` |
| Shared chart and client API | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/README.md` |

## Optimize storage

Before selecting or changing storage, follow [references/storage-cost.md](references/storage-cost.md).
Compare the current line variants, estimate payload and physical allocation,
measure representative deltas/change rates, and report precision, retention,
and CPU tradeoffs. Do not claim savings from steady-state bytes alone.

## Add a producer

1. Read the component's nearest `AGENTS.md`, then the ownership and storage
   READMEs above. Inspect an existing producer with the same update pattern.
2. Choose a stable metric name and bounded labels. Check `AllowedMetricPrefixes`
   in the target setup; add the owning prefix if needed. Do not create another
   registry inside a producer. Avoid request IDs and other unbounded labels.
3. Obtain `NActors::IMetricSystem` through `GetMetricSystem(actorSystem)` (or the
   actor-context overload). Handle a missing interface. Keep each move-only line
   as producer state; do not recreate it on every sample. Serialize its writes
   and stop the producer before the metric system is destroyed.
4. Choose `TRawLineFrontend<T>` for periodic samples,
   `TOnChangeLineFrontend<T>` for gauges or flags that change infrequently,
   `TGroupLineFrontend<Descriptor>` for a compile-time schema, or
   `TDynamicGroupLine` for a runtime participant count. Group fields only when
   they share a writer and collection lifecycle. Distinguish a stored counter
   from a rate; compute rates using the actual interval and handle resets.
5. Select compressed storage explicitly for typed frontends when its precision
   fits the metric. See `lines/README.md` for integer delta and decimal policies.
   A timestamp step quantizes time; it does not schedule sampling. Dynamic groups
   currently use a fixed 100 ms step and decimal precision of two places.
6. Check `Append`'s result. Registration is asynchronous, so initial writes may
   fail before readiness; chunk exhaustion also rejects writes. Never spin or
   block waiting for a chunk. Follow the component's bounded drop/retry policy.

Example setup inside a producer, with a member `TLine<TCpu> Cpu`:

```cpp
using TCpu = NActors::TOnChangeLineFrontend<double,
    NActors::TCompressedLineStorage<100'000, NActors::TDecimalEncoding<2>>>;

if (auto* metrics = NActors::GetMetricSystem(actorSystem)) {
    const std::array<NActors::TLabel, 1> labels = {{{"pool", "User"}}};
    Cpu = metrics->CreateLine<TCpu>("example.cpu_cores", labels);
}
// Later, from the same writer; false means this sample was not accepted.
const bool accepted = Cpu.Append(cpuCores);
```

Include `core/subsystems/metric_system.h` and the selected frontend/storage
headers under `ydb/library/actors/`. Use the consumer's existing build dependencies
or add the owning library to its `PEERDIR`.

For fields discovered at startup, construct the schema once:

```cpp
TVector<NActors::TDynamicGroupField> fields;
for (const auto& pool : pools) {
    fields.push_back({"example.cpu_cores", {{"pool", pool.Name}},
        NActors::EGroupValueType::Decimal});
}
// Validate a nonempty schema of at most TDynamicGroupSchema::MaxFields first.
auto line = NActors::TDynamicGroupLine::Create(metrics, "example.pools",
    std::move(fields), NActors::EGroupUpdateMode::OnChangePartial);
// Supply every current field in schema order, even in partial update mode.
const bool accepted = line.Append(values);
```

Include `ydb/library/actors/metrics/lines/dynamic_group_line.h`. Values are a
`std::span<const TLineNumericValue>` with alternatives matching field kinds.
The participant count is dynamic at registration; changing the immutable schema
requires a new line generation. Partial mode stores changed fields, not a
partial input vector. Preserve field labels when displaying each logical series.

## Read snapshots and histories

1. Readers use the concrete registry: `GetInMemoryMetrics(actorSystem)`.
   `IMetricSystem` exposes writing, not export. Handle a missing registry.
2. Request `RequestSnapshot(recipient, cookie)` for a catalog or
   `RequestLineSnapshot(recipient, lineId, cookie)` for selected history.
   Check admission, correlate the `TEvInMemoryMetricsSnapshot` reply by cookie,
   and set a timeout: shutdown can cancel an accepted request without replying.
3. Read borrowed metadata only inside `Snapshot.Read`. Copy anything needed
   after the callback. Release owning snapshots promptly; pins delay chunk reuse.
4. Prefer `line.Meta.Frontend->ReadNumericRange` for mixed frontend types.
   Preserve field order, field labels, and exact integer values. Do not
   cast compressed chunk payloads to raw records. For typed reading, use the
   selected frontend's public reader and follow its tests.

Example inside a snapshot reply handler; `begin` and `end` are `TInstant`:

```cpp
reply->Get()->Snapshot.Read([&](const NActors::TSnapshotView& view) {
    view.ForEachLine([&](const NActors::TLineSnapshot& line) {
        if (line.Meta.Frontend && line.Meta.Frontend->ReadNumericRange) {
            line.Meta.Frontend->ReadNumericRange(line, begin, end, nullptr,
                [](void*, TInstant timestamp,
                        std::span<const NActors::TLineNumericValue> values) {
                    // Consume or copy values here; do not retain this span.
                });
        }
    });
});
```

For browser consumers, use the existing same-origin endpoint:
`/actors/metrics?format=json` returns the catalog;
`/actors/metrics?format=json&line=<id>&seconds=300` returns selected history.
Use a catalog ID, not a metric name, for `line`. The current range is 1..3600
seconds. Handle HTTP errors, truncation, and missing retained history.
Use the shared `createInMemoryMetricsClient` to select metric fields and labels;
a physical group can contain multiple matching series. Follow the chart README
for `queryMany`, `createMetricChart`, cursor groups, and `destroy()`.
Do not add page reloads or modify the separately generated monitoring bundle.

## Validation

1. Check the affected producer tests plus
   `ydb/library/actors/core/ut/metric_system_ut.cpp` for interface integration.
   Registry snapshot, ownership, and eviction cases are in
   `ydb/library/actors/core/ut/inmemory_metrics_ut.cpp`; line codec cases are in
   `ydb/library/actors/metrics/ut/`. Inspect suite names before selecting filters.
2. Cover disabled registration, successful writes, capacity rejection, labels,
   exact integer values, rounding, and reset/on-change behavior as applicable.
   For groups, verify multiple participants and reading after chunk rollover.
3. Follow active build instructions; use `ssh_ya` for requested remote builds.
   Do not compile broad targets merely to validate documentation. For browser
   changes, check retained values, multiple matching group fields, time ranges,
   and stale-request cancellation through the running viewer.
