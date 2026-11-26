#include <ydb/core/tx/schemeshard/schemeshard_path_describer.h>

#include <benchmark/benchmark.h>

#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

// ---------------------------------------------------------------------------
// Helpers to fabricate a table + shard map without a live SchemeShard.
// ---------------------------------------------------------------------------

// EndOfRange key for a uniform Int64 range: partition i covers [2(i-1), 2i);
// the last shard's empty range encodes +inf (matches the real SchemeShard
// convention). The stride of 2 leaves an interior integer key in every range
// so a split benchmark can split it genuinely.
TString MakeEndOfRangeKey(ui64 i, ui64 n) {
    if (i == n) {
        return TSerializedCellVec::Serialize({});
    }
    return TSerializedCellVec::Serialize({TCell::Make(2 * i)});
}

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        // Ranges [0,1), [1,2), ..., [n-1, +inf)
        v.emplace_back(TShardIdx(ownerId, i), MakeEndOfRangeKey(i + 1, n));
    }
    return v;
}

// Populate a TTableInfo with a realistic column set, partitioning and
// fabricated per-shard TPartitionStats.
// state.range(0) = partition count, state.range(1) = value column count.
TTableInfo::TPtr MakeBenchTable(::benchmark::State& state) {
    const ui32 numPartitions = static_cast<ui32>(state.range(0));
    const ui32 numValueCols = static_cast<ui32>(state.range(1));

    auto info = MakeIntrusive<TTableInfo>();

    // Key column (id=1).
    info->Columns.emplace(1, TTableInfo::TColumn("key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", true));
    info->Columns[1].KeyOrder = 0;
    info->KeyColumnIds = {1};

    // Value columns (id=2..).
    for (ui32 c = 0; c < numValueCols; ++c) {
        const TString colName = TStringBuilder() << "col_" << c;
        info->Columns.emplace(
            2 + c,
            TTableInfo::TColumn(colName, 2 + c, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", false)
        );
    }

    info->SetPartitioning(MakeShards(numPartitions));

    // Fabricated per-shard stats. UpdateShardStats is the public API; it also
    // maintains the aggregated part seen by describes.
    for (ui32 i = 0; i < numPartitions; ++i) {
        TPartitionStats s;
        s.SeqNo = TMessageSeqNo{1, 0};
        s.RowCount = 1'000'000 + i;
        s.DataSize = 64'000'000 + 37 * i;
        s.IndexSize = 10'000'000 + i;
        s.ByKeyFilterSize = 1'000'000 + i;
        s.PartCount = 1;
        s.LastAccessTime = TInstant::Seconds(1'700'000'000ull + i);
        s.LastUpdateTime = TInstant::Seconds(1'700'000'100ull + i);
        s.StoragePoolsStats["hdd"] = {.DataSize = s.DataSize, .IndexSize = s.IndexSize};
        s.StoragePoolsStats["nvme"] = {.DataSize = s.DataSize / 2, .IndexSize = s.IndexSize / 2};
        TDiskSpaceUsageDelta delta;
        info->UpdateShardStats(&delta, TShardIdx(1, i), s, TInstant::Zero());
    }

    return info;
}

THashMap<TShardIdx, TShardInfo> MakeShardInfos(ui32 n, ui64 ownerId = 1) {
    THashMap<TShardIdx, TShardInfo> shards;
    shards.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TShardInfo shard;
        shard.TabletID = TTabletId(1000 + i);
        shards.emplace(TShardIdx(ownerId, i), shard);
    }
    return shards;
}

void ClearDescribeCaches(TTableInfo& info) {
    info.PreserializedTablePartitionsElems.clear();
    info.PreserializedTablePartitionsNoKeysElems.clear();
    info.PreserializedTableSplitBoundariesElems.clear();
    info.PreserializedTablePartitionStatsElems.clear();
}

TTableDescribeOptions AllDescribeOptions() {
    return TTableDescribeOptions{
        .CreateFinished = true,
        .ReturnBoundaries = true,
        .ReturnPartitioning = true,
        .ReturnPartitionStats = true,
        .ReturnRangeKey = true,
    };
}

// ---------------------------------------------------------------------------
// Benchmarks.
// ---------------------------------------------------------------------------

// The dominant uncached loop of a describe with returnPartitionStats:
// per-shard stats/metrics fill + serialization (no cache exists for this today).
static void BenchFillPartitionStats(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnPartitionStats = true,
    };
    for (auto _ : state) {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.Record.SerializeToString(&out);
        benchmark::DoNotOptimize(out);
    }
}

// FillTablePartitions into scratch proto + serialization (cold: caches cleared
// on each iteration) vs the warm cached path.
static void BenchSerializeTablePartitions(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnPartitioning = true,
        .ReturnRangeKey = true,
    };
    for (auto _ : state) {
        ClearDescribeCaches(*info); // cold path
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
    }
}

static void BenchSerializeTablePartitionsCached(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnPartitioning = true,
        .ReturnRangeKey = true,
    };
    // Warm the caches so the timed loop measures only the warm path
    {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder warmup("/Root/table", TPathId(1, 10));
        BuildTableDescribe(warmup, *info, shards, opts);
    }
    for (auto _ : state) {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
    }
}

// FillTableBoundaries + serialization (cold: per-boundary cell parse and key
// tuple conversion; the result is cached on TTableInfo today).
static void BenchBuildTableBoundaries(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnBoundaries = true,
    };
    for (auto _ : state) {
        ClearDescribeCaches(*info); // cold path
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
    }
}

// End-to-end serialized-parts build with all table sections requested,
// cold caches (the cost paid between split steps, during backup listing etc).
static void BenchBuildTableDescribe(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts = AllDescribeOptions();
    for (auto _ : state) {
        ClearDescribeCaches(*info); // cold path
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.Record.SerializeToString(&out);
        benchmark::DoNotOptimize(result.PreSerializedData);
        benchmark::DoNotOptimize(out);
    }
}

static void BenchBuildTableDescribeCached(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts = AllDescribeOptions();
    // Warm the caches so the timed loop measures only the warm path
    {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder warmup("/Root/table", TPathId(1, 10));
        BuildTableDescribe(warmup, *info, shards, opts);
    }
    for (auto _ : state) {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
    }
}

// One split step of a split campaign: ApplySplitMerge of the first partition
// [0,2) into [0,1) + [1,2) + the subsequent warm describe build. The fixture
// is restored between iterations (outside the timed region) so every sample
// measures the same n -> n+1 step with valid, ordered ranges.
static void BenchSplitStep(::benchmark::State& state) {
    const ui32 n = static_cast<ui32>(state.range(0));
    auto info = MakeBenchTable(state);
    // The two split outputs are registered once and reused by every iteration.
    auto shards = MakeShardInfos(n);
    const TShardIdx auxLo(1, n), auxHi(1, n + 1);
    for (const TShardIdx aux : {auxLo, auxHi}) {
        TShardInfo shard;
        shard.TabletID = TTabletId(1000 + ui64(aux.second));
        shards.emplace(aux, shard);
    }

    // The original upper bound of partition 0 (empty = +inf when n == 1);
    // reused by the upper split child and the restored parent
    const TString upperBound = info->GetPartitions()[0]->EndOfRange;

    // Seed stats for the shard the restore below erases (same values as
    // MakeBenchTable, see below).
    const auto SeedStats = [&](TShardIdx idx) {
        TPartitionStats s;
        s.SeqNo = TMessageSeqNo{1, 0};
        s.RowCount = 1'000'000;
        s.DataSize = 64'000'000;
        s.IndexSize = 10'000'000;
        s.ByKeyFilterSize = 1'000'000;
        s.PartCount = 1;
        TDiskSpaceUsageDelta delta;
        info->UpdateShardStats(&delta, idx, s, TInstant::Zero());
    };

    {
        // Warm the caches so the timed loop measures only the warm path
        TEvSchemeShard::TEvDescribeSchemeResultBuilder warmup("/Root/table", TPathId(1, 10));
        BuildTableDescribe(warmup, *info, shards, AllDescribeOptions());
    }

    for (auto _ : state) {
        // Split partition 0 at the interior key 1; the upper child and the
        // restored parent inherit the original upper bound (empty = +inf for
        // the one-partition argument)
        TVector<TTableShardInfo> dst = {
            TTableShardInfo(auxLo, TSerializedCellVec::Serialize({TCell::Make(ui64(1))})),
            TTableShardInfo(auxHi, upperBound),
        };
        info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 0)}, 0, TInstant::Now(), false, false);

        // ApplySplitMerge splices the element-wise caches itself: only the
        // touched positions are re-serialized lazily by this describe.
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, AllDescribeOptions());
        benchmark::DoNotOptimize(result.PreSerializedData);

        // Restore the pristine n-partition fixture outside the timed region.
        state.PauseTiming();
        TVector<TTableShardInfo> restored = {
            TTableShardInfo(TShardIdx(1, 0), upperBound),
        };
        info->ApplySplitMerge(std::move(restored), {auxLo, auxHi}, 0, TInstant::Now(), false, false);
        // ApplySplitMerge re-seeded shard (1,0) with zeroed stats: restore the
        // original values so every iteration starts from the same fixture.
        SeedStats(TShardIdx(1, 0));
        state.ResumeTiming();
    }
}

// ---------------------------------------------------------------------------
// Receiver-side (scheme-cache subscriber) cost of the two wire forms:
// parsing the canonical assembly produced by BuildTableDescribe (all sections
// in one PathDescription wrapper spliced from the cached wire entries) vs a
// conventionally serialized single-fill description — byte-budget and parse
// time should now match exactly.
// ---------------------------------------------------------------------------

// The exact wire form produced today: the canonical assembly (framed cached
// elements inside a single PathDescription wrapper) in PreSerializedData,
// followed by the serialized Record (TEventPreSerializedPB payload order).
static void BenchParseDescribeCanonical(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    const ui32 n = static_cast<ui32>(state.range(0));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnPartitioning = true,
        .ReturnRangeKey = true,
    };
    TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
    BuildTableDescribe(result, *info, shards, opts);
    const TString wire = result.PreSerializedData + result.Record.SerializeAsString();
    state.counters["bytes"] = wire.size();
    for (auto _ : state) {
        NKikimrScheme::TEvDescribeSchemeResult parsed;
        Y_ABORT_UNLESS(parsed.ParseFromString(wire));
        Y_ABORT_UNLESS(static_cast<ui32>(parsed.GetPathDescription().TablePartitionsSize()) == n);
        benchmark::DoNotOptimize(parsed);
    }
}

// Reference form: the same description serialized conventionally — all
// elements inside one TEvDescribeSchemeResult wrapper (single wire blob).
static void BenchParseDescribeSingle(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    const ui32 n = static_cast<ui32>(state.range(0));
    TTableDescribeOptions opts{
        .CreateFinished = true,
        .ReturnPartitioning = true,
        .ReturnRangeKey = true,
    };
    TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
    BuildTableDescribe(result, *info, shards, opts);

    // The old wire form: the same Record (path, path id, aggregated
    // stats/metrics) with the partitions filled inside it, serialized as one
    // message
    NKikimrScheme::TEvDescribeSchemeResult msg = result.Record;
    FillTablePartitions(msg.MutablePathDescription()->MutableTablePartitions(), *info, shards, true, false);
    TString wire;
    Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&wire);
    state.counters["bytes"] = wire.size();

    // The two forms must parse to the same content before we time them
    NKikimrScheme::TEvDescribeSchemeResult a, b;
    Y_ABORT_UNLESS(a.ParseFromString(result.PreSerializedData + result.Record.SerializeAsString()));
    Y_ABORT_UNLESS(b.ParseFromString(wire));
    Y_ABORT_UNLESS(a.SerializeAsString() == b.SerializeAsString());

    for (auto _ : state) {
        NKikimrScheme::TEvDescribeSchemeResult parsed;
        Y_ABORT_UNLESS(parsed.ParseFromString(wire));
        Y_ABORT_UNLESS(static_cast<ui32>(parsed.GetPathDescription().TablePartitionsSize()) == n);
        benchmark::DoNotOptimize(parsed);
    }
}

// Crude memory measurement like ut_pq_configure_bench: allocator-stat deltas
// (meaningful with the tcmalloc-backed IAllocState, the default in binaries).
// cacheAllocBytes / legacyCacheAllocBytes = what one cold build leaves behind
// for the new element caches vs the legacy three-blob layout (measured
// strings-only: the old code never retained the proto tree it serialized).
// The warm loop verifies net churn is zero and reports the live response
// footprint (== the all-sections wire size) sampled mid-iteration.
static void BenchCacheMemory(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)));
    TTableDescribeOptions opts = AllDescribeOptions();

    const NKikimr::IAllocState::TState base = NKikimr::TAllocState::Get();
    {
        // Warm both partition variants so the footprint matches the legacy
        // baseline, which retains the with-keys and the no-keys blob alike
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
        auto noKeysOpts = opts;
        noKeysOpts.ReturnRangeKey = false;
        TEvSchemeShard::TEvDescribeSchemeResultBuilder resultNoKeys("/Root/table", TPathId(1, 10));
        BuildTableDescribe(resultNoKeys, *info, shards, noKeysOpts);
        benchmark::DoNotOptimize(resultNoKeys.PreSerializedData);
    }
    const NKikimr::IAllocState::TState afterCold = NKikimr::TAllocState::Get();
    const i64 cacheAllocBytes = i64(afterCold.AllocatedMemory) - i64(base.AllocatedMemory);

    i64 warmChurn = 0, ctorChurn = 0, recordBytes = 0;
    for (auto _ : state) {
        const NKikimr::IAllocState::TState before = NKikimr::TAllocState::Get();
        {
            TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
            const NKikimr::IAllocState::TState afterCtor = NKikimr::TAllocState::Get();
            BuildTableDescribe(result, *info, shards, opts);
            benchmark::DoNotOptimize(result.PreSerializedData);
            ctorChurn += i64(afterCtor.AllocatedMemory) - i64(before.AllocatedMemory);
            // The Record tree's own heap usage estimate: Record.Clear() retains
            // submessage allocations, so an allocator delta across it reads ~0
            recordBytes += result.Record.SpaceUsedLong();
            // The response buffer's allocation (its capacity == the wire
            // size: the reserve is exact)
            const NKikimr::IAllocState::TState beforeResponseClear = NKikimr::TAllocState::Get();
            result.PreSerializedData = TString();
            const NKikimr::IAllocState::TState afterResponseClear = NKikimr::TAllocState::Get();
            ctorChurn += i64(beforeResponseClear.AllocatedMemory) - i64(afterResponseClear.AllocatedMemory);
        }
        const NKikimr::IAllocState::TState afterDtor = NKikimr::TAllocState::Get();
        warmChurn += i64(afterDtor.AllocatedMemory) - i64(before.AllocatedMemory);
    }

    const double iters = state.iterations() ? double(state.iterations()) : 1.0;
    state.counters["cacheAllocBytes"] = double(cacheAllocBytes);
    state.counters["warmNetBytes"] = double(warmChurn) / iters;
    state.counters["warmResponseBytes"] = double(ctorChurn) / iters;
    state.counters["warmRecordBytes"] = double(recordBytes) / iters;
    state.counters["warmIters"] = double(state.iterations());

    // The legacy layout for comparison: three monolithic blobs on TTableInfo
    // (partitions with/without keys, boundaries); the old code kept no stats
    // cache at all - stats were refilled on every describe.
    info->PreserializedTablePartitionsElems.clear();
    info->PreserializedTablePartitionsNoKeysElems.clear();
    info->PreserializedTableSplitBoundariesElems.clear();
    info->PreserializedTablePartitionStatsElems.clear();
    const NKikimr::IAllocState::TState baseLegacy = NKikimr::TAllocState::Get();
    i64 legacyCacheBytes = 0;
    // Only the serialized strings count: the old code never retained the
    // protobuf tree it used to produce them
    TString partitions, partitionsNoKeys, boundaries;
    {
        NKikimrScheme::TEvDescribeSchemeResult msg;
        FillTablePartitions(msg.MutablePathDescription()->MutableTablePartitions(), *info, shards, true, false);
        Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&partitions);
        msg.Clear();
        FillTablePartitions(msg.MutablePathDescription()->MutableTablePartitions(), *info, shards, false, false);
        Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&partitionsNoKeys);
        msg.Clear();
        FillTableBoundaries(msg.MutablePathDescription()->MutableTable()->MutableSplitBoundary(), *info);
        Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&boundaries);
    }
    benchmark::DoNotOptimize(partitions);
    benchmark::DoNotOptimize(partitionsNoKeys);
    benchmark::DoNotOptimize(boundaries);
    const NKikimr::IAllocState::TState afterLegacy = NKikimr::TAllocState::Get();
    legacyCacheBytes = i64(afterLegacy.AllocatedMemory) - i64(baseLegacy.AllocatedMemory);
    state.counters["legacyCacheAllocBytes"] = double(legacyCacheBytes);
}

const auto ArgPartitionCols = [](::benchmark::Benchmark* b) {
    for (int64_t partitions : {1, 256, 10'000, 100'000}) {
        for (int64_t cols : {4, 64}) {
            b->Args({partitions, cols});
        }
    }
};

BENCHMARK(BenchFillPartitionStats)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchSerializeTablePartitions)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchSerializeTablePartitionsCached)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchBuildTableBoundaries)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchBuildTableDescribe)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchBuildTableDescribeCached)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchSplitStep)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchParseDescribeCanonical)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchParseDescribeSingle)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchCacheMemory)->Apply(ArgPartitionCols)->Unit(benchmark::kMicrosecond);

} // namespace
