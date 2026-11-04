#include <ydb/core/tx/schemeshard/schemeshard_path_describer.h>

#include <benchmark/benchmark.h>

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

// EndOfRange key for a uniform Int64 range: [i - 1, i); the last shard's empty
// range encodes +inf (matches the real SchemeShard convention).
TString MakeEndOfRangeKey(ui64 i, ui64 n) {
    if (i == n) {
        return TSerializedCellVec::Serialize({});
    }
    return TSerializedCellVec::Serialize({TCell::Make(i)});
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
        info->Columns.emplace(2 + c, TTableInfo::TColumn(colName, 2 + c, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", false));
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
    for (auto _ : state) {
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, opts);
        benchmark::DoNotOptimize(result.PreSerializedData);
    }
}

// One split step of a split campaign: ApplySplitMerge of one partition into
// two + the subsequent cold describe build. Captures the per-step storm cost;
// per iteration the table gains one partition (negligible vs the args range).
static void BenchSplitStep(::benchmark::State& state) {
    auto info = MakeBenchTable(state);
    // Shard map covers the initial shards plus room for the split outputs.
    auto shards = MakeShardInfos(static_cast<ui32>(state.range(0)) + 1000);
    ui64 nextShardId = static_cast<ui64>(state.range(0)) + 1;
    ui64 nextKey = state.range(0);
    TShardIdx firstShard(1, 0);
    for (auto _ : state) {
        // Split the first partition into two; the low half becomes the new
        // first partition, so the next iteration splits it further.
        TShardIdx removed = firstShard;
        TVector<TTableShardInfo> dst = {
            TTableShardInfo(TShardIdx(1, nextShardId), TSerializedCellVec::Serialize({TCell::Make(nextKey + 1)})),
            TTableShardInfo(TShardIdx(1, nextShardId + 1), TSerializedCellVec::Serialize({TCell::Make(ui64(1))})),
        };
        TShardInfo loShard;
        loShard.TabletID = TTabletId(1000 + nextShardId);
        shards.emplace(TShardIdx(1, nextShardId), loShard);
        TShardInfo hiShard;
        hiShard.TabletID = TTabletId(1000 + nextShardId + 1);
        shards.emplace(TShardIdx(1, nextShardId + 1), hiShard);
        firstShard = dst.front().ShardIdx;
        nextShardId += 2;
        info->ApplySplitMerge(std::move(dst), {removed}, 0, TInstant::Now(), false, false);

        // ApplySplitMerge splices the element-wise caches itself: only the touched
        // positions are re-serialized lazily by this describe.
        TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
        BuildTableDescribe(result, *info, shards, AllDescribeOptions());
        benchmark::DoNotOptimize(result.PreSerializedData);
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
    NKikimrScheme::TEvDescribeSchemeResult msg;
    msg.SetPath("/Root/table");
    auto parts = msg.MutablePathDescription()->MutableTablePartitions();
    parts->Reserve(n);
    for (const auto* p : info->GetPartitions()) {
        auto* part = parts->Add();
        part->SetDatashardId(ui64(shards.at(p->ShardIdx).TabletID));
        part->SetIsPoint(false);
        part->SetIsInclusive(false);
        part->SetEndOfRangeKeyPrefix(p->EndOfRange);
    }
    TString wire;
    Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&wire);
    state.counters["bytes"] = wire.size();
    for (auto _ : state) {
        NKikimrScheme::TEvDescribeSchemeResult parsed;
        Y_ABORT_UNLESS(parsed.ParseFromString(wire));
        Y_ABORT_UNLESS(static_cast<ui32>(parsed.GetPathDescription().TablePartitionsSize()) == n);
        benchmark::DoNotOptimize(parsed);
    }
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

} // namespace
