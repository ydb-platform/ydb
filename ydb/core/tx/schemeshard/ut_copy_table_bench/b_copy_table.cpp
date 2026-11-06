#include <ydb/core/tx/schemeshard/schemeshard__operation_copy_table.h> // for TShardProposal and TShardProposalInputs

#include <benchmark/benchmark.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

// ---------------------------------------------------------------------------
// Helpers to fabricate TTableInfo objects without a live SchemeShard.
// ---------------------------------------------------------------------------

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n) ? TString(1, char(i + 1)) : TString{};
        v.emplace_back(TShardIdx(ownerId, i), range);
    }
    return v;
}

// Populate a TTableInfo with a realistic column set and partitioning.
// state.range(0) = partition count, state.range(1) = value column count.
TTableInfo::TPtr MakeBenchTable(::benchmark::State& state) {
    const ui32 numPartitions = static_cast<ui32>(state.range(0));
    const ui32 numValueCols  = static_cast<ui32>(state.range(1));

    auto info = MakeIntrusive<TTableInfo>();

    // Key column (id=1).
    info->Columns.emplace(1, TTableInfo::TColumn("key", 1, NScheme::NTypeIds::Int64, "", true));
    info->Columns[1].KeyOrder = 0;
    // KeyColumnIds is maintained by the real SchemeShard at every Columns mutation point;
    // FillDescriptionCache relies on it, so the fabricated table must match a real one.
    info->KeyColumnIds = {1};

    // Value columns (id=2..).
    for (ui32 c = 0; c < numValueCols; ++c) {
        const TString colName = TStringBuilder() << "col_" << c;
        info->Columns.emplace(2 + c, TTableInfo::TColumn(colName, 2 + c, NScheme::NTypeIds::Uint64, "", false));
    }

    info->SetPartitioning(MakeShards(numPartitions));
    return info;
}

// ---------------------------------------------------------------------------
// Benchmark fixture: fabricates src/dst TTableInfo + TShardProposalInputs.
// ---------------------------------------------------------------------------

struct TCopyTableBenchFixture : public benchmark::Fixture {
    TTableInfo::TPtr SrcTable;
    TTableInfo::TPtr DstTable;
    THashMap<TShardIdx, TTabletId> ShardToTabletMap;
    NKikimrSchemeOp::TTableDescription EmptyChildren;
    TVector<TPathId> EmptyStreams;
    NKikimrTxDataShard::TCreateCdcStreamNotice EmptyCdcNotice;
    NKikimrSubDomains::TProcessingParams ProcessingParams;
    // Storage for the inputs — kept alive across benchmark iterations.
    // Constructed via placement-new to work around the deleted copy assignment
    // (TShardProposalInputs has reference members).
    NCopyTable::TShardProposalInputs* Inputs = nullptr;

    alignas(NCopyTable::TShardProposalInputs) char InputsStorage[sizeof(NCopyTable::TShardProposalInputs)];

    void SetUp(::benchmark::State& state) override {
        SrcTable = MakeBenchTable(state);
        DstTable = MakeBenchTable(state);

        const ui32 n = static_cast<ui32>(state.range(0));
        ShardToTabletMap.reserve(n * 2);
        for (ui32 i = 0; i < n; ++i) {
            TShardIdx srcIdx(1, i);
            TShardIdx dstIdx(2, i);
            ShardToTabletMap[srcIdx] = TTabletId(1000 + i);
            ShardToTabletMap[dstIdx] = TTabletId(2000 + i);
        }

        auto shardResolver = [this](TShardIdx idx) -> TTabletId {
            return ShardToTabletMap.at(idx);
        };

        Inputs = new (InputsStorage) NCopyTable::TShardProposalInputs{
            .SrcTable = *SrcTable,
            .DstTable = *DstTable,
            .ShardToTablet = shardResolver,
            .SourcePathId = TPathId(1, 10),
            .TargetPathId = TPathId(1, 20),
            .CdcPathId = TPathId(),  // InvalidPathId — no incremental backup
            .DstSubDomainPathId = 0,
            .DstProcessingParams = ProcessingParams,
            .SelfTabletId = 72057594046678944ull,
            .SelfId = TActorId(),
            .SeqNo = TMessageSeqNo{1, 0},
            .DstSchemaVersion = 1,
            .TxId = TTxId(42),
            .DstPath = "/Root/dst",
            .DstName = "dst",
            .DstChildrenTemplate = EmptyChildren,
            .UseIncrementalBackup = false,
            .StreamsToDrop = EmptyStreams,
            .CoordVersion = 0,
            .CreateCdcNotice = EmptyCdcNotice,
        };
    }

    void TearDown(::benchmark::State&) override {
        if (Inputs) {
            Inputs->~TShardProposalInputs();
            Inputs = nullptr;
        }
    }
};

// ---------------------------------------------------------------------------
// Benchmark: BuildConfigurePartsProposals for N partitions.
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(TCopyTableBenchFixture, BuildConfigureParts)(benchmark::State& state) {
    for (auto _ : state) {
        auto proposals = NCopyTable::BuildConfigurePartsProposals(*Inputs);
        benchmark::DoNotOptimize(proposals);
    }
}

BENCHMARK_REGISTER_F(TCopyTableBenchFixture, BuildConfigureParts)
    ->ArgsProduct({
        {1, 4, 16, 64, 256},   // partition count
        {4, 16, 64}             // value column count
    })
    ->Unit(benchmark::kMicrosecond);

} // namespace
