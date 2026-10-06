#pragma once
#include "dq_hash_join_table.h"
#include "dq_block_hash_join_settings.h"
#include "dq_join_filters.h"
#include <algorithm>
#include <numeric>
#include <vector>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/spilled_storage.h>
#include <yql/essentials/minikql/comp_nodes/mkql_counters.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

namespace NKikimr::NMiniKQL {

// The join a hash join node actually computes: the SQL kind plus the physical side whose rows that
// kind preserves. Passed as a single template argument so nothing has to carry the side at runtime.
struct TPhysicalJoin {
    EJoinKind Kind;
    ESide Preserved = ESide::Probe;
    // empty equality keys turn the hash lookup into a cartesian scan
    bool IsGrid = Kind == EJoinKind::Cross;

    constexpr ESide NullSupplying() const {
        return OtherSide(Preserved);
    }
};

struct TColumnsMetadata {
    TMKQLVector<ui32> KeyColumns;
    TMKQLVector<TType*> ColumnTypes;
};

struct TJoinMetadata {
    TColumnsMetadata Build;
    TColumnsMetadata Probe;
    TKeyTypes KeyTypes;
};

TKeyTypes KeyTypesFromColumns(const TMKQLVector<TType*>& types, const TMKQLVector<ui32>& keyIndexes);

template <EJoinKind Kind> struct TRenamedOutput {
    TRenamedOutput(TDqUserRenames renames, const TMKQLVector<TType*>& leftColumnTypes,
                   const TMKQLVector<TType*>& rightColumnTypes)
        : OutputBuffer()
        , NullTuples(std::max(leftColumnTypes.size(), rightColumnTypes.size()), NYql::NUdf::TUnboxedValuePod{})
        , Renames(std::move(renames))
    {}

    int TupleSize() const {
        return Renames.size();
    }

    int SizeTuples() const {
        MKQL_ENSURE(OutputBuffer.size() % TupleSize() == 0, "buffer contains tuple parts??");
        return OutputBuffer.size() / TupleSize();
    }

    TMKQLVector<NUdf::TUnboxedValue> OutputBuffer;

    auto MakeConsumeFn() {
        return [&] {
            if constexpr (SemiOrOnlyJoin(Kind)) {
                return [&](NJoinTable::TTuple tuple) {
                    MKQL_ENSURE(tuple != nullptr, "null output row in semi/only join?");
                    for (int index = 0; index < std::ssize(Renames); ++index) {
                        auto thisRename = Renames[index];
                        OutputBuffer.push_back(tuple[thisRename.Index]);
                    }
                };
            } else {
                return [&](NJoinTable::TTuple probe, NJoinTable::TTuple build) {
                    if (!probe) { // todo: remove nullptr checks for some join types.
                        probe = NullTuples.data();
                    }

                    if (!build) {
                        build = NullTuples.data();
                    }
                    for (int index = 0; index < std::ssize(Renames); ++index) {
                        auto thisRename = Renames[index];
                        if (thisRename.Side == EJoinSide::kLeft) {
                            OutputBuffer.push_back(probe[thisRename.Index]);
                        } else {
                            OutputBuffer.push_back(build[thisRename.Index]);
                        }
                    }
                };
            }
        }();
    }

  private:
    const TMKQLVector<NYql::NUdf::TUnboxedValue> NullTuples;
    const TDqUserRenames Renames;
};

// Some joins produce concatenation of 2 tuples, some produce one tuple(effectively)
template <typename Fun, typename Tuple>
concept JoinMatchFun = std::invocable<Fun, NJoinTable::TTuple> || std::invocable<Fun, TSides<Tuple>>;


template <typename Source, EJoinKind Kind> class TJoin : public TComputationValue<TJoin<Source, Kind>> {
    using TBase = TComputationValue<TJoin>;

  public:
    TJoin(TMemoryUsageInfo* memInfo, Source probe, Source build, TJoinMetadata meta, NUdf::TLoggerPtr logger,
          TString componentName)
        : TBase(memInfo)
        , Meta_(meta)
        , Logger_(logger)
        , LogComponent_(logger->RegisterComponent(componentName))
        , Build_(std::move(build))
        , Probe_(std::move(probe))
        , Table_(BuildSize(), TWideUnboxedEqual{Meta_.KeyTypes}, TWideUnboxedHasher{Meta_.KeyTypes},
                 NJoinTable::NeedToTrackUnusedRightTuples(Kind))
    {
        MKQL_ENSURE(BuildSize() == ProbeSize(), "unimplemented");
        MKQL_ENSURE(Kind != EJoinKind::Cross, "Unsupported join kind");
        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Debug, "TScalarHashJoinState created");
    }

    const TJoinMetadata& Meta() const {
        return Meta_;
    }

    int ProbeSize() const {
        return Probe_.UserDataSize();
    }

    int BuildSize() const {
        return Build_.UserDataSize();
    }

    EFetchResult MatchRows(TComputationContext& ctx, auto consumeOneOrTwoTuples) {
        while (!Build_.Finished()) {
            auto res = Build_.ForEachRow(ctx, [&](auto tuple) { Table_.Add({tuple, tuple + Build_.UserDataSize()}); });
            switch (res) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.Build();
                break;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                break;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (!Probe_.Finished()) {
            auto result = Probe_.ForEachRow(ctx, [&](NJoinTable::TTuple probeTuple) {
                bool found = false;
                Table_.Lookup(probeTuple, [&](NJoinTable::TTuple matchedBuildTuple) {
                    if constexpr (ContainsRowsFromInnerJoin(Kind)) {
                        consumeOneOrTwoTuples(probeTuple, matchedBuildTuple);
                    }
                    found = true;
                });
                if (!found) {
                    if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Left || Kind == EJoinKind::Full) {
                        consumeOneOrTwoTuples(probeTuple, nullptr);
                    }
                    if constexpr (Kind == EJoinKind::LeftOnly) {
                        consumeOneOrTwoTuples(probeTuple);
                    }
                }
                if constexpr (Kind == EJoinKind::LeftSemi) {
                    if (found) {
                        consumeOneOrTwoTuples(probeTuple);
                    }
                }
            });
            switch (result) {
            case NYql::NUdf::EFetchStatus::Finish: {
                int consumedTotal = 0;
                if (Table_.UnusedTrackingOn()) {
                    if constexpr (Kind == EJoinKind::RightSemi) {
                        for (auto& v : Table_.MapView()) {
                            if (v.second.Used) {
                                for (NJoinTable::TTuple used : v.second.Tuples) {

                                    ++consumedTotal;
                                    consumeOneOrTwoTuples(used);
                                }
                            }
                        }
                    }
                    Table_.ForEachUnused([&](NJoinTable::TTuple unused) {
                        if constexpr (Kind == EJoinKind::RightOnly) {
                            ++consumedTotal;
                            consumeOneOrTwoTuples(unused);
                        }
                        if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Right ||
                                      Kind == EJoinKind::Full) {
                            ++consumedTotal;
                            consumeOneOrTwoTuples(nullptr, unused);
                        }
                    });
                }
                return consumedTotal == 0 ? EFetchResult::Finish : EFetchResult::One;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                return EFetchResult::One;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        return EFetchResult::Finish;
    }

  private:
    const TJoinMetadata Meta_;
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;

    Source Build_;
    Source Probe_;
    NJoinTable::TStdJoinTable Table_;
};


enum class EIsInMemory : bool {
    Spilled,
    InMemory,
};

template <typename T> [[nodiscard]] T ExtractReadyFuture(NThreading::TFuture<T>&& future) {
    MKQL_ENSURE(future.IsReady(), "no blocking wait in comp nodes");
    return future.ExtractValueSync();
}



template<typename T>
concept JoinMatchFn = std::invocable<T, TSingleTuple> && std::invocable<T, TSides<TSingleTuple>>;

TPackResult GetPage(TFuturePage&& future);

using ProbeSpillingPage = std::optional<TPackResult>;

struct TSpilledBucket : public TSides<TMKQLVector<ISpiller::TKey>> {
    TMKQLVector<ISpiller::TKey> ProbeMatchKeys;
};

using PairOfSpilledBuckets = TSides<TBucket>;

bool AllFuturesReady(const auto& futures) {
    return std::ranges::all_of(futures, [&](const auto& future) { return future.IsReady(); });
}

struct TFutureTableData {
    TMKQLVector<TFuturePage> Futures;
    NThreading::TFuture<void> All;
};

struct TPendingProbeMatchWrite {
    NThreading::TFuture<ISpiller::TKey> Write;
    size_t GridProbeIndex;
};

struct TTableAndSomeData {
    NJoinTable::TNeumannJoinTable Table;
    TMKQLDeque<TFuturePage> Futures;
    TMKQLDeque<TFuturePage> FutureMatchBits;
    TMKQLDeque<size_t> FutureGridProbeIndices;
    TMKQLVector<TPendingProbeMatchWrite> PendingMatchWrites;
    std::optional<TPackResult> CurrentProbePack;
    TMKQLBitMap CurrentMatchBits;
    ui32 ProbeResumeIndex = 0;
    size_t BuildCursor = 0;
    size_t PreservedResumeIndex = 0;
};

namespace NJoinPackedTuples {
template <typename Source> class TInMemoryHashJoin {
  public:
    using TTable = NJoinTable::TNeumannJoinTable;

    static constexpr bool FlushOnYield = false;

    TInMemoryHashJoin(TSides<Source> sources, TComputationContext& ctx, TString componentName,
                    TSides<const NPackedTuple::TTupleLayout*> layouts)
        : Logger_(ctx.MakeLogger())
        , LogComponent_(Logger_->RegisterComponent(componentName))
        , Sources_(std::move(sources))
        , Layouts_(layouts)
        , Table_(Layouts_.Build)
    {}

    TPackResult Flatten(TMKQLVector<TPackResult> tuples) {
        return Layouts_.Build->Flatten(tuples);
    }

    EFetchResult MatchRows([[maybe_unused]] TComputationContext& ctx,
                           JoinMatchFun<TSingleTuple> auto consumeOneOrTwoTuples, auto isFull) {
        while (!Sources_.Build.Finished()) {
            FetchResult<IBlockLayoutConverter::TPackResult> var = Sources_.Build.FetchRow();
            switch (AsStatus(var)) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.BuildWith(Flatten(std::move(BuildChunks_)));
                break;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                BuildChunks_.push_back(std::move(GetPayload(var)));
                break;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (Table_.Empty()) {
            return EFetchResult::Finish;
        }

        auto lookupProbeRow = [&](TSingleTuple probeTuple) {
            return Table_.Lookup(probeTuple, BuildCursor_,
                                 [&](TSingleTuple buildTuple) {
                                     consumeOneOrTwoTuples(TSides<TSingleTuple>{.Build = buildTuple, .Probe = probeTuple});
                                 },
                                 isFull);
        };

        if (FetchedPack_.has_value()) {
            ui32 idx = 0;
            for (TSingleTuple probeTuple : *FetchedPack_) {
                if (idx++ < ResumeIndex_) {
                    continue;
                }
                if (!lookupProbeRow(probeTuple)) {
                    ResumeIndex_ = idx - 1;
                    return EFetchResult::One;
                }
                if (isFull()) {
                    ResumeIndex_ = idx;
                    return EFetchResult::One;
                }
            }
            FetchedPack_ = std::nullopt;
            ResumeIndex_ = 0;
        }

        if (!Sources_.Probe.Finished()) {
            FetchResult<IBlockLayoutConverter::TPackResult> var = Sources_.Probe.FetchRow();
            const NKikimr::NMiniKQL::EFetchResult resEnum = AsResult(var);

            if (resEnum == EFetchResult::One) {
                FetchedPack_ = std::move(GetPayload(var));
                ResumeIndex_ = 0;
                ui32 idx = 0;
                for (TSingleTuple probeTuple : *FetchedPack_) {
                    idx++;
                    if (!lookupProbeRow(probeTuple)) {
                        ResumeIndex_ = idx - 1;
                        return EFetchResult::One;
                    }
                    if (isFull()) {
                        ResumeIndex_ = idx;
                        return EFetchResult::One;
                    }
                }
                FetchedPack_ = std::nullopt;
            }

            return resEnum;
        }

        return EFetchResult::Finish;
    }

  private:
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    TSides<Source> Sources_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    TTable Table_;
    TPackResult BuildData_;
    TMKQLVector<IBlockLayoutConverter::TPackResult> BuildChunks_;
    std::optional<IBlockLayoutConverter::TPackResult> FetchedPack_;
    ui32 ResumeIndex_ = 0;
    size_t BuildCursor_ = 0;
};

template <typename Source, TSpillerSettings Settings, TPhysicalJoin Join> class THybridHashJoin {
    struct Logger {
        Logger(TComputationContext& ctx, TString name)
        : Logger_(ctx.MakeLogger())
        , LogComponent_(Logger_->RegisterComponent(name)) {}
        NUdf::TLoggerPtr Logger_;
        NUdf::TLogComponentId LogComponent_ ;

        void LogDebug(TStringRef msg) const {
            UDF_LOG(Logger_, LogComponent_, NYql::NUdf::ELogLevel::Debug, msg);
        }
    };

    using Self = THybridHashJoin<Source, Settings, Join>;

  public:
    using TTable = NJoinTable::TNeumannJoinTable;

    static constexpr bool FlushOnYield = false;

    static constexpr bool IsGrid = Join.IsGrid;

    // Row-preserving side is hashed, so its rows are emitted by scanning the table after the probe is done
    static constexpr bool PreservedRowsInBuildTable() {
        return Join.Preserved == ESide::Build && (Join.Kind == EJoinKind::Left || LeftSemiOrOnly(Join.Kind));
    }

    static constexpr bool PreservedRowsInProbeStream() {
        return Join.Preserved == ESide::Probe && (Join.Kind == EJoinKind::Left || LeftSemiOrOnly(Join.Kind));
    }

    // Keep the same per-row representation for keyed and grid joins. A keyed probe page is consumed
    // once, while a grid probe page carries these bits through all spilled build partitions.
    static constexpr bool TracksProbeMatches() {
        return PreservedRowsInProbeStream();
    }

    struct Init {};

    struct FetchingBuild {
        FetchingBuild(Self& self)
            : Build(std::move(self.Sources_).Build())
            , Spiller(self.Spiller_, self.Layouts_.Build, IsGrid ? EBucketAssign::RoundRobin : EBucketAssign::Hash)
        {
            self.Logger_.LogDebug("FetchingBuild stage started");
        }

        Source Build;
        TBucketsSpiller<Settings> Spiller;
        std::optional<TPackResult> Pack;
    };

    struct BuildingInMemoryTable {
        BuildingInMemoryTable(Self& self, TBucketsSpiller<Settings> spiller)
            : Spiller(std::move(spiller))
        {
            for(int index = 0; index < std::ssize(Spiller.GetBuckets()); ++index) {
                ProbeState.Buckets.push_back(TTable{self.Layouts_.Build, PreservedRowsInBuildTable()});
            }
            self.Logger_.LogDebug("BuildingInMemoryTable stage started");
        }

        TBucketsSpiller<Settings> Spiller;
        TProbeSpiller<Settings>::State ProbeState;
    };

    struct Probing {
        Probing(Self& self, TProbeSpiller<Settings>::State&& init)
            : Probe(std::move(self.Sources_).Probe())
            , Spiller(self.Spiller_, self.Layouts_.Probe, std::move(init))
        {
            using SpillerType = TProbeSpiller<Settings>;
            int inMemoryBuckets =
                std::accumulate(Spiller.GetState().Buckets.begin(), Spiller.GetState().Buckets.end(), 0,
                                [&](int im, const SpillerType::Bucket& bucket) { return im += !SpillerType::IsBucketSpilled(bucket); });
            int spilledBuckets =
                std::accumulate(Spiller.GetState().Buckets.begin(), Spiller.GetState().Buckets.end(), 0,
                                [&](int spilled, const SpillerType::Bucket& bucket) { return spilled += SpillerType::IsBucketSpilled(bucket); });

            if constexpr (IsGrid) {
                GridProbeBucket = Spiller.FirstSpilledBucket();
            }

            self.Logger_.LogDebug(Sprintf("Probing stage started, in memory buckets: %i, spilled buckets: %i",
                                          inMemoryBuckets, spilledBuckets));
        }

        Source Probe;
        TProbeSpiller<Settings> Spiller;
        std::optional<TPackResult> FetchedPack;
        ui32 ResumeIndex = 0;
        size_t BuildCursor = 0;
        // Cursor of the post-probe scan over preserved rows left in the in-memory tables
        int PreservedBucketIndex = 0;
        size_t PreservedResumeIndex = 0;
        bool ProbeRowMatched = false;
        // Grid only: build table the current probe row stopped at, and the bucket keeping the probe stream
        int GridBuildBucket = 0;
        std::optional<int> GridProbeBucket;
    };

    using DumpedBuckets = std::unordered_map<int, TSpilledBucket>;

    struct DumpRestOfPages {
        DumpRestOfPages(Self& self, std::unordered_map<int, TSpilledBucket>&& base,
                        TMKQLVector<TSpillingPage>&& pages)
            : AlreadyDumped(std::move(base))
            , Pages(std::move(pages))
        {
            const int pageCount = Pages.size();
            StartNextBatch(*self.Spiller_);
            self.Logger_.LogDebug(Sprintf("DumpRestOfPages stage started, page count: %i", pageCount));
        }

        void StartNextBatch(ISpiller& spiller) {
            MKQL_ENSURE(Futures.empty(), "previous spilling batch is not finished");
            MKQL_ENSURE(!Pages.empty(), "no pages to spill");

            const int batchSize = std::min<int>(Settings.SpillingPagesAtTime, Pages.size());
            Futures.reserve(batchSize);
            for (int index = 0; index < batchSize; ++index) {
                auto page = *GetBackOrNull(Pages);
                page.StartSpilling(spiller);
                Futures.push_back(std::move(page));
            }

            NThreading::TWaitGroup<NThreading::TWaitPolicy::TAll> wg;
            for (auto& page : Futures) {
                wg.Add(page.Write);
                if (page.MatchWrite) {
                    wg.Add(*page.MatchWrite);
                }
            }
            All = std::move(wg).Finish();
        }

        DumpedBuckets AlreadyDumped;
        TMKQLVector<TSpillingPage> Pages;
        TMKQLVector<TSpillingPage> Futures;
        NThreading::TFuture<void> All;
    };

    struct PairAndMetadata {
        TSpilledBucket Buckets;
        int BucketIndex;
        // Grid replays the probe stream for every build bucket, so only the last pass may drop the blobs
        bool IsLastPair = false;
        size_t GridProbeCursor = 0;
        std::variant<TFutureTableData, TTableAndSomeData> Table = TFutureTableData{};
    };

    struct JoinPairsOfPartitions {
        JoinPairsOfPartitions(Self& self, std::unordered_map<int, TSpilledBucket>&& pairs)
            : Pairs(std::move(pairs))
        {
            if constexpr (IsGrid) {
                size_t probePages = 0;
                for (const auto& [_, bucket] : Pairs) {
                    probePages += bucket.Probe.size();
                    if constexpr (TracksProbeMatches()) {
                        MKQL_ENSURE(bucket.Probe.size() == bucket.ProbeMatchKeys.size(),
                                    "probe pages and match keys are out of sync");
                    }
                }
                GridProbeKeys.reserve(probePages);
                if constexpr (TracksProbeMatches()) {
                    GridProbeMatchKeys.reserve(probePages);
                }
                // The probe stream sits in one bucket, but that bucket is joined like any other, so
                // move the keys out and share them with every pair.
                for (auto& [_, bucket] : Pairs) {
                    GridProbeKeys.insert(GridProbeKeys.end(), bucket.Probe.begin(), bucket.Probe.end());
                    TMKQLVector<ISpiller::TKey>().swap(bucket.Probe);
                    if constexpr (TracksProbeMatches()) {
                        GridProbeMatchKeys.insert(GridProbeMatchKeys.end(), bucket.ProbeMatchKeys.begin(),
                                                  bucket.ProbeMatchKeys.end());
                        TMKQLVector<ISpiller::TKey>().swap(bucket.ProbeMatchKeys);
                    }
                }
                if constexpr (TracksProbeMatches()) {
                    MKQL_ENSURE(GridProbeKeys.size() == GridProbeMatchKeys.size(),
                                "probe pages and match keys are out of sync");
                }
            }
            self.Logger_.LogDebug(Sprintf("JoinPairsOfPartitions stage started, partitions count: %i",
                                          static_cast<int>(Pairs.size())));
        }

        std::unordered_map<int, TSpilledBucket> Pairs;
        std::optional<PairAndMetadata> SelectedPair;
        TMKQLVector<ISpiller::TKey> GridProbeKeys;
        TMKQLVector<ISpiller::TKey> GridProbeMatchKeys;
    };

    class Sources {
      public:
        Sources(TSides<Source> data) {
            for(ESide side: EachSide) { 
                Data_.SelectSide(side).emplace(std::move(data.SelectSide(side))); 
            }
        }

        Source Build() && {
            MKQL_ENSURE(Data_.Build, "trying to clone Source");
            return std::move(*Data_.Build);
        }

        Source Probe() && {
            MKQL_ENSURE(Data_.Probe, "trying to clone Source");
            return std::move(*Data_.Probe);
        }

      private:
        TSides<std::optional<Source>> Data_;
    };

    THybridHashJoin(TSides<Source> sources, TComputationContext& ctx, TString componentName,
                    TSides<const NPackedTuple::TTupleLayout*> layouts)
        : Logger_(ctx, componentName)
        , Layouts_(layouts)
        , Spiller_(ctx.SpillerFactory ? ctx.SpillerFactory->CreateSpiller() : nullptr)
        , Sources_(std::move(sources))
    {
    }

    struct Finish {};

    TPackResult Flatten(TMKQLVector<TPackResult> tuples) {
        return Layouts_.Build->Flatten(tuples);
    }

    EFetchResult WaitWhileSpilling() {
        return EFetchResult::Yield;
    }

    TPackResult GetPage(TFuturePage&& future, ESide side) {
        std::optional<NYql::TChunkedBuffer> buff = ExtractReadyFuture(std::move(future));
        MKQL_ENSURE(buff.has_value(), "corrupted extract key?");
        return Parse(std::move(*buff), Layouts_.SelectSide(side));
    }

    // Joins one probe row with every in-memory build table, then stores it for the spilled ones.
    // Returns false when the output filled up: GridBuildBucket and BuildCursor point at the table and
    // the build row to continue this probe row from.
    bool MatchGridRowInMemory(Probing& state, TSingleTuple probeRow, auto lookupToTable, auto finishProbeRow,
                              auto isFull) {
        auto& buckets = state.Spiller.GetState().Buckets;
        for (; state.GridBuildBucket < std::ssize(buckets); ++state.GridBuildBucket) {
            TTable* table = std::get_if<TTable>(&buckets[state.GridBuildBucket]);
            if (!table || table->Empty()) {
                continue;
            }
            if (!lookupToTable(*table, probeRow, state.BuildCursor, state.ProbeRowMatched)) {
                return false;
            }
            if (isFull()) {
                ++state.GridBuildBucket;
                return false;
            }
        }
        state.GridBuildBucket = 0;
        if (state.GridProbeBucket) {
            if constexpr (TracksProbeMatches()) {
                state.Spiller.AddRow({.Val = probeRow, .Side = ESide::Probe, .BucketIndex = *state.GridProbeBucket},
                                     state.ProbeRowMatched);
            } else {
                state.Spiller.AddRow({.Val = probeRow, .Side = ESide::Probe, .BucketIndex = *state.GridProbeBucket});
            }
        } else {
            finishProbeRow(probeRow, state.ProbeRowMatched);
        }
        return true;
    }

    EFetchResult MatchRows(TComputationContext& ctx, auto consume, auto isFull,
                           TPackedTuplePairFilter* filter = nullptr) {
        return filter ? MatchRowsImpl<true>(ctx, consume, isFull, filter)
                      : MatchRowsImpl<false>(ctx, consume, isFull, nullptr);
    }

    template <bool HasFilter>
    EFetchResult MatchRowsImpl([[maybe_unused]] TComputationContext& ctx, auto consume, auto isFull,
                               [[maybe_unused]] TPackedTuplePairFilter* filter) {
        auto notEnoughMemory = [hasSpiller = !!Spiller_] {
            return hasSpiller && TlsAllocState->IsMemoryYellowZoneEnabled();
        };
        auto finishProbeRow = [&](TSingleTuple probeRow, bool found) {
            if constexpr (PreservedRowsInProbeStream()) {
                if constexpr (Join.Kind == EJoinKind::Left || Join.Kind == EJoinKind::LeftOnly) {
                    if (!found) {
                        consume(probeRow);
                    }
                } else if constexpr (Join.Kind == EJoinKind::LeftSemi) {
                    if (found) {
                        consume(probeRow);
                    }
                }
            }
        };
        auto lookupToTable = [&](TTable& table, TSingleTuple probeRow, size_t& buildCursor, bool& found) {
            if constexpr (HasFilter) {
                filter->StartProbeRow(probeRow);
            }
            auto onMatch = [&](TSingleTuple tableMatch) {
                if constexpr (HasFilter) {
                    if (!filter->PairPasses(tableMatch)) {
                        return;
                    }
                }
                found = true;
                table.MarkUsed(tableMatch);
                if constexpr (Join.Kind == EJoinKind::Inner || Join.Kind == EJoinKind::Left ||
                              Join.Kind == EJoinKind::Cross) {
                    consume(TSides<TSingleTuple>{.Build = tableMatch, .Probe = probeRow});
                }
            };
            if constexpr (IsGrid) {
                if (!table.ForEachFrom(buildCursor, onMatch, isFull)) {
                    return false;
                }
                buildCursor = 0;
            } else if constexpr (SemiOrOnlyJoin(Join.Kind) && !PreservedRowsInBuildTable()) {
                found = table.LookupAny(probeRow, [&](TSingleTuple tableMatch) {
                    if constexpr (HasFilter) {
                        return filter->PairPasses(tableMatch);
                    }
                    return true;
                });
            } else {
                if (!table.Lookup(probeRow, buildCursor, onMatch, isFull)) {
                    return false;
                }
            }
            return true;
        };
        if (std::get_if<Init>(&State_)) {
            State_ = FetchingBuild{*this};
        } else if (auto* s = std::get_if<FetchingBuild>(&State_)) {
            FetchingBuild& state = *s;
            if (!state.Pack.has_value()) {
                FetchResult<TPackResult> var = state.Build.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok) {
                    state.Pack = std::move(GetPayload(var));
                } else {
                    MKQL_ENSURE(status == NYql::NUdf::EFetchStatus::Finish, "unhandled status");
                    MKQL_ENSURE(state.Build.Finished(), "sanity check");
                    State_ = BuildingInMemoryTable{*this, std::move(state.Spiller)};
                }
            } else {
                ESpillResult res = state.Spiller.SpillWhile(notEnoughMemory);
                switch (res) {
                case Spilling:
                    return WaitWhileSpilling();
                case FinishedSpilling:
                    break;
                case DontHavePages:{
                    break;
                }
                }
                for (TSingleTuple tuple: *state.Pack) { 
                    state.Spiller.AddRow(tuple); 
                }
                state.Pack = std::nullopt;
            }
        } else if (auto* s = std::get_if<BuildingInMemoryTable>(&State_)) {
            BuildingInMemoryTable& state = *s;
            ESpillResult res = state.Spiller.SpillWhile(notEnoughMemory);
            switch (res) {
                case Spilling:
                    return WaitWhileSpilling();
                case FinishedSpilling:
                    break;
                case DontHavePages:
                    break;
            }
            std::optional<int> smallestBucket = std::nullopt;
            for (int index = 0; index < std::ssize(state.Spiller.GetBuckets()); ++index) {
                TBucket& bucket = state.Spiller.GetBuckets()[index];
                bucket.DetatchBuildingPage();
                if (!bucket.Empty() && !bucket.IsSpilled()) {
                    if (!smallestBucket || state.Spiller.GetBuckets()[*smallestBucket].InMemoryPages().size() > bucket.InMemoryPages().size()) {
                        smallestBucket = index;
                    }
                }
            }
            if (!smallestBucket) {
                for(int index = 0; index < std::ssize(state.Spiller.GetBuckets()); ++index) {
                    TBucket& bucket = state.Spiller.GetBuckets()[index];
                    typename TProbeSpiller<Settings>::Bucket& probeBucket = state.ProbeState.Buckets[index];
                    if (!bucket.Empty()) {
                        TTable* table = std::get_if<TTable>(&probeBucket);
                        MKQL_ENSURE(table && table->Empty(), "sanity check");
                        MKQL_ENSURE(bucket.IsSpilled(), "only spilled buckets are left after building in-memory tables");
                        TSides<TBucket> thisBucket;
                        thisBucket.Build = std::move(bucket);
                        thisBucket.Probe.SpilledPages.emplace();
                        probeBucket = std::move(thisBucket);
                    }
                }
                State_ = Probing{*this, std::move(state.ProbeState)};
            } else {
                TTable* table = std::get_if<TTable>(&state.ProbeState.Buckets[*smallestBucket]);
                MKQL_ENSURE(table, "sanity check");
                TBucket& buildBucket = state.Spiller.GetBuckets()[*smallestBucket];

                table->BuildWith(Flatten(buildBucket.ReleaseInMemoryPages()));
                MKQL_ENSURE(state.Spiller.GetBuckets()[*smallestBucket].Empty(), "this bucket should be empty now");
            }

        } else if (auto* s = std::get_if<Probing>(&State_)) {
            Probing& state = *s;
            if (!state.FetchedPack.has_value()) {
                FetchResult<TPackResult> var = state.Probe.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok) {
                    state.FetchedPack = std::move(GetPayload(var));
                } else {
                    MKQL_ENSURE(status == NYql::NUdf::EFetchStatus::Finish, "unexpected enum");
                    if constexpr (PreservedRowsInBuildTable()) {
                        if (!EmitPreservedBuildRowsFromInMemoryBuckets(state.Spiller, state.PreservedBucketIndex,
                                                                       state.PreservedResumeIndex, consume, isFull)) {
                            return EFetchResult::One;
                        }
                    }
                    std::unordered_map<int, TSpilledBucket> alreadyDumped;
                    TMKQLVector<TSpillingPage> pages;
                    state.Spiller.FlushBuildingPages();
                    for (int index = 0; index < std::ssize(state.Spiller.GetState().Buckets); ++index) {
                        if (state.Spiller.IsBucketSpilled(index)) {
                            TSides<TBucket>& thisPair =
                                *std::get_if<TSides<TBucket>>(&state.Spiller.GetState().Buckets[index]);
                            for (ESide side : EachSide) {
                                TBucket& bucket = thisPair.SelectSide(side);
                                alreadyDumped[index].SelectSide(side) = std::move(*bucket.SpilledPages);
                                bucket.SpilledPages = std::nullopt;
                            }
                            alreadyDumped[index].ProbeMatchKeys =
                                std::move(state.Spiller.GetState().ProbeMatchKeys[index]);
                        }
                    }
                    for (auto& page : state.Spiller.GetState().InMemoryPages) {
                        pages.push_back(std::move(page));
                    }
                    state.Spiller.GetState().InMemoryPages.clear();
                    state.Spiller.GetState().InMemoryPages.shrink_to_fit();
                    if (pages.empty()) {
                        if (alreadyDumped.empty()) {
                            State_ = Finish{};
                        } else {
                            State_ = JoinPairsOfPartitions{*this, std::move(alreadyDumped)};
                        }
                    } else {

                        MKQL_ENSURE(!alreadyDumped.empty(), "0 dumped buckets but have some parts in memory?");
                        State_ = DumpRestOfPages{*this, std::move(alreadyDumped), std::move(pages)};
                    }
                }
            } else {
                switch (state.Spiller.SpillWhile(notEnoughMemory)) {
                case Spilling:
                    return WaitWhileSpilling();
                case FinishedSpilling:
                    break;
                case DontHavePages: {
                    break;
                }
                default:
                    MKQL_ENSURE(false, "unhandled ESpillResult case");
                }
                ui32 idx = 0;
                for (TSingleTuple tuple : *state.FetchedPack) {
                    if (idx++ < state.ResumeIndex) {
                        continue;
                    }
                    if constexpr (IsGrid) {
                        if (!MatchGridRowInMemory(state, tuple, lookupToTable, finishProbeRow, isFull)) {
                            state.ResumeIndex = idx - 1;
                            return EFetchResult::One;
                        }
                    } else {
                        int bucketIndex = Settings.BucketIndex(tuple);
                        bool thisBucketSpilled = state.Spiller.IsBucketSpilled(bucketIndex);
                        if (thisBucketSpilled) {
                            if constexpr (TracksProbeMatches()) {
                                state.Spiller.AddRow(
                                    {.Val = tuple, .Side = ESide::Probe, .BucketIndex = bucketIndex},
                                    state.ProbeRowMatched);
                            } else {
                                state.Spiller.AddRow(
                                    {.Val = tuple, .Side = ESide::Probe, .BucketIndex = bucketIndex});
                            }
                        } else {
                            TTable* thisTable = std::get_if<TTable>(&state.Spiller.GetState().Buckets[bucketIndex]);
                            MKQL_ENSURE(thisTable, "sanity check");
                            if (!lookupToTable(*thisTable, tuple, state.BuildCursor, state.ProbeRowMatched)) {
                                state.ResumeIndex = idx - 1;
                                return EFetchResult::One;
                            }
                            finishProbeRow(tuple, state.ProbeRowMatched);
                        }
                    }
                    state.ProbeRowMatched = false;
                    if (isFull()) {
                        state.ResumeIndex = idx;
                        return EFetchResult::One;
                    }
                }
                state.FetchedPack = std::nullopt;
                state.ResumeIndex = 0;
            }
        } else if (auto* s = std::get_if<DumpRestOfPages>(&State_)) {
            DumpRestOfPages& state = *s;
            if (state.All.IsReady()) {
                for (auto& page : state.Futures) {
                    auto it = state.AlreadyDumped.find(page.BucketIndex);
                    MKQL_ENSURE(it != state.AlreadyDumped.end(), "bucket with this index is processed already");
                    const ISpiller::TKey key = ExtractReadyFuture(std::move(page.Write));
                    it->second.SelectSide(page.Side).push_back(key);
                    if (page.MatchWrite) {
                        MKQL_ENSURE(page.Side == ESide::Probe, "build page has probe match state");
                        it->second.ProbeMatchKeys.push_back(ExtractReadyFuture(std::move(*page.MatchWrite)));
                    }
                }
                state.Futures.clear();
                if (state.Pages.empty()) {
                    State_ = JoinPairsOfPartitions{*this, std::move(state.AlreadyDumped)};
                } else {
                    state.StartNextBatch(*Spiller_);
                }

            } else {
                return WaitWhileSpilling();
            }
        } else if (auto* s = std::get_if<JoinPairsOfPartitions>(&State_)) {
            // TODO: Implement repartitioning logic here to handle cases where a single partition is too large and may cause out-of-memory (OOM) errors.
            JoinPairsOfPartitions& state = *s;
            if (!state.SelectedPair.has_value()) {
                std::optional bucket = GetFrontOrNull(state.Pairs);
                if (bucket.has_value()) {
                    state.SelectedPair =
                        PairAndMetadata{.Buckets = std::move(bucket->second), .BucketIndex = bucket->first,
                                        .IsLastPair = state.Pairs.empty()};
                    if constexpr (IsGrid) {
                        state.SelectedPair->GridProbeCursor = state.GridProbeKeys.size();
                    }
                    TFutureTableData data;
                    for (ISpiller::TKey key : state.SelectedPair->Buckets.Build) {
                        data.Futures.push_back(Spiller_->Extract(key));
                    }
                    data.All = NThreading::WaitAll(data.Futures);
                    state.SelectedPair->Table = std::move(data);
                } else {
                    State_ = Finish{};
                }
            } else {
                TMKQLVector<ISpiller::TKey>& currentProbe = state.SelectedPair->Buckets.Probe;
                TMKQLVector<ISpiller::TKey>& currentMatchKeys = state.SelectedPair->Buckets.ProbeMatchKeys;
                if constexpr (TracksProbeMatches() && !IsGrid) {
                    MKQL_ENSURE(currentProbe.size() == currentMatchKeys.size(),
                                "probe pages and match keys are out of sync");
                }
                if (auto* tdata = std::get_if<TFutureTableData>(&state.SelectedPair->Table)) {
                    if (tdata->All.IsReady()) {
                        TMKQLVector<TPackResult> vec;
                        for (auto& future : tdata->Futures) {
                            vec.push_back(GetPage(std::move(future), ESide::Build));
                        }
                        NJoinTable::TNeumannJoinTable table{Layouts_.Build, PreservedRowsInBuildTable()};
                        table.BuildWith(Flatten(std::move(vec)));
                        state.SelectedPair->Table = TTableAndSomeData{.Table = std::move(table), .Futures = {}};
                    } else {
                        return WaitWhileSpilling();
                    }
                } else {
                    auto* table = std::get_if<TTableAndSomeData>(&state.SelectedPair->Table);
                    MKQL_ENSURE(table, "sanity check");
                    const bool keepProbeBlobs = IsGrid && !state.SelectedPair->IsLastPair;
                    const bool lastProbePass = !IsGrid || state.SelectedPair->IsLastPair;
                    constexpr int MinFuturesInBuffer = 10;
                    if constexpr (TracksProbeMatches() && IsGrid) {
                        for (auto it = table->PendingMatchWrites.begin();
                             it != table->PendingMatchWrites.end();) {
                            if (it->Write.IsReady()) {
                                state.GridProbeMatchKeys[it->GridProbeIndex] =
                                    ExtractReadyFuture(std::move(it->Write));
                                it = table->PendingMatchWrites.erase(it);
                            } else {
                                ++it;
                            }
                        }
                        if (table->PendingMatchWrites.size() >= Settings.SpillingPagesAtTime) {
                            return WaitWhileSpilling();
                        }
                    }
                    const auto probePagesLeft = [&]() {
                        if constexpr (IsGrid) {
                            return state.SelectedPair->GridProbeCursor;
                        } else {
                            return currentProbe.size();
                        }
                    };
                    while (table->Futures.size() < MinFuturesInBuffer && probePagesLeft() != 0) {
                        ISpiller::TKey key;
                        ISpiller::TKey matchKey = 0;
                        if constexpr (IsGrid) {
                            const size_t pageIndex = --state.SelectedPair->GridProbeCursor;
                            key = state.GridProbeKeys[pageIndex];
                            if constexpr (TracksProbeMatches()) {
                                matchKey = state.GridProbeMatchKeys[pageIndex];
                                table->FutureGridProbeIndices.push_back(pageIndex);
                            }
                        } else {
                            key = *GetBackOrNull(currentProbe);
                            if constexpr (TracksProbeMatches()) {
                                matchKey = *GetBackOrNull(currentMatchKeys);
                            }
                        }
                        table->Futures.push_back(keepProbeBlobs ? Spiller_->Get(key) : Spiller_->Extract(key));
                        if constexpr (TracksProbeMatches()) {
                            table->FutureMatchBits.push_back(Spiller_->Extract(matchKey));
                        }
                    }
                    if (table->CurrentProbePack.has_value()) {
                        ui32 idx = 0;
                        for (TSingleTuple probeTuple : *table->CurrentProbePack) {
                            if (idx++ < table->ProbeResumeIndex) {
                                continue;
                            }
                            bool matched = false;
                            if constexpr (TracksProbeMatches()) {
                                MKQL_ENSURE(!table->CurrentMatchBits.Empty(),
                                            "missing current probe page match state");
                                matched = table->CurrentMatchBits.Get(idx - 1);
                            }
                            if (!lookupToTable(table->Table, probeTuple, table->BuildCursor, matched)) {
                                if constexpr (TracksProbeMatches()) {
                                    table->CurrentMatchBits.Set(idx - 1, matched);
                                }
                                table->ProbeResumeIndex = idx - 1;
                                return EFetchResult::One;
                            }
                            if (lastProbePass) {
                                finishProbeRow(probeTuple, matched);
                            }
                            if constexpr (TracksProbeMatches()) {
                                table->CurrentMatchBits.Set(idx - 1, matched);
                            }
                            if (isFull()) {
                                table->ProbeResumeIndex = idx;
                                return EFetchResult::One;
                            }
                        }
                        if constexpr (TracksProbeMatches()) {
                            MKQL_ENSURE(!table->CurrentMatchBits.Empty(),
                                        "missing current probe page match state");
                            if (!lastProbePass) {
                                MKQL_ENSURE(IsGrid, "only grid joins replay probe match state");
                                MKQL_ENSURE(!table->FutureGridProbeIndices.empty(),
                                            "missing current grid probe page index");
                                table->PendingMatchWrites.push_back(
                                    {.Write = Spill(*Spiller_, table->CurrentMatchBits),
                                     .GridProbeIndex = table->FutureGridProbeIndices.front()});
                            }
                            table->CurrentMatchBits.Reset();
                            if constexpr (IsGrid) {
                                table->FutureGridProbeIndices.pop_front();
                            }
                        }
                        table->CurrentProbePack = std::nullopt;
                        table->ProbeResumeIndex = 0;
                    } else if (table->Futures.empty()) {
                        MKQL_ENSURE(probePagesLeft() == 0, "sanity check");
                        if constexpr (TracksProbeMatches()) {
                            MKQL_ENSURE(table->FutureMatchBits.empty(), "unconsumed probe match reads");
                            if constexpr (!IsGrid) {
                                MKQL_ENSURE(currentMatchKeys.empty(), "unconsumed probe match keys");
                            } else {
                                MKQL_ENSURE(table->FutureGridProbeIndices.empty(),
                                            "unconsumed grid probe page indices");
                                if (!table->PendingMatchWrites.empty()) {
                                    return WaitWhileSpilling();
                                }
                            }
                        }
                        if constexpr (PreservedRowsInBuildTable()) {
                            if (!EmitPreservedBuildRows(table->Table, table->PreservedResumeIndex, consume, isFull)) {
                                return EFetchResult::One;
                            }
                        }
                        state.SelectedPair = std::nullopt;
                    } else {
                        const bool matchBitsReady = [&]() {
                            if constexpr (TracksProbeMatches()) {
                                return !table->FutureMatchBits.empty() && table->FutureMatchBits.front().IsReady();
                            } else {
                                return true;
                            }
                        }();
                        if (table->Futures.front().IsReady() && matchBitsReady) {
                            table->CurrentProbePack = GetPage(*GetFrontOrNull(table->Futures), ESide::Probe);
                            if constexpr (TracksProbeMatches()) {
                                std::optional<NYql::TChunkedBuffer> buffer =
                                    ExtractReadyFuture(std::move(*GetFrontOrNull(table->FutureMatchBits)));
                                MKQL_ENSURE(buffer, "missing queued probe page match state");
                                Parse(std::move(*buffer), table->CurrentMatchBits);
                                MKQL_ENSURE(table->CurrentMatchBits.Size() >=
                                                static_cast<size_t>(table->CurrentProbePack->NTuples),
                                            "probe page and match bitmap sizes differ");
                            }
                            table->ProbeResumeIndex = 0;
                        } else {
                            return WaitWhileSpilling();
                        }
                    }
                }
            }
        } else if (std::get_if<Finish>(&State_)) {
            return EFetchResult::Finish;
        } else {
            MKQL_ENSURE(false, "unreachable");
        }

        return EFetchResult::One;
    }

    // Both emit helpers return false when they stopped on a full output; the caller must return
    // control to the batch loop and call MatchRows again to pick the scan up where it left off.
    bool EmitPreservedBuildRows(TTable& table, size_t& resumeIndex, auto consume, auto isFull) {
        return table.ForEachWhereUsed(Join.Kind == EJoinKind::LeftSemi, resumeIndex, consume, isFull);
    }

    template <typename TSpiller>
    bool EmitPreservedBuildRowsFromInMemoryBuckets(TSpiller& spiller, int& bucketIndex, size_t& resumeIndex,
                                                   auto consume, auto isFull) {
        for (; bucketIndex < std::ssize(spiller.GetState().Buckets); ++bucketIndex) {
            if (spiller.IsBucketSpilled(bucketIndex)) {
                continue;
            }
            TTable* table = std::get_if<TTable>(&spiller.GetState().Buckets[bucketIndex]);
            if (!table || table->Empty()) {
                continue;
            }
            if (!EmitPreservedBuildRows(*table, resumeIndex, consume, isFull)) {
                return false;
            }
            resumeIndex = 0;
        }
        return true;
    }

  private:
    const Logger Logger_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    ISpiller::TPtr Spiller_;
    Sources Sources_;
    std::variant<Init, FetchingBuild, BuildingInMemoryTable, Probing, DumpRestOfPages, JoinPairsOfPartitions, Finish>
        State_ = Init{};
};
} // namespace NJoinPackedTuples

struct TParsedHashJoinArgs {
    EJoinKind Kind;
    TSides<TVector<ui32>> KeyColumns;
    TDqUserRenames UserRenames;
};

inline TBlockHashJoinSettings ParseHashJoinSettingsTuple(TRuntimeNode node) {
    TBlockHashJoinSettings settings;
    const auto* settingsTuple = AS_VALUE(TTupleLiteral, node);
    if (settingsTuple->GetValuesCount() >= 1) {
        settings.BuildSide =
            static_cast<EBuildSide>(AS_VALUE(TDataLiteral, settingsTuple->GetValue(0))->AsValue().Get<ui32>());
    }
    if (settingsTuple->GetValuesCount() >= 2) {
        const auto* keys = AS_VALUE(TTupleLiteral, settingsTuple->GetValue(1));
        settings.EqualNullsKeys.reserve(keys->GetValuesCount());
        for (ui32 i = 0; i < keys->GetValuesCount(); ++i) {
            settings.EqualNullsKeys.push_back(AS_VALUE(TDataLiteral, keys->GetValue(i))->AsValue().Get<ui32>());
        }
    }
    return settings;
}

inline TParsedHashJoinArgs ParseCommonHashJoinArgs(TCallable& callable) {
    TParsedHashJoinArgs res;
    res.Kind = GetJoinKind(AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<ui32>());

    const auto parseKeys = [](TRuntimeNode node) {
        TVector<ui32> keys;
        const auto tuple = AS_VALUE(TTupleLiteral, node);
        for (ui32 i = 0; i < tuple->GetValuesCount(); ++i) {
            keys.push_back(AS_VALUE(TDataLiteral, tuple->GetValue(i))->AsValue().Get<ui32>());
        }
        return keys;
    };
    res.KeyColumns.Probe = parseKeys(callable.GetInput(3));
    res.KeyColumns.Build = parseKeys(callable.GetInput(4));
    MKQL_ENSURE(res.KeyColumns.Build.size() == res.KeyColumns.Probe.size(), "Key columns mismatch");
    if (res.Kind == EJoinKind::Cross) {
        MKQL_ENSURE(res.KeyColumns.Build.empty(), "Specifying key columns is not allowed for cross join");
    } else if (res.Kind == EJoinKind::Inner && res.KeyColumns.Build.empty()) {
        // A keyless inner join has exactly cross-join semantics. Canonicalize it
        // so block and scalar execution use the same physical specialization.
        res.Kind = EJoinKind::Cross;
    }

    res.UserRenames = FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));
    return res;
}

inline TDqRenames<ESide> BuildImplRenames(const TDqUserRenames& userRenames) {
    TDqRenames<ESide> renames;
    for (auto rename : userRenames) {
        const ESide side = rename.Side == EJoinSide::kLeft ? ESide::Probe : ESide::Build;
        renames.push_back({.Index = rename.Index, .Side = side});
    }
    return renames;
}

template <bool IsGrid, template <TPhysicalJoin> class Wrapper, typename TResult, EJoinKind Kind, typename... Args>
TResult* DispatchHashJoinByPreservedSide(ESide preservedSide, Args&&... args) {
    switch (preservedSide) {
    case ESide::Probe:
        return new Wrapper<TPhysicalJoin{Kind, ESide::Probe, IsGrid}>(std::forward<Args>(args)...);
    case ESide::Build:
        return new Wrapper<TPhysicalJoin{Kind, ESide::Build, IsGrid}>(std::forward<Args>(args)...);
    }
    Y_UNREACHABLE();
}

template <template <TPhysicalJoin> class Wrapper, typename TResult, EJoinKind Kind, typename... Args>
TResult* DispatchHashJoinByMode(ESide preservedSide, bool isGrid, Args&&... args) {
    if (isGrid) {
        return DispatchHashJoinByPreservedSide<true, Wrapper, TResult, Kind>(preservedSide,
                                                                            std::forward<Args>(args)...);
    }
    return DispatchHashJoinByPreservedSide<false, Wrapper, TResult, Kind>(preservedSide,
                                                                         std::forward<Args>(args)...);
}

template <template <TPhysicalJoin> class Wrapper, typename TResult, typename... Args>
TResult* DispatchHashJoinByKind(EJoinKind kind, ESide preservedSide, bool isGrid, TStringBuf unsupportedMessage,
                                Args&&... args) {
    using enum EJoinKind;
    switch (kind) {
    case Inner:
        return new Wrapper<TPhysicalJoin{Inner}>(std::forward<Args>(args)...);
    case LeftOnly:
        return DispatchHashJoinByMode<Wrapper, TResult, LeftOnly>(preservedSide, isGrid,
                                                                  std::forward<Args>(args)...);
    case LeftSemi:
        return DispatchHashJoinByMode<Wrapper, TResult, LeftSemi>(preservedSide, isGrid,
                                                                  std::forward<Args>(args)...);
    case Left:
        return DispatchHashJoinByMode<Wrapper, TResult, Left>(preservedSide, isGrid,
                                                              std::forward<Args>(args)...);
    case Cross:
        return new Wrapper<TPhysicalJoin{Cross}>(std::forward<Args>(args)...);
    default:
        break;
    }
    MKQL_ENSURE(false, unsupportedMessage);
    Y_UNREACHABLE();
}

template <typename TKeyCols, typename TInputTypes>
void ApplyKeyColumnPermutation(TSides<TKeyCols>& keyColumns, TSides<TInputTypes>& inputTypes, int trailingColumns,
                               TDqRenames<ESide>& renames, TSides<TVector<int>>& outColumnPermutation) {
    for (ESide side : EachSide) {
        auto& keyCols = keyColumns.SelectSide(side);
        auto& types = inputTypes.SelectSide(side);
        const int numDataCols = std::ssize(types) - trailingColumns;
        const int numKeys = std::ssize(keyCols);

        bool needsReorder = false;
        for (int i = 0; i < numKeys; ++i) {
            if (static_cast<int>(keyCols[i]) != i) {
                needsReorder = true;
                break;
            }
        }
        if (!needsReorder) {
            continue;
        }

        TVector<int> perm(numDataCols);
        std::iota(perm.begin(), perm.end(), 0);
        for (int i = 0; i < numKeys; ++i) {
            const int keyColumn = static_cast<int>(keyCols[i]);
            MKQL_ENSURE(keyColumn >= 0 && keyColumn < numDataCols,
                        Sprintf("key column index %i on %s side is out of range [0, %i)", keyColumn, AsString(side),
                                numDataCols));
            auto it = std::find(perm.begin() + i, perm.end(), keyColumn);
            MKQL_ENSURE(it != perm.end(),
                        Sprintf("key column index %i on %s side is duplicated or could not be placed", keyColumn,
                                AsString(side)));
            std::swap(perm[i], *it);
        }

        outColumnPermutation.SelectSide(side) = perm;

        using TElem = std::decay_t<decltype(types[0])>;
        const TVector<TElem> orig(types.begin(), types.begin() + numDataCols);
        for (int i = 0; i < numDataCols; ++i) {
            types[i] = orig[perm[i]];
        }

        TVector<int> inv(numDataCols);
        for (int i = 0; i < numDataCols; ++i) {
            inv[perm[i]] = i;
        }
        for (auto& rename : renames) {
            if (rename.Side == side) {
                rename.Index = inv[rename.Index];
            }
        }

        for (int i = 0; i < numKeys; ++i) {
            keyCols[i] = i;
        }
    }
}

inline TSides<TVector<TType*>> ForceOptionalOnNullableSide(const TSides<TVector<TType*>>& itemTypes, EJoinKind kind,
                                                           ESide nullableSide, const TTypeEnvironment& env) {
    TSides<TVector<TType*>> userTypes;
    for (ESide side : EachSide) {
        for (TType* thisType : itemTypes.SelectSide(side)) {
            if (kind == EJoinKind::Left && side == nullableSide && !thisType->IsOptional()) {
                userTypes.SelectSide(side).push_back(TOptionalType::Create(thisType, env));
            } else {
                userTypes.SelectSide(side).push_back(thisType);
            }
        }
    }
    return userTypes;
}

template <TPhysicalJoin Join, typename Converter>
struct TPackedTupleOutputBase : NNonCopyable::TMoveOnly {
    struct Empty {};
    using BuildNullIfNeeded = std::conditional_t<Join.Kind == EJoinKind::Left, TPackResult, Empty>;

    TPackedTupleOutputBase(const TDqRenames<ESide>* renames, TSides<Converter*> converters)
        : Renames_(renames)
        , Converters_(converters)
    {}

    int Columns() const {
        return Renames_->size();
    }

    i64 SizeTuples() const {
        AssertSizeIsSane();
        return Output_.SelectSide(Join.Preserved).NTuples;
    }

    i64 SizeBytes() const {
        return Output_.Build.AllocatedBytes() + Output_.Probe.AllocatedBytes();
    }

    auto MakeConsumeFn() {
        struct ConsumeFn {
            TPackedTupleOutputBase& Self;

            void operator()(TSides<TSingleTuple> tuples) {
                for (ESide side : EachSide) {
                    Self.Output_.SelectSide(side).AppendTuple(tuples.SelectSide(side),
                                                              Self.Converters_.SelectSide(side)->GetTupleLayout());
                }
            }

            void operator()(TSingleTuple tuple) {
                if constexpr (Join.Kind == EJoinKind::Left) {
                    const TSingleTuple null{.PackedData = Self.Nulls_.PackedTuples.data(),
                                            .OverflowBegin = Self.Nulls_.Overflow.data()};
                    TSides<TSingleTuple> row;
                    row.SelectSide(Join.Preserved) = tuple;
                    row.SelectSide(Join.NullSupplying()) = null;
                    (*this)(row);
                } else if constexpr (SemiOrOnlyJoin(Join.Kind)) {
                    Self.Output_.SelectSide(Join.Preserved)
                        .AppendTuple(tuple, Self.Converters_.SelectSide(Join.Preserved)->GetTupleLayout());
                }
            }
        };
        return ConsumeFn{*this};
    }

protected:
    void AssertSizeIsSane() const {
        if constexpr (LeftSemiOrOnly(Join.Kind)) {
            MKQL_ENSURE(Output_.SelectSide(Join.NullSupplying()).NTuples == 0,
                        "Left Only and Left Semi join types shouldn't collect any tuples on the non-output side");
        } else if constexpr (Join.Kind == EJoinKind::Left || Join.Kind == EJoinKind::Inner ||
                             Join.Kind == EJoinKind::Cross) {
            MKQL_ENSURE(Output_.Build.NTuples == Output_.Probe.NTuples,
                        "Inner, Left and Cross join types must collect same amount of tuples from build and probe");
        }
    }

    const TDqRenames<ESide>* Renames_;
    TSides<Converter*> Converters_;
    TSides<TPackResult> Output_;
    BuildNullIfNeeded Nulls_;
};

template <typename JoinType, typename OutputType, typename FlushSink>
EFetchResult RunPackedHashJoinBatch(TComputationContext& ctx, JoinType& join, OutputType& output, FlushSink&& onFlush,
                                    TPackedTuplePairFilter* filter = nullptr) {
    // Bound the batch in bytes, not rows: rows say nothing about memory once overflow columns are fat
    auto outputIsFull = [&]() { return output.SizeBytes() >= static_cast<i64>(MaxBlockSizeInBytes); };
    while (!outputIsFull()) {
        switch (join.MatchRows(ctx, output.MakeConsumeFn(), outputIsFull, filter)) {
        case EFetchResult::Finish:
            if (output.SizeTuples() == 0) {
                return EFetchResult::Finish;
            }
            onFlush(output.Flush());
            return EFetchResult::One;
        case EFetchResult::Yield:
            if constexpr (JoinType::FlushOnYield) {
                if (output.SizeTuples() > 0) {
                    onFlush(output.Flush());
                    return EFetchResult::One;
                }
            }
            return EFetchResult::Yield;
        case EFetchResult::One:
            break;
        default:
            MKQL_ENSURE(false, "unexpected fetch result");
        }
    }
    onFlush(output.Flush());
    return EFetchResult::One;
}

} // namespace NKikimr::NMiniKQL
