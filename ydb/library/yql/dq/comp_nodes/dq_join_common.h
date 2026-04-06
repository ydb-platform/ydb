#pragma once
#include "dq_hash_join_table.h"
#include "dq_block_hash_join_settings.h"
#include <vector>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/spilled_storage.h>
#include <yql/essentials/minikql/comp_nodes/mkql_counters.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>

namespace NKikimr::NMiniKQL {

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

template<typename T>
concept HasArrowField = requires(T t) { t.Arrow; t.Packed; };

template<typename T>
concept HasIndexedConsume = requires(T t) {
    { t.CurrentBucketIdx } -> std::convertible_to<int>;
    { t.CurrentProbeLocalIdx } -> std::convertible_to<ui32>;
};

TPackResult GetPage(TFuturePage&& future);

using ProbeSpillingPage = std::optional<TPackResult>;

struct TSpilledBucket : public TSides<TMKQLVector<ISpiller::TKey>> {};

using PairOfSpilledBuckets = TSides<TBucket>;

bool AllFuturesReady(const auto& futures) {
    return std::ranges::all_of(futures, [&](const auto& future) { return future.IsReady(); });
}

struct TFutureTableData {
    TMKQLVector<TFuturePage> Futures;
    NThreading::TFuture<void> All;
};

struct TTableAndSomeData {
    NJoinTable::TNeumannJoinTable Table;
    TMKQLDeque<TFuturePage> Futures;
    std::optional<TPackResult> CurrentProbePack;
    ui32 ProbeResumeIndex = 0;
};

namespace NJoinPackedTuples {
template <typename Source> class TInMemoryHashJoin {
  public:
    using TTable = NJoinTable::TNeumannJoinTable;

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
            auto var = Sources_.Build.FetchRow();
            switch (AsStatus(var)) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.BuildWith(Flatten(std::move(BuildChunks_)));
                break;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                auto& payload = GetPayload(var);
                if constexpr (HasArrowField<std::decay_t<decltype(payload)>>) {
                    BuildChunks_.push_back(std::move(payload.Packed));
                } else {
                    BuildChunks_.push_back(std::move(payload));
                }
                break;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (Table_.Empty()) {
            return EFetchResult::Finish;
        }

        if (FetchedPack_.has_value()) {
            ui32 idx = 0;
            for (TSingleTuple probeTuple : *FetchedPack_) {
                if (idx++ < ResumeIndex_) {
                    continue;
                }
                Table_.Lookup(probeTuple, [&](TSingleTuple buildTuple) {
                    consumeOneOrTwoTuples(TSides<TSingleTuple>{.Build = buildTuple, .Probe = probeTuple});
                });
                if (isFull()) {
                    ResumeIndex_ = idx;
                    return EFetchResult::One;
                }
            }
            FetchedPack_ = std::nullopt;
            ResumeIndex_ = 0;
        }

        if (!Sources_.Probe.Finished()) {
            auto var = Sources_.Probe.FetchRow();
            const NKikimr::NMiniKQL::EFetchResult resEnum = AsResult(var);

            if (resEnum == EFetchResult::One) {
                auto& payload = GetPayload(var);
                if constexpr (HasArrowField<std::decay_t<decltype(payload)>>) {
                    FetchedPack_ = std::move(payload.Packed);
                } else {
                    FetchedPack_ = std::move(payload);
                }
                ResumeIndex_ = 0;
                ui32 idx = 0;
                for (TSingleTuple probeTuple : *FetchedPack_) {
                    idx++;
                    Table_.Lookup(probeTuple, [&](TSingleTuple buildTuple) {
                        consumeOneOrTwoTuples(TSides<TSingleTuple>{.Build = buildTuple, .Probe = probeTuple});
                    });
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
};

template <typename Source, TSpillerSettings Settings, EJoinKind Kind> class THybridHashJoin {
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

    using Self = THybridHashJoin<Source, Settings, Kind>;

  public:
    using TTable = NJoinTable::TNeumannJoinTable;

    struct Init {};

    struct FetchingBuild {
        FetchingBuild(Self& self)
            : Build(std::move(self.Sources_).Build())
            , Spiller(self.Spiller_, self.Layouts_.Build)
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
            bool trackUsed = (Kind == EJoinKind::Left) && self.Settings_.LeftIsBuild();
            for(int index = 0; index < std::ssize(Spiller.GetBuckets()); ++index) {
                ProbeState.Buckets.push_back(TTable{self.Layouts_.Build, trackUsed});
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

            self.Logger_.LogDebug(Sprintf("Probing stage started, in memory buckets: %i, spilled buckets: %i",
                                          inMemoryBuckets, spilledBuckets));
        }

        Source Probe;
        TProbeSpiller<Settings> Spiller;
        std::optional<TPackResult> FetchedPack;
        ui32 ResumeIndex = 0;
    };

    using DumpedBuckets = std::unordered_map<int, TSpilledBucket>;

    struct DumpRestOfPages {
        DumpRestOfPages(Self& self, std::unordered_map<int, TSpilledBucket>&& base,
                        TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>>&& futures)
            : AlreadyDumped(std::move(base))
            , Futures(std::move(futures))
        {
            NThreading::TWaitGroup<NThreading::TWaitPolicy::TAll> wg;
            for (auto& future : Futures) {
                wg.Add(future.Val);
            }
            All = std::move(wg).Finish();
            self.Logger_.LogDebug(Sprintf("DumpRestOfPages stage started, page count: %i", Futures.size()));
        }

        DumpedBuckets AlreadyDumped;
        TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>> Futures;
        NThreading::TFuture<void> All;
    };

    struct PairAndMetadata {
        TSpilledBucket Buckets;
        int BucketIndex;
        std::variant<TFutureTableData, TTableAndSomeData> Table = TFutureTableData{};
    };

    struct JoinPairsOfPartitions {
        JoinPairsOfPartitions(Self& self, std::unordered_map<int, TSpilledBucket>&& pairs)
            : Pairs(std::move(pairs))
        {
            self.Logger_.LogDebug(Sprintf("JoinPairsOfPartitions stage started, partitions count: %i", pairs.size()));
        }

        std::unordered_map<int, TSpilledBucket> Pairs;
        std::optional<PairAndMetadata> SelectedPair;
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
                    TSides<const NPackedTuple::TTupleLayout*> layouts, TBlockHashJoinSettings settings = {})
        : Logger_(ctx, componentName)
        , Layouts_(layouts)
        , Spiller_(ctx.SpillerFactory ? ctx.SpillerFactory->CreateSpiller() : nullptr)
        , Sources_(std::move(sources))
        , Settings_(settings)
        , BucketRowGlobalIndices_(Settings.Buckets)
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


    const TMKQLVector<TMKQLVector<ui32>>& GetBucketRowGlobalIndices() const {
        return BucketRowGlobalIndices_;
    }

    TVector<std::shared_ptr<arrow::Array>>& GetConcatenatedBuildCols() {
        return ConcatenatedBuildCols_;
    }

    TVector<arrow::Datum>& GetCurrentProbeArrow() {
        return CurrentProbeArrow_;
    }

    EFetchResult MatchRows([[maybe_unused]] TComputationContext& ctx, auto consume, auto isFull) {
        auto notEnoughMemory = [hasSpiller = !!Spiller_] {
            return hasSpiller && TlsAllocState->IsMemoryYellowZoneEnabled();
        };
        auto consumeSingle = [&](TSingleTuple tuple) {
            if constexpr (HasIndexedConsume<decltype(consume)>) {
                consume.FallbackConsumeSingle(tuple);
            } else if constexpr (std::invocable<decltype(consume), TSingleTuple>) {
                consume(tuple);
            }
        };
        auto lookupToTableFallback = [&](TTable& table, TSingleTuple tuple) {
            bool found = false;
            if constexpr (Kind == EJoinKind::Left) {
                if (Settings_.LeftIsBuild()) {
                    table.Lookup(tuple, [&](TSingleTuple tableMatch) {
                        found = true;
                        consume(TSides<TSingleTuple>{.Build = tableMatch, .Probe = tuple});
                    });
                } else {
                    table.Lookup(tuple, [&](TSingleTuple tableMatch) {
                        found = true;
                        consume(TSides<TSingleTuple>{.Build = tableMatch, .Probe = tuple});
                    });
                    if (!found) {
                        consumeSingle(tuple);
                    }
                }
            } else {
                table.Lookup(tuple, [&](TSingleTuple tableMatch) {
                    found = true;
                    if constexpr (Kind == EJoinKind::Inner) {
                        consume(TSides<TSingleTuple>{.Build = tableMatch, .Probe = tuple});
                    }
                });
                if constexpr (Kind == EJoinKind::LeftOnly) {
                    if (!found) {
                        consumeSingle(tuple);
                    }
                }
                if constexpr(Kind == EJoinKind::LeftSemi) {
                    if (found) {
                        consumeSingle(tuple);
                    }
                }
            }
        };
        [[maybe_unused]] auto lookupToTableIndexed = [&](TTable& table, TSingleTuple tuple, int bucketIdx, ui32 probeLocalIdx, auto& indexedConsume) {
            bool found = false;
            table.LookupWithIndex(tuple, [&](ui32 buildPosInBucket, TSingleTuple buildTuple) {
                found = true;
                if constexpr (Kind == EJoinKind::Inner || Kind == EJoinKind::Left) {
                    indexedConsume.CurrentBucketIdx = bucketIdx;
                    indexedConsume.CurrentProbeLocalIdx = probeLocalIdx;
                    indexedConsume(buildPosInBucket, buildTuple);
                }
            });
            if constexpr (Kind == EJoinKind::Left) {
                if (!Settings_.LeftIsBuild() && !found) {
                    indexedConsume.CurrentProbeLocalIdx = probeLocalIdx;
                    indexedConsume(tuple);
                }
            }
            if constexpr (Kind == EJoinKind::LeftOnly) {
                if (!found) {
                    indexedConsume.CurrentProbeLocalIdx = probeLocalIdx;
                    indexedConsume(tuple);
                }
            }
            if constexpr (Kind == EJoinKind::LeftSemi) {
                if (found) {
                    indexedConsume.CurrentProbeLocalIdx = probeLocalIdx;
                    indexedConsume(tuple);
                }
            }
        };
        if (std::get_if<Init>(&State_)) {
            State_ = FetchingBuild{*this};
        } else if (auto* s = std::get_if<FetchingBuild>(&State_)) {
            FetchingBuild& state = *s;
            if (!state.Pack.has_value()) {
                auto var = state.Build.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok) {
                    auto& block = GetPayload(var);
                    if constexpr (HasArrowField<std::decay_t<decltype(block)>>) {
                        state.Pack = std::move(block.Packed);
                        BuildArrowBlocks_.push_back(std::move(block.Arrow));
                        BuildBlockCumRows_.push_back(BuildGlobalRowCount_);
                    } else {
                        state.Pack = std::move(block);
                    }
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
                    int bucketIndex = Settings.BucketIndex(tuple);
                    state.Spiller.AddRow(tuple);
                    BucketRowGlobalIndices_[bucketIndex].push_back(static_cast<ui32>(BuildGlobalRowCount_));
                    BuildGlobalRowCount_++;
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
                ConcatenateBuildArrow();
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
                auto var = state.Probe.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok) {
                    auto& block = GetPayload(var);
                    if constexpr (HasArrowField<std::decay_t<decltype(block)>>) {
                        state.FetchedPack = std::move(block.Packed);
                        CurrentProbeArrow_ = std::move(block.Arrow);
                    } else {
                        state.FetchedPack = std::move(block);
                    }
                } else {
                    MKQL_ENSURE(status == NYql::NUdf::EFetchStatus::Finish, "unexpected enum");
                    if constexpr (Kind == EJoinKind::Left) {
                        if (Settings_.LeftIsBuild()) {
                            if constexpr (HasIndexedConsume<decltype(consume)>) {
                                EmitUnmatchedFromInMemoryBucketsIndexed(state.Spiller, consume);
                            } else {
                                EmitUnmatchedFromInMemoryBuckets(state.Spiller, consume);
                            }
                        }
                    }
                    std::unordered_map<int, TSpilledBucket> alreadyDumped;
                    TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>> futures;
                    for (int index = 0; index < std::ssize(state.Spiller.GetState().Buckets); ++index) {
                        if (state.Spiller.IsBucketSpilled(index)) {
                            TSides<TBucket>& thisPair = *std::get_if<TSides<TBucket>>(&state.Spiller.GetState().Buckets[index]);
                            for(ESide side: EachSide) {
                                TBucket& thisBucket = thisPair.SelectSide(side);
                                thisBucket.DetatchBuildingPage();
                                for( TPackResult& page: thisBucket.DetatchPages()){

                                    futures.push_back(TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>{
                                        .Val = SpillPage(*Spiller_, std::move(page)), .Side = side,
                                        .BucketIndex = index});
                                }
                                alreadyDumped[index].SelectSide(side) = std::move(*thisBucket.SpilledPages);
                                thisBucket.SpilledPages = std::nullopt;
                            }
                        }
                    }
                    for (auto& page : state.Spiller.GetState().InMemoryPages) {
                        futures.push_back(TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>{
                            .Val = SpillPage(*Spiller_, std::move(page.Val)), .Side = page.Side,
                            .BucketIndex = page.BucketIndex});
                    }
                    state.Spiller.GetState().InMemoryPages.clear();
                    state.Spiller.GetState().InMemoryPages.shrink_to_fit();
                    BuildArrowBlocks_.clear();
                    BuildBlockCumRows_.clear();
                    if (futures.empty()) {
                        if (alreadyDumped.empty()) {
                            State_ = Finish{};
                        } else {
                            State_ = JoinPairsOfPartitions{*this, std::move(alreadyDumped)};
                        }
                    } else {

                        MKQL_ENSURE(!alreadyDumped.empty(), "0 dumped buckets but have some parts in memory?");
                        State_ = DumpRestOfPages{*this, std::move(alreadyDumped), std::move(futures)};
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
                    int bucketIndex = Settings.BucketIndex(tuple);
                    bool thisBucketSpilled = state.Spiller.IsBucketSpilled(bucketIndex);
                    if (thisBucketSpilled) {
                        state.Spiller.AddRow({.Val = tuple, .Side = ESide::Probe, .BucketIndex = bucketIndex});
                    } else {
                        TTable* thisTable = std::get_if<TTable>(&state.Spiller.GetState().Buckets[bucketIndex]);
                        MKQL_ENSURE(thisTable, "sanity check");
                        if constexpr (HasIndexedConsume<decltype(consume)>) {
                            lookupToTableIndexed(*thisTable, tuple, bucketIndex, idx - 1, consume);
                        } else {
                            lookupToTableFallback(*thisTable, tuple);
                        }
                    }
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
                for (auto& future : state.Futures) {
                    auto it = state.AlreadyDumped.find(future.BucketIndex);
                    MKQL_ENSURE(it != state.AlreadyDumped.end(), "bucket with this index is processed already");
                    it->second.SelectSide(future.Side).push_back(ExtractReadyFuture(std::move(future.Val)));
                }
                State_ = JoinPairsOfPartitions{*this, std::move(state.AlreadyDumped)};

            } else {
                return WaitWhileSpilling();
            }
        } else if (auto* s = std::get_if<JoinPairsOfPartitions>(&State_)) {
            JoinPairsOfPartitions& state = *s;
            if (!state.SelectedPair.has_value()) {
                std::optional bucket = GetFrontOrNull(state.Pairs);
                if (bucket.has_value()) {
                    state.SelectedPair =
                        PairAndMetadata{.Buckets = std::move(bucket->second), .BucketIndex = bucket->first};
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
                if (auto* tdata = std::get_if<TFutureTableData>(&state.SelectedPair->Table)) {
                    if (tdata->All.IsReady()) {
                        TMKQLVector<TPackResult> vec;
                        for (auto& future : tdata->Futures) {
                            vec.push_back(GetPage(std::move(future), ESide::Build));
                        }
                        bool trackUsed = (Kind == EJoinKind::Left) && Settings_.LeftIsBuild();
                        NJoinTable::TNeumannJoinTable table{Layouts_.Build, trackUsed};
                        table.BuildWith(Flatten(std::move(vec)));
                        state.SelectedPair->Table = TTableAndSomeData{.Table = std::move(table), .Futures = {}};
                    } else {
                        return WaitWhileSpilling();
                    }
                } else {
                    auto* table = std::get_if<TTableAndSomeData>(&state.SelectedPair->Table);
                    MKQL_ENSURE(table, "sanity check");
                    constexpr int MinFuturesInBuffer = 10;
                    while (table->Futures.size() < MinFuturesInBuffer && !currentProbe.empty()) {
                        table->Futures.push_back(Spiller_->Extract(*GetBackOrNull(currentProbe)));
                    }
                    if (table->CurrentProbePack.has_value()) {
                        ui32 idx = 0;
                        for (TSingleTuple probeTuple : *table->CurrentProbePack) {
                            if (idx++ < table->ProbeResumeIndex) {
                                continue;
                            }
                            lookupToTableFallback(table->Table, probeTuple);
                            if (isFull()) {
                                table->ProbeResumeIndex = idx;
                                return EFetchResult::One;
                            }
                        }
                        table->CurrentProbePack = std::nullopt;
                        table->ProbeResumeIndex = 0;
                    } else if (table->Futures.empty()) {
                        MKQL_ENSURE(currentProbe.empty(), "sanity check");
                        if constexpr (Kind == EJoinKind::Left) {
                            if (Settings_.LeftIsBuild()) {
                                if constexpr (HasIndexedConsume<decltype(consume)>) {
                                    table->Table.ForEachUnused([&](TSingleTuple buildTuple) {
                                        consume.FallbackConsumeUnmatched(buildTuple);
                                    });
                                } else {
                                    table->Table.ForEachUnused(consume);
                                }
                            }
                        }
                        state.SelectedPair = std::nullopt;
                    } else {
                        if (table->Futures.front().IsReady()) {
                            table->CurrentProbePack = GetPage(*GetFrontOrNull(table->Futures), ESide::Probe);
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

    template <typename TSpiller, typename F>
    void EmitUnmatchedFromInMemoryBuckets(TSpiller& spiller, F&& consume) {
        for (int index = 0; index < std::ssize(spiller.GetState().Buckets); ++index) {
            if (spiller.IsBucketSpilled(index)) {
                continue;
            }
            TTable* table = std::get_if<TTable>(&spiller.GetState().Buckets[index]);
            if (table && !table->Empty()) {
                table->ForEachUnused(std::forward<F>(consume));
            }
        }
    }

    template <typename TSpiller, typename F>
    void EmitUnmatchedFromInMemoryBucketsIndexed(TSpiller& spiller, F& consume) {
        for (int index = 0; index < std::ssize(spiller.GetState().Buckets); ++index) {
            if (spiller.IsBucketSpilled(index)) {
                continue;
            }
            TTable* table = std::get_if<TTable>(&spiller.GetState().Buckets[index]);
            if (table && !table->Empty()) {
                consume.CurrentBucketIdx = index;
                table->ForEachUnusedWithIndex([&](ui32 posInBucket, TSingleTuple) {
                    consume.EmitUnmatchedBuild(posInBucket);
                });
            }
        }
    }

  private:
    void ConcatenateBuildArrow() {
        if (BuildArrowBlocks_.empty()) {
            return;
        }
        int numCols = BuildArrowBlocks_[0].size();
        ConcatenatedBuildCols_.resize(numCols);
        for (int col = 0; col < numCols; col++) {
            std::vector<std::shared_ptr<arrow::Array>> chunks;
            chunks.reserve(BuildArrowBlocks_.size());
            for (auto& block : BuildArrowBlocks_) {
                chunks.push_back(block[col].make_array());
            }
            auto concat = arrow::Concatenate(chunks);
            MKQL_ENSURE(concat.ok(), concat.status().ToString().c_str());
            ConcatenatedBuildCols_[col] = *concat;
        }
    }

    const Logger Logger_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    ISpiller::TPtr Spiller_;
    Sources Sources_;
    TBlockHashJoinSettings Settings_;
    std::variant<Init, FetchingBuild, BuildingInMemoryTable, Probing, DumpRestOfPages, JoinPairsOfPartitions, Finish>
        State_ = Init{};

    TMKQLVector<TVector<arrow::Datum>> BuildArrowBlocks_;
    TMKQLVector<i64> BuildBlockCumRows_;
    i64 BuildGlobalRowCount_ = 0;
    TMKQLVector<TMKQLVector<ui32>> BucketRowGlobalIndices_;
    TVector<std::shared_ptr<arrow::Array>> ConcatenatedBuildCols_;
    TVector<arrow::Datum> CurrentProbeArrow_;
};
} // namespace NJoinPackedTuples

} // namespace NKikimr::NMiniKQL
