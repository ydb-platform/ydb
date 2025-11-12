#pragma once
#include "dq_hash_join_table.h"
#include <vector>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/spilled_storage.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

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

IBlockLayoutConverter::TPackResult Flatten(TMKQLVector<TPackResult> tuples);

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

bool AllInMemory(const TBuckets& buckets);

enum class EIsInMemory: bool {
    Spilled,
    InMemory,
};

template<typename T> 
[[nodiscard]] T ExtractReadyFuture(NThreading::TFuture<T>&& future) {
    MKQL_ENSURE(future.IsReady(), "no blocking wait in comp nodes");
    return future.ExtractValueSync();
}

TPackResult GetPage(TFuturePage&& future);

using ProbeSpillingPage = std::optional<TPackResult>;

struct TSpilledBucket: public TSides<TMKQLVector<ISpiller::TKey>> {};

using PairOfSpilledBuckets = TSides<TBucket>;

bool AllFuturesReady(const auto& futures) {
    return std::ranges::all_of(futures,  [&](const auto& future){return future.IsReady(); });
}

struct TFutureTableData {
    TMKQLVector<TFuturePage> Futures;
    NThreading::TFuture<void> All;
};

struct TTableAndSomeData {
    NJoinTable::TNeumannJoinTable Table;
    TMKQLDeque<TFuturePage> Futures;
};


struct TBucketPairsAndMetadata {
    TSpilledBucket Buckets;
    int BucketIndex;
    // TMKQLVector<TFuturePage> PagesFromDisk;
    // i64 MaxInMemoryPages;
    std::variant<TFutureTableData, TTableAndSomeData> Table = TFutureTableData{};
    // i64 PagesInMemory() {
    //     return std::ssize(PagesFromDisk);
    // }
};

struct TPageFutures {
    std::unordered_map<int, TSpilledBucket> AlreadyDumped;
    TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>> Futures;
    NThreading::TFuture<void> All;
};


template <typename Source, TSpillerSettings Settings> class TJoinPackedTuples {
public:
    using TTable = NJoinTable::TNeumannJoinTable;

    TJoinPackedTuples(TSides<Source> sources, NUdf::TLoggerPtr logger, TString componentName,
                      TSides<const NPackedTuple::TTupleLayout*> layouts, TComputationContext& ctx)
        : Logger_(logger)
        , LogComponent_(logger->RegisterComponent(componentName))
        , Layouts_(layouts)
        , Sources_(std::move(sources))
        , Spiller_(ctx.SpillerFactory->CreateSpiller())
        // , BuildSpilling_()
        // , ProbeSpilling_(Spiller_)
        // , Table_(Layouts_.Build)
    {}

    struct Probing {
        NJoinTable::TNeumannJoinTable Table;
        Source Probe;
    };

    IBlockLayoutConverter::TPackResult Flatten(TMKQLVector<IBlockLayoutConverter::TPackResult>&& tuples) {
        IBlockLayoutConverter::TPackResult flattened;
        flattened.NTuples = std::accumulate(tuples.begin(), tuples.end(), i64{0},
                                            [](i64 summ, const auto& packRes) { return summ += packRes.NTuples; });

        i64 totalTuplesSize = std::accumulate(tuples.begin(), tuples.end(), i64{0}, [](i64 summ, const auto& packRes) {
            return summ += std::ssize(packRes.PackedTuples);
        });
        flattened.PackedTuples.reserve(totalTuplesSize);

        i64 totaOverflowlSize =
            std::accumulate(tuples.begin(), tuples.end(), i64{0},
                            [](i64 summ, const auto& packRes) { return summ += std::ssize(packRes.Overflow); });
        flattened.Overflow.reserve(totaOverflowlSize);

        int tupleSize = Layouts_.Build->TotalRowSize;
        for (const TPackResult& tupleBatch : tuples) {
            Layouts_.Build->Concat(flattened.PackedTuples, flattened.Overflow,
                                   std::ssize(flattened.PackedTuples) / tupleSize, tupleBatch.PackedTuples.data(),
                                   tupleBatch.Overflow.data(), tupleBatch.PackedTuples.size() / tupleSize,
                                   tupleBatch.Overflow.size());
        }
        return flattened;
    }

    EFetchResult WaitWhileSpilling() {
        Cout << "spill" << Endl;
        return EFetchResult::Yield;
    }

    EFetchResult MatchRows([[maybe_unused]] TComputationContext& ctx,
                           std::invocable<TSides<TSingleTuple>> auto consumePairOfTuples) {
        static auto fetchSpillingCondition = []{
            return !MemoryPercentIsFree(30);
        }; 
        // int  = 0;
        auto selectStage = [stagesCalled = 0] () mutable{
            stagesCalled++;
            MKQL_ENSURE(stagesCalled == 1, "selected 2 stages at time?");
        };
        Cout << "MatchRows ";
        if (!Sources_.Build.Finished()) { // fetching build side, todo(becalm): can do better(FetchedPack dont need to be optional)
            selectStage();
            Cout << "Fetching Build" << Endl;
            if (!FetchedPack_.has_value()) {
                FetchResult<TPackResult> var = Sources_.Build.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok) {
                    auto& packResult = std::get<One<IBlockLayoutConverter::TPackResult>>(var);
                    FetchedPack_ = std::move(packResult.Data);   
                } else {
                    MKQL_ENSURE(status == NYql::NUdf::EFetchStatus::Finish, "unhandled status");
                    BuildingInMemoryTable_.emplace(Layouts_.Build);
                    MKQL_ENSURE(Sources_.Build.Finished(), "sanity check");
                }
            } else {
                ESpillResult res = BuildSpilling_.SpillWhile(fetchSpillingCondition);
                switch (res) {
                case Spilling:
                    return WaitWhileSpilling();
                case FinishedSpilling:
                    break;
                case DontHavePages:
                    MKQL_ENSURE(false, "spilling in smaller pages is not implemented"); // we can not spill much and do not have memory. spilling smaller chunks is not implemented currently.
                break;
                }
                FetchedPack_->ForEachTuple([&](TSingleTuple tuple) {
                    BuildSpilling_.AddRow(tuple);
                });
                FetchedPack_ = std::nullopt; // todo(becalm):i can fill it here, not in other if brach.
            }
        } else if (BuildingInMemoryTable_.has_value()) { // building in memory table
            selectStage();
            Cout << "making ht" << Endl;
            TBuckets& buckets = BuildSpilling_.GetBuckets();
            const i64 memoryWithoutDummyStorage = TlsAllocState->GetAllocated();
            auto extraMemoryForBuild = [&]{    
                int64_t flattenMemory = 0;
                i64 inMemoryTuples = 0;
                for(const auto& bucket: buckets) {
                    if (!bucket.IsSpilled()) {
                        for(auto& page: bucket.InMemoryPages) {
                            flattenMemory += page.AllocatedBytes();
                            inMemoryTuples += page.NTuples;
                        }
                        flattenMemory += bucket.BuildingPage.AllocatedBytes();
                        inMemoryTuples += bucket.BuildingPage.NTuples;
                    }
                }

                return std::max(BuildingInMemoryTable_->RequiredMemoryForBuild(inMemoryTuples), flattenMemory);
            };
            auto peakMemoryDuringBuild = [&] () -> ui64 {
                return memoryWithoutDummyStorage + extraMemoryForBuild();
            };

            {
                ui64 optimisticPeak = peakMemoryDuringBuild();
                TMKQLVector<TMKQLVector<std::byte>> dummyStorage;
                while( optimisticPeak > TlsAllocState->GetLimit() && !TlsAllocState->GetMaximumLimitValueReached()) {
                    int allocSize = std::min(static_cast<i64>(TlsAllocState->GetLimit()*0.1), i64{100*1<<20});
                    Cout << std::format("peak:{}, limit:{}, next: {}\n",optimisticPeak, TlsAllocState->GetLimit(), allocSize);
                    dummyStorage.emplace_back();
                    dummyStorage.back().resize(allocSize, static_cast<std::byte>(allocSize&1));
                }
            }
            ESpillResult res = BuildSpilling_.SpillWhile([&]{ return peakMemoryDuringBuild() > TlsAllocState->GetLimit(); } );
            switch (res) {
            case Spilling:
                return WaitWhileSpilling();
            case FinishedSpilling:
                break;
            case DontHavePages:
                MKQL_ENSURE(false, "spilling in smaller pages is not implemented"); // we can not spill much and do not have memory. spilling smaller chunks is not implemented currently.
              break;
            }
            MKQL_ENSURE(peakMemoryDuringBuild() < TlsAllocState->GetLimit(), "sanity check");
            MKQL_ENSURE(FreeMemory() > extraMemoryForBuild() , Sprintf("%i > %i, extra allocations because of dummyStorage?", FreeMemory(), extraMemoryForBuild()));
            // following for cycle will allocate and thus BuildWith may fail with exception, but allocations are small enough to not think about them for now.

            // TMKQLVector<SpilledBucket> spilledBuckets;
            TMKQLVector<TPackResult> inMemoryPages;

            for(int index = 0; index < std::ssize(buckets); ++index) {
                TBucket& bucket = buckets[index];
                if (!bucket.IsSpilled()) {
                    bucket.DetatchBuildingPage();
                    for(auto& page: bucket.InMemoryPages) {
                        inMemoryPages.push_back(std::move(page));
                    }
                    bucket.InMemoryPages.clear();
                } else {
                    bucket.DetatchBuildingPage();
                    ProbeSpilling_.GetState().SpilledBuckets_[index].Build = std::move(bucket);
                }
                MKQL_ENSURE(bucket.Empty() , "state left in buckets?");
            }
            buckets.clear();
            buckets.shrink_to_fit();
            if (!inMemoryPages.empty()) {
                BuildingInMemoryTable_->BuildWith(Flatten(std::move(inMemoryPages))); 
                Probing_ = Probing{.Table = std::move(*BuildingInMemoryTable_), .Probe = std::move(Sources_.Probe)};
            }
            BuildingInMemoryTable_ = std::nullopt;

        } else if (Probing_.has_value()) {
            selectStage();
            Cout << "probing" << Endl;
            if (!FetchedPack_.has_value()) { // same as build side
                FetchResult<TPackResult> var = Probing_->Probe.FetchRow();
                NYql::NUdf::EFetchStatus status = AsStatus(var);
                if (status == NYql::NUdf::EFetchStatus::Yield ) {
                    return EFetchResult::Yield;
                } else if (status == NYql::NUdf::EFetchStatus::Ok ) {
                    auto& packResult = std::get<One<IBlockLayoutConverter::TPackResult>>(var);
                    FetchedPack_ = std::move(packResult.Data);
                } else { 
                    MKQL_ENSURE(status == NYql::NUdf::EFetchStatus::Finish, "unexpected enum");
                    // SpilledPairs_ = ProbeSpilling_.GetSpilledBuckets();
                    DumpInProgress_.emplace();
                    // auto map
                    for(int index = 0; index < std::ssize(ProbeSpilling_.GetState().SpilledBuckets_); ++index ) {
                        TSides<TBucket>& thisPair = ProbeSpilling_.GetState().SpilledBuckets_[index];
                        if (IsBucketSpilled(thisPair)) {
                            ForEachSide([&](ESide side) {
                                TBucket& thisBucket = thisPair.SelectSide(side);
                                thisBucket.DetatchBuildingPage();
                                for( TPackResult& page: thisBucket.InMemoryPages ) {
                                    DumpInProgress_->Futures.push_back(TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>{ .Val = SpillPage(*Spiller_, std::move(page)), .Side = side, .BucketIndex = index});
                                }
                                DumpInProgress_->AlreadyDumped[index].SelectSide(side) = std::move(*thisBucket.SpilledPages);
                                thisBucket.SpilledPages = std::nullopt;
                            });
                        }
                    }
                    for(auto& page: ProbeSpilling_.GetState().InMemoryPages_) {
                        DumpInProgress_->Futures.push_back(TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>{.Val = SpillPage(*Spiller_, std::move(page.Val)), .Side = page.Side, .BucketIndex = page.BucketIndex});
                    }
                    ProbeSpilling_.GetState().InMemoryPages_.clear();
                    ProbeSpilling_.GetState().InMemoryPages_.shrink_to_fit();
                    // BuildingInMemoryTable_ = std::nullopt;
                    Probing_ = std::nullopt;
                    if (DumpInProgress_->Futures.empty()) {
                        if (DumpInProgress_->AlreadyDumped.empty()) {
                            DumpInProgress_ = std::nullopt; // Finish
                        } else {
                            ProcessingSpilledData_ = std::move(DumpInProgress_->AlreadyDumped);
                            DumpInProgress_ = std::nullopt;
                        }
                    } else {
                        MKQL_ENSURE(!DumpInProgress_->AlreadyDumped.empty(), "0 dumped buckets but have some parts in memory?");
                        NThreading::TWaitGroup<NThreading::TWaitPolicy::TAll> wg;
                        for(auto& future: DumpInProgress_->Futures) {
                            wg.Add(future.Val);
                        }
                        DumpInProgress_->All = std::move(wg).Finish();
                    }
                }
            } else {
                switch(ProbeSpilling_.SpillWhile(fetchSpillingCondition)){
                case Spilling:
                    return WaitWhileSpilling();
                case FinishedSpilling:
                    break;
                case DontHavePages: {
                    MKQL_ENSURE(false, "dont have any pages to spill, spilling in smaller pages is not implemented"); // we can not spill much and do not have memory. spilling smaller chunks is not implemented currently.
                }
                default:
                    MKQL_ENSURE(false, "unhanded ESpillResult case");    
                }
                TPackResult& packResult = *FetchedPack_;
                packResult.ForEachTuple([&] (TSingleTuple tuple) {
                    int bucketIndex = Settings.BucketIndex(tuple);
                    
                    bool thisBucketSpilled = ProbeSpilling_.GetState().SpilledBuckets_[bucketIndex].Build.IsSpilled();
                    if (thisBucketSpilled) {
                        ProbeSpilling_.AddRow({.Val = tuple, .Side = ESide::Probe, .BucketIndex = bucketIndex});
                    } else {
                        Probing_->Table.Lookup(tuple, [&](TSingleTuple tableMatch) {
                            consumePairOfTuples(TSides<TSingleTuple>{.Build = tableMatch, .Probe = tuple});
                        });
                    }
                });
                FetchedPack_ = std::nullopt;
            }
        } else if (DumpInProgress_.has_value()) {
            selectStage();
            Cout << "dumping rest of data t disk" << Endl;
            if (DumpInProgress_->All.IsReady()) {
                for(auto& future: DumpInProgress_->Futures) {
                    auto it = DumpInProgress_->AlreadyDumped.find(future.BucketIndex);
                    MKQL_ENSURE(it != DumpInProgress_->AlreadyDumped.end(), "bucket with this index is processed already");
                    it->second.SelectSide(future.Side).push_back(ExtractReadyFuture(std::move(future.Val)));
                }
                ProcessingSpilledData_.emplace();
                ProcessingSpilledData_ = std::move(DumpInProgress_->AlreadyDumped);
                DumpInProgress_ = std::nullopt;
            } else {
                return WaitWhileSpilling();
            }
        } else if (ProcessingSpilledData_.has_value()) { 
            selectStage();
            Cout << "processing pairs of buckets" << Endl;

            if (!CurrentBucket_.has_value()) {
                std::optional bucket = GetFrontOrNull(*ProcessingSpilledData_);
                if (bucket.has_value()) {
                    CurrentBucket_ = TBucketPairsAndMetadata{.Buckets = std::move(bucket->second), .BucketIndex = bucket->first};
                    TFutureTableData data;
                    for(ISpiller::TKey key: CurrentBucket_->Buckets.Build) {
                        data.Futures.push_back(Spiller_->Extract(key));
                    }
                    data.All = NThreading::WaitAll(data.Futures);
                    CurrentBucket_->Table = std::move(data);
                } else {
                    ProcessingSpilledData_ = std::nullopt;   
                }
            } else {
                TMKQLVector<ISpiller::TKey>& currentProbe = CurrentBucket_->Buckets.Probe;
                if ( auto* tdata = std::get_if<TFutureTableData>(&CurrentBucket_->Table)) {
                    if (tdata->All.IsReady()) {
                        TMKQLVector<TPackResult> vec;
                        for(auto& future: tdata->Futures) {
                            vec.push_back(GetPage(std::move(future)));
                        }
                        NJoinTable::TNeumannJoinTable table{Layouts_.Build};
                        table.BuildWith(Flatten(std::move(vec)));
                        CurrentBucket_->Table = TTableAndSomeData{.Table = std::move(table), .Futures = {}};
                    } else {
                        return WaitWhileSpilling();
                    }
                } else {
                    auto* table = std::get_if<TTableAndSomeData>(&CurrentBucket_->Table);
                    MKQL_ENSURE(table, "sanity check");
                    constexpr int MinFuturesInBuffer = 10;
                    while (table->Futures.size() < MinFuturesInBuffer && !currentProbe.empty()) {
                        table->Futures.push_back(Spiller_->Extract(*GetBackOrNull(currentProbe)));
                    }
                    if (table->Futures.empty()){
                        MKQL_ENSURE(currentProbe.empty(), "sanity check");
                        CurrentBucket_ = std::nullopt;
                    } else {
                        if (table->Futures.front().IsReady()) {
                            TPackResult pack = GetPage(*GetFrontOrNull(table->Futures));
                            pack.ForEachTuple([&](TSingleTuple probeTuple) {
                                table->Table.Lookup(probeTuple, [&](TSingleTuple buildTuple){
                                    consumePairOfTuples({.Build = buildTuple, .Probe = probeTuple});
                                });
                            });
                        } else {
                            return WaitWhileSpilling();
                        }
                    }
                }

            }
        } else { 
            Cout << "Finished" << Endl;
            return EFetchResult::Finish; }
        
        return EFetchResult::One;
    }

private:
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;

    TSides<Source> Sources_;
    std::optional<TPackResult> FetchedPack_;
    ISpiller::TPtr Spiller_;
    TBucketsSpiller<Settings> BuildSpilling_{Spiller_, Layouts_.Build};
    TSides<TMKQLVector<TMKQLVector<ISpiller::TKey>>> SpilledBuckets_;
    TSimpleSpiller<Settings> ProbeSpilling_{Spiller_, Layouts_.Probe};
    // TMKQLDeque<PageForSpilling> Pages_;
    // bool FirstCall = true;
    std::optional<TBucketPairsAndMetadata> CurrentBucket_;
    std::optional<Probing> Probing_;
    std::optional<NJoinTable::TNeumannJoinTable> BuildingInMemoryTable_{Layouts_.Build};
    std::optional<TPageFutures> DumpInProgress_;
    std::optional<std::unordered_map<int, TSpilledBucket>> ProcessingSpilledData_;
    TMKQLVector<PairOfSpilledBuckets> Buckets_;
    TMKQLDeque<BlobIdAndBucketIndex> Blobs_;
    TPairOfBuckets SpilledPairs_;
    
};

} // namespace NKikimr::NMiniKQL