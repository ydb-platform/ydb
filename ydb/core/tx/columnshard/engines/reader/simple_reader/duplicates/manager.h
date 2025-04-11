#pragma once

#include "events.h"
#include "interval_counter.h"
#include "interval_index.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;

class TEvDuplicateFilterIntervalResult
    : public NActors::TEventLocal<TEvDuplicateFilterIntervalResult, NColumnShard::TEvPrivate::EvDuplicateFilterIntervalResult> {
private:
    using TFilterBySourceId = THashMap<ui64, NArrow::TColumnFilter>;
    YDB_READONLY(TConclusion<TFilterBySourceId>, Result, TFilterBySourceId());
    YDB_READONLY_DEF(ui32, IntervalIdx);

public:
    TEvDuplicateFilterIntervalResult(TConclusion<TFilterBySourceId>&& result, const ui32 intervalIdx)
        : Result(std::move(result))
        , IntervalIdx(intervalIdx) {
        AFL_VERIFY(!Result->empty());
    }

    TFilterBySourceId&& DetachResult() {
        return Result.DetachResult();
    }
};

class TEvDuplicateFilterDataFetched
    : public NActors::TEventLocal<TEvDuplicateFilterDataFetched, NColumnShard::TEvPrivate::EvDuplicateFilterDataFetched> {
private:
    YDB_READONLY_DEF(ui64, SourceId);
    YDB_READONLY(TConclusionStatus, Status, TConclusionStatus::Success());

public:
    TEvDuplicateFilterDataFetched(const ui64 sourceId, const TConclusionStatus& status)
        : SourceId(sourceId)
        , Status(status) {
    }
};

class TEvDuplicateFilterStartFetching
    : public NActors::TEventLocal<TEvDuplicateFilterStartFetching, NColumnShard::TEvPrivate::EvDuplicateFilterStartFetching> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TColumnFetchingContext>, Context);
    YDB_READONLY_DEF(std::shared_ptr<NBlobOperations::NRead::ITask>, NextAction);

public:
    TEvDuplicateFilterStartFetching(
        const std::shared_ptr<TColumnFetchingContext>& context, const std::shared_ptr<NBlobOperations::NRead::ITask>& nextAction)
        : Context(context)
        , NextAction(nextAction) {
    }
};

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
    friend class TColumnFetchingContext;

    class TNoOpSubscriber: public IFilterSubscriber {
    private:
        virtual void OnFilterReady(const NArrow::TColumnFilter&) {
        }
        virtual void OnFailure(const TString&) {
        }
    };

    class TIntervalsRange {
    private:
        YDB_READONLY_DEF(ui32, FirstIdx);
        YDB_READONLY_DEF(ui32, LastIdx);

    public:
        TIntervalsRange(const ui32 first, ui32 last)
            : FirstIdx(first)
            , LastIdx(last) {
            AFL_VERIFY(last >= first)("first", first)("last", last);
        }

        ui32 NumIntervals() const {
            return LastIdx - FirstIdx + 1;
        }

        TString DebugString() const {
            return TStringBuilder() << '[' << FirstIdx << ',' << LastIdx << ']';
        }
    };

    class TSourceIntervals {
    private:
        using TRangeBySourceId = THashMap<ui64, TIntervalsRange>;
        using TIdsByIdx = std::map<ui32, std::vector<ui64>>;
        YDB_READONLY_DEF(TRangeBySourceId, SourceRanges);
        std::vector<NArrow::TReplaceKey> IntervalBorders;

    public:
        TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& sources);

        const NArrow::TReplaceKey& GetRightInclusiveBorder(const ui32 intervalIdx) const {
            return IntervalBorders[intervalIdx];
        }
        const std::optional<NArrow::TReplaceKey> GetLeftExlusiveBorder(const ui32 intervalIdx) const {
            if (intervalIdx == 0) {
                return std::nullopt;
            }
            return IntervalBorders[intervalIdx - 1];
        }
        ui32 NumIntervals() const {
            return IntervalBorders.size();
        }
        ui32 NumSources() const {
            return SourceRanges.size();
        }

        TIntervalsRange GetRangeBySourceId(const ui64 sourceId) const {
            const TIntervalsRange* findRange = SourceRanges.FindPtr(sourceId);
            AFL_VERIFY(findRange)("source", sourceId);
            return *findRange;
        }
    };

    class TRowRange {
    private:
        YDB_READONLY_DEF(ui64, Begin);
        YDB_READONLY_DEF(ui64, End);

    public:
        ui64 Size() const {
            return End - Begin;
        }

        bool Empty() const {
            return Begin == End;
        }

        TString DebugString() const {
            return TStringBuilder() << "[" << Begin << ";" << End << ")";
        }

        TRowRange(const ui64 begin, const ui64 end)
            : Begin(begin)
            , End(end) {
            AFL_VERIFY(Begin <= End)("begin", Begin)("end", End);
        }
    };

    class TSourceFilterConstructor: NColumnShard::TMonitoringObjectsCounter<TSourceFilterConstructor>, TMoveOnly {
    private:
        ui32 FirstIntervalIdx;
        YDB_READONLY_DEF(std::vector<std::optional<NArrow::TColumnFilter>>, IntervalFilters);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, ColumnData);
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> MemoryGuard;
        YDB_READONLY_DEF(std::shared_ptr<TPortionDataSource>, Source);
        std::shared_ptr<IFilterSubscriber> Subscriber;
        std::vector<ui64> IntervalOffsets;
        std::vector<NArrow::TReplaceKey> RightIntervalBorders;
        ui64 ReadyFilterCount = 0;

    public:
        TSourceFilterConstructor(const std::shared_ptr<TPortionDataSource>& source, const TSourceIntervals& intervals);

        void SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter);
        void SetMemoryGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
            AFL_VERIFY(!MemoryGuard);
            MemoryGuard = std::move(guard);
        }
        void SetColumnData(std::shared_ptr<NArrow::TGeneralContainer>&& data);
        void SetSubscriber(const std::shared_ptr<IFilterSubscriber>& subscriber) {
            AFL_VERIFY(!Subscriber);
            Subscriber = subscriber;
            AFL_VERIFY(Subscriber);
        }

        bool IsReady() const;

        TRowRange GetIntervalRange(const ui32 globalIntervalIdx) const;

        void Finish();
        void AbortConstruction(const TString& reason);
    };

    class TWaitingSourceInfo {
    private:
        std::atomic_uint64_t FetchingMemoryGroupId = std::numeric_limits<uint64_t>::max();
        std::atomic_bool IsFetchingStarted = false;
        ui32 PortionIdx;

    public:
        TWaitingSourceInfo(const ui32 portionIdx)
            : PortionIdx(portionIdx) {
        }

        [[nodiscard]] bool SetStartAllocation(const ui64 memoryGroupId) {
            ui64 oldMemGroup = FetchingMemoryGroupId.load();
            while (true) {
                if (oldMemGroup < memoryGroupId) {
                    return false;
                }
                if (FetchingMemoryGroupId.compare_exchange_weak(oldMemGroup, memoryGroupId)) {
                    break;
                }
            }

            AFL_VERIFY(oldMemGroup != memoryGroupId);
            if (IsFetchingStarted.load()) {
                return false;
            }
            return true;
        }

        bool SetStartFetching() {
            return !IsFetchingStarted.exchange(true);
        }

        std::shared_ptr<TPortionDataSource> Construct(const std::shared_ptr<TSpecialReadContext>& context) const;
    };

private:
    THashMap<ui64, std::shared_ptr<TWaitingSourceInfo>> WaitingSources;
    THashMap<ui64, std::shared_ptr<TSourceFilterConstructor>> ActiveSources;

    const bool ReverseFetchingOrder;
    const TSourceIntervals Intervals;
    TIntervalCounter NotFetchedSourcesCount;
    TIntervalSet NotFetchedSourcesIndex;
    std::shared_ptr<TIntervalSet::TCursor> StableBorderCursor;
    THashMap<ui32, std::vector<ui64>> FetchedSourcesByInterval;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(TEvDuplicateFilterIntervalResult, Handle);
            hFunc(TEvDuplicateFilterDataFetched, Handle);
            hFunc(TEvDuplicateFilterStartFetching, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterStartFetching::TPtr&);
    void Handle(const TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const TEvDuplicateFilterIntervalResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);

    void AbortAndPassAway(const TString& reason);
    void StartAllocation(const ui64 sourceId, const std::shared_ptr<IDataSource>& requester);
    void StartMergingColumns(const ui32 intervalIdx);

    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceId(const ui64 sourceId) const;
    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceSeqNumber(const ui32 seqNumber) const;

public:
    TDuplicateFilterConstructor(const TSpecialReadContext& context, const bool reverseFetchingOrder);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
