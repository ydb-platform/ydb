#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {
class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TColumnDataCachePolicy::TAddress;
    using TObject = NGeneralCache::TColumnDataCachePolicy::TObject;

    TActorId Owner;
    TActorId ColumnShardActorId;
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);
    std::map<ui32, std::shared_ptr<arrow::Field>> Columns;
    std::vector<TPortionInfo::TConstPtr> Portions;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    void OnDone() {
        AFL_VERIFY(Owner);
        Owner = TActorId();
    }

    bool IsDone() const {
        return !Owner;
    }

    virtual void DoOnResultReady(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& /*removedAddresses*/,
        THashMap<TAddress, TString>&& errorAddresses) override {
        if (!errorAddresses.empty()) {
            TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, errorAddresses.begin()->second));
            OnDone();
            return;
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& [_, field] : Columns) {
            fields.emplace_back(field);
        }

        THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> result;
        for (const TPortionInfo::TConstPtr& portion : Portions) {
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
            for (const auto& [columnId, field] : Columns) {
                std::shared_ptr<NArrow::NAccessor::IChunkedArray>* findColumn =
                    objectAddresses.FindPtr(NGeneralCache::TGlobalColumnAddress(ColumnShardActorId, portion->GetAddress(), columnId));
                AFL_VERIFY(findColumn)("portion", portion->DebugString())("column", columnId);
                columns.emplace_back(*findColumn);
            }
            std::shared_ptr<NArrow::TGeneralContainer> container = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(columns));
            result.emplace(portion->GetPortionId(), std::move(container));
        }

        AFL_VERIFY(Owner);
        AFL_VERIFY(AllocationGuard);
        TActorContext::AsActorContext().Send(
            Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, std::move(result), std::move(AllocationGuard)));
        OnDone();
    }

    virtual bool DoIsAborted() const override {
        return false;
    }

public:
    TColumnFetchingCallback(const TActorId& owner, const TActorId& columnShardActorId, std::shared_ptr<TInternalFilterConstructor>&& context,
        std::map<ui32, std::shared_ptr<arrow::Field>>&& columns, const std::vector<TPortionInfo::TConstPtr>& portions)
        : Owner(owner)
        , ColumnShardActorId(columnShardActorId)
        , Context(std::move(context))
        , Columns(std::move(columns))
        , Portions(std::move(portions))
    {
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Owner);
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, errorMessage));
        OnDone();
    }

    void SetAllocationGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard) {
        AFL_VERIFY(!AllocationGuard);
        AllocationGuard = allocationGuard;
        AFL_VERIFY(AllocationGuard);
    }

    ~TColumnFetchingCallback() {
        AFL_VERIFY(IsDone());
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    std::shared_ptr<TColumnFetchingCallback> Callback;
    THashSet<NGeneralCache::TColumnDataCachePolicy::TAddress> ColumnsToFetch;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Callback->OnError(errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        Callback->SetAllocationGuard(std::move(guard));
        NColumnFetching::TGeneralCache::AskObjects(NBlobOperations::EConsumer::DUPLICATE_FILTERING, std::move(ColumnsToFetch), Callback);
        return true;
    }

public:
    TColumnDataAllocation(const std::shared_ptr<TColumnFetchingCallback>& callback,
        THashSet<NGeneralCache::TColumnDataCachePolicy::TAddress>&& columnsToFetch, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Callback(callback)
        , ColumnsToFetch(std::move(columnsToFetch))
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<TColumnFetchingCallback> Callback;
    TActorId ColumnShardActorId;
    std::vector<TPortionAddress> Portions;
    std::set<ui32> Columns;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(Callback);
        if (result.HasErrors()) {
            Callback->OnError(result.GetErrorsByPathId().begin()->second);
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Columns);
        }

        THashSet<NGeneralCache::TColumnDataCachePolicy::TAddress> columnsToFetch;
        for (const auto& portion : Portions) {
            for (const ui32 column : Columns) {
                columnsToFetch.emplace(ColumnShardActorId, portion, column);
            }
        }
        AFL_VERIFY(!columnsToFetch.empty());
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(Callback->GetContext()->GetMemoryProcessId(),
            Callback->GetContext()->GetMemoryScopeId(), Callback->GetContext()->GetMemoryGroupId(),
            { std::make_shared<TColumnDataAllocation>(Callback, std::move(columnsToFetch), mem) }, std::nullopt);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }

public:
    TColumnDataAccessorFetching(const std::shared_ptr<TColumnFetchingCallback>& callback, const TActorId& columnShardActorId,
        const std::vector<TPortionAddress>& portions, const std::set<ui32>& columns)
        : Callback(callback)
        , ColumnShardActorId(columnShardActorId)
        , Portions(portions)
        , Columns(columns)
    {
    }
};
}   // namespace

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, TPortionIntervalTree&& portions)
    : TActor(&TDuplicateManager::StateMain)
    , ColumnShardActorId(context.GetCommonContext()->GetColumnShardActorId())
    , PKColumns(context.GetPKColumns())
    , Counters(context.GetCommonContext()->GetDuplicateFilteringCounters())
    , Intervals(std::move(portions))
    , Portions([this]() {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
        Intervals.EachRange(
            [&portions](const TPortionIntervalTree::TOwnedRange& /*range*/, const std::shared_ptr<TPortionInfo>& portion) mutable {
                AFL_VERIFY(portions.emplace(portion->GetPortionId(), portion).second);
            });
        return portions;
    }())
    , FiltersCache(100)   // FIXME configure
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<TPortionInfo::TConstPtr> sourcesToFetch;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(ev->Get()->GetSourceId());
    {
        const auto collector = [&sourcesToFetch, &borders](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            sourcesToFetch.emplace_back(portion);
            borders.emplace(portion->GetPortionId(),
                NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
        };
        Intervals.EachIntersection(TPortionIntervalTree::TRange(source->IndexKeyStart(), true, source->IndexKeyEnd(), true), collector);
    }

    LOCAL_LOG_TRACE("event", "request_filter")("source", ev->Get()->GetSourceId())("fetching_sources", sourcesToFetch.size());
    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1 && ev->Get()->GetMaxVersion() >= source->RecordSnapshotMax(ev->Get()->GetMaxVersion())) {
        AFL_VERIFY(sourcesToFetch.front()->GetPortionId() == ev->Get()->GetSourceId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, sourcesToFetch.front()->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(source->IndexKeyStart(), source->IndexKeyEnd(), source->IndexKeyStart().GetSchema()));
    std::shared_ptr<TInternalFilterConstructor> constructor = std::make_shared<TInternalFilterConstructor>(ev, std::move(splitter));

    std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
    {
        for (const auto& columnId : PKColumns->GetColumnIds()) {
            fieldsByColumn.emplace(columnId, PKColumns->GetFilteredSchemaVerified().GetFieldByColumnIdVerified(columnId));
        }
        for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
            fieldsByColumn.emplace(columnId, IIndexInfo::GetColumnFieldVerified(columnId));
        }
    }

    {
        std::vector<TPortionAddress> portionAddresses;
        for (const auto& portion : sourcesToFetch) {
            portionAddresses.emplace_back(portion->GetAddress());
        }
        std::set<ui32> columns;
        for (const auto& [columnId, _] : fieldsByColumn) {
            columns.emplace(columnId);
        }

        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(
            std::make_shared<TColumnFetchingCallback>(SelfId(), ColumnShardActorId, std::move(constructor), std::move(fieldsByColumn),
                sourcesToFetch), ColumnShardActorId, portionAddresses, columns));
        for (auto&& source : sourcesToFetch) {
            request->AddPortion(source);
        }
        DataAccessorsManager->AskData(request);
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        const TString& error = ev->Get()->GetConclusion().GetErrorMessage();
        ev->Get()->GetContext()->Abort(error);
        AbortAndPassAway(error);
        return;
    }

    const std::shared_ptr<TInternalFilterConstructor>& context = ev->Get()->GetContext();
    const TEvRequestFilter* filterRequest = context->GetRequest()->Get();
    const TColumnDataSplitter& splitter = context->GetIntervals();
    const TSnapshot maxVersion = context->GetRequest()->Get()->GetMaxVersion();
    auto allocationGuard = ev->Get()->ExtractAllocationGuard();

    THashMap<ui64, std::vector<TRowRange>> splitted;
    for (const auto& [id, data] : *ev->Get()->GetConclusion()) {
        splitted[id] = splitter.SplitPortion(data);
    }

    THashSet<ui64> builtIntervals;
    {
        const auto& splittedMain = *TValidator::CheckNotNull(splitted.FindPtr(filterRequest->GetSourceId()));
        AFL_VERIFY(splittedMain.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            TDuplicateMapInfo mapInfo(maxVersion, splittedMain[i], filterRequest->GetSourceId());
            if (!mapInfo.GetRows().NumRows()) {
                builtIntervals.insert(i);
            } else if (auto* findBuilding = BuildingFilters.FindPtr(mapInfo)) {
                findBuilding->emplace_back(context);
                builtIntervals.insert(i);
            } else if (auto findCached = FiltersCache.Find(mapInfo); findCached != FiltersCache.End()) {
                context->AddFilter(findCached.Key(), findCached.Value());
                builtIntervals.insert(i);
            } else {
                BuildingFilters[mapInfo].emplace_back(context);
            }
        }
    }
    LOCAL_LOG_TRACE("event", "construct_filters")
    ("source", filterRequest->GetSourceId())("built_intervals", builtIntervals.size())("intervals", splitter.NumIntervals())(
        "done", context->IsDone())("splitter", splitter.DebugString());
    if (context->IsDone()) {
        return;
    }

    std::vector<THashMap<ui64, TRowRange>> intervals(splitter.NumIntervals());
    for (auto&& [source, portionIntervals] : splitted) {
        for (ui64 i = 0; i < portionIntervals.size(); ++i) {
            if (portionIntervals[i].NumRows()) {
                intervals[i].emplace(source, std::move(portionIntervals[i]));
            }
        }
    }

    NArrow::NMerger::TCursor maxVersionBatch = [&maxVersion]() {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, maxVersion, std::numeric_limits<ui64>::max());
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    }();

    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        auto&& segments = intervals[i];
        if (segments.empty() || builtIntervals.contains(i)) {
            // Do nothing
        } else if (segments.size() == 1 && maxVersion >= GetPortionVerified(segments.begin()->first)->RecordSnapshotMax(maxVersion)) {
            TDuplicateMapInfo mapInfo(maxVersion, segments.begin()->second, segments.begin()->first);
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, mapInfo.GetRows().NumRows());
            AFL_VERIFY(BuildingFilters.contains(mapInfo));
            Send(SelfId(),
                new NPrivate::TEvFilterConstructionResult(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mapInfo, filter } })));
        } else {
            const TColumnDataSplitter::TBorder& finish = splitter.GetIntervalFinish(i);
            const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
                finish.GetKey().GetSchema(), maxVersionBatch, finish.GetKey(), finish.GetIsLast(), Counters, SelfId());
            for (auto&& [source, segment] : segments) {
                const auto* columnData = ev->Get()->GetConclusion()->FindPtr(source);
                AFL_VERIFY(columnData)("source", source);
                task->AddSource(*columnData, allocationGuard, TDuplicateMapInfo(maxVersion, segment, source));
                Y_UNUSED(BuildingFilters
                             .emplace(TDuplicateMapInfo(maxVersion, segment, source), std::vector<std::shared_ptr<TInternalFilterConstructor>>())
                             .second);
            }
            NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
        }
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->GetConclusion().GetErrorMessage());
        return AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
    }
    LOCAL_LOG_TRACE("event", "filters_constructed")("sources", ev->Get()->GetConclusion().GetResult().size());
    for (auto&& [key, filter] : ev->Get()->ExtractResult()) {
        LOCAL_LOG_TRACE("event", "extract_constructed_filter")("range", key.DebugString());
        auto findWaiting = BuildingFilters.find(key);
        AFL_VERIFY(findWaiting != BuildingFilters.end());
        for (const auto& callback : findWaiting->second) {
            callback->AddFilter(key, std::move(filter));
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
