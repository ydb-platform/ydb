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
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    std::shared_ptr<TInternalFilterConstructor> Context;
    std::shared_ptr<ISnapshotSchema> Schema;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            Context->Abort(errorAddresses.GetErrorMessage());
            return;
        }

        AFL_VERIFY(AllocationGuard);
        auto task =
            std::make_shared<TBuildDuplicateFilters>(TDuplicateSourceCacheResult(std::move(objectAddresses), Schema), Context, AllocationGuard);
        NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
    }

    virtual bool DoIsAborted() const override {
        return false;
    }

public:
    TColumnFetchingCallback(std::shared_ptr<TInternalFilterConstructor>&& context, const std::shared_ptr<ISnapshotSchema>& schema,
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard)
        : Context(std::move(context))
        , Schema(schema)
        , AllocationGuard(guard)
    {
    }

    void OnError(const TString& errorMessage) {
        Context->Abort(errorMessage);
    }

    void SetAllocationGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard) {
        AFL_VERIFY(!AllocationGuard);
        AllocationGuard = std::move(allocationGuard);
        AFL_VERIFY(AllocationGuard);
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    std::shared_ptr<TInternalFilterConstructor> Context;
    THashSet<TPortionAddress> Portions;
    std::shared_ptr<ISnapshotSchema> Schema;
    std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        AFL_VERIFY(Context);
        Context->Abort(errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        AFL_VERIFY(Context);
        ColumnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, Portions,
            { Schema->GetColumnIds().begin(), Schema->GetColumnIds().end() },
            std::make_shared<TColumnFetchingCallback>(std::move(Context), std::move(Schema), std::move(guard)));
        return true;
    }

public:
    TColumnDataAllocation(const std::shared_ptr<TInternalFilterConstructor>& context, const THashSet<TPortionAddress>& portions,
        const std::shared_ptr<ISnapshotSchema>& schema, const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager,
        const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Context(context)
        , Portions(portions)
        , Schema(schema)
        , ColumnDataManager(columnDataManager)
    {
        AFL_VERIFY(Context);
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<TInternalFilterConstructor> Context;
    THashSet<TPortionAddress> Portions;
    std::shared_ptr<ISnapshotSchema> Schema;
    std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(Context);
        if (result.HasErrors()) {
            Context->Abort(result.GetErrorMessage());
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes({ Schema->GetColumnIds().begin(), Schema->GetColumnIds().end() }, false);
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(Context->GetMemoryProcessId(), Context->GetMemoryScopeId(),
            Context->GetMemoryGroupId(), { std::make_shared<TColumnDataAllocation>(Context, Portions, Schema, ColumnDataManager, mem) },
            std::nullopt);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }

public:
    TColumnDataAccessorFetching(const std::shared_ptr<TInternalFilterConstructor>& context, const THashSet<TPortionAddress>& portions,
        const std::shared_ptr<ISnapshotSchema>& schema, const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager)
        : Context(context)
        , Portions(portions)
        , Schema(schema)
        , ColumnDataManager(columnDataManager)
    {
        AFL_VERIFY(Context);
    }
};
}   // namespace

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

std::shared_ptr<ISnapshotSchema> TDuplicateManager::MakeFetchingSchema(const TSpecialReadContext& context) {
    const auto& schema = context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema();
    std::set<ui32> columnIds = schema->GetPkColumnsIds();
    for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
        columnIds.emplace(columnId);
    }
    return std::make_shared<TFilteredSnapshotSchema>(
        context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema(), columnIds);
}

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, const std::deque<NSimple::TSourceConstructor>& portions)
    : TActor(&TDuplicateManager::StateMain)
    , FetchingSchema(MakeFetchingSchema(context))
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Intervals(MakeIntervalTree(portions))
    , Portions(MakePortionsIndex(Intervals))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
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
        AFL_VERIFY((*sourcesToFetch.begin())->GetPortionId() == ev->Get()->GetSourceId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, (*sourcesToFetch.begin())->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    auto constructor = std::make_shared<TInternalFilterConstructor>(ev, *source, Counters, PKSchema);

    THashSet<TPortionAddress> portionAddresses;
    for (const auto& portion : sourcesToFetch) {
        portionAddresses.insert(portion->GetAddress());
    }

    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
    request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(
        std::move(constructor), std::move(portionAddresses), FetchingSchema, ColumnDataManager));
    for (auto&& source : sourcesToFetch) {
        request->AddPortion(source);
    }
    DataAccessorsManager->AskData(request);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
