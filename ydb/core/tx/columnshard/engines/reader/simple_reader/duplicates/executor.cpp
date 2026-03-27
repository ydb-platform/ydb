#include "executor.h"
#include "private_events.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {

class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    TBuildFilterTaskContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvIntervalConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(errorAddresses.GetErrorMessage()), Request.GetGlobalContext().MakeResultInFlightGuard()));
            return;
        }

        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvIntervalConstructionResult>(std::move(Request), std::move(objectAddresses), std::move(AllocationGuard)));
    }

    virtual bool DoIsAborted() const override {
        return Request.GetGlobalContext().GetAbortionFlag() && Request.GetGlobalContext().GetAbortionFlag()->Val();
    }

public:
    TColumnFetchingCallback(TBuildFilterTaskContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Request(std::move(request))
        , AllocationGuard(allocationGuard)
    {
        AFL_VERIFY(allocationGuard);
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Request.GetGlobalContext().GetOwner());
        TActorContext::AsActorContext().Send(
            Request.GetGlobalContext().GetOwner(), std::make_unique<TEvIntervalConstructionResult>(std::move(Request), TConclusionStatus::Fail(errorMessage),
                                                       Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterTaskContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvIntervalConstructionResult>(std::move(Request), TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage),
                Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        THashSet<TPortionAddress> portionAddresses;
        for (const ui64 portionId : Request.GetRequiredPortions()) {
            AFL_VERIFY(portionAddresses.emplace(Request.GetGlobalContext().GetPortion(portionId)->GetAddress()).second);
        }
        auto columnDataManager = Request.GetGlobalContext().GetColumnDataManager();
        auto columns = Request.GetGlobalContext().GetFetchingColumnIds();
        columnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, portionAddresses, std::move(columns),
            std::make_shared<TColumnFetchingCallback>(std::move(Request), guard));
        return true;
    }

public:
    TColumnDataAllocation(TBuildFilterTaskContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    TBuildFilterTaskContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsMemoryGuard;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        if (result.HasErrors()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvIntervalConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(result.GetErrorMessage()), Request.GetGlobalContext().MakeResultInFlightGuard()));
            return;
        }

        if (result.HasRemovedData()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvIntervalConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(TStringBuilder{} << "Has removed accessors data, count " << result.GetRemovedData().size()), Request.GetGlobalContext().MakeResultInFlightGuard()));
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Request.GetGlobalContext().GetFetchingColumnIds(), false);
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(
            Request.GetGlobalContext().GetRequestGuard()->GetMemoryProcessId(), Request.GetGlobalContext().GetRequestGuard()->GetMemoryScopeId(),
            Request.GetGlobalContext().GetRequestGuard()->GetMemoryGroupId(),
            { std::make_shared<TColumnDataAllocation>(std::move(Request), mem) }, (ui64)TFilterAccumulator::EFetchingStage::COLUMN_DATA);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Request.GetGlobalContext().GetAbortionFlag();
    }

public:
    TColumnDataAccessorFetching(
        TBuildFilterTaskContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& accessorsMemoryGuard)
        : Request(std::move(request))
        , AccessorsMemoryGuard(accessorsMemoryGuard)
    {
    }

    static ui64 GetRequiredMemory(const THashSet<ui64>& portions, const TBuildFilterContext& context) {
        TDataAccessorsRequest dataAccessorsRequest(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const ui64 portionId : portions) {
            dataAccessorsRequest.AddPortion(context.GetPortion(portionId));
        }
        return dataAccessorsRequest.PredictAccessorsMemory(context.GetSnapshotSchema());
    }
};

class TDataAccessorAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterTaskContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvIntervalConstructionResult>(std::move(Request), TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage),
                Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const ui64 portionId : Request.GetRequiredPortions()) {
            request->AddPortion(Request.GetGlobalContext().GetPortion(portionId));
        }
        auto dataAccessorsManager = Request.GetGlobalContext().GetDataAccessorsManager();
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(std::move(Request), guard));
        dataAccessorsManager->AskData(request);
        return true;
    }

public:
    TDataAccessorAllocation(TBuildFilterTaskContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

}   // namespace

bool TBuildFilterTaskExecutor::ScheduleNext(TBuildFilterContext&& context) {
    std::vector<TIntervalInfo> intervals;
    THashSet<ui64> portionIds;

    while (portionIds.size() < BATCH_PORTIONS_COUNT_SOFT_LIMIT && Portions.Next()) {
        intervals.emplace_back(Portions.GetCurrentInterval());
        for (const ui64 portion : Portions.GetCurrentPortionIds()) {
            portionIds.emplace(portion);
        }
    }

    AFL_VERIFY(intervals.empty() == portionIds.empty());
    if (intervals.empty()) {
        AFL_VERIFY(Portions.IsDone());
        return false;
    }

    const ui64 mem = TColumnDataAccessorFetching::GetRequiredMemory(portionIds, context);
    const ui64 processId = context.GetRequestGuard()->GetMemoryProcessId();
    const ui64 scopeId = context.GetRequestGuard()->GetMemoryScopeId();
    const ui64 groupId = context.GetRequestGuard()->GetMemoryGroupId();
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId,
        { std::make_shared<TDataAccessorAllocation>(TBuildFilterTaskContext(std::move(context), shared_from_this(), std::move(intervals),
                                                        std::move(portionIds)), mem) }, (ui64)TFilterAccumulator::EFetchingStage::ACCESSORS);
    return true;
}

TIntervalsIterator::TPortionSpan::TPortionSpan(const ui64 portionId, const ui64 first, const ui64 last)
    : PortionId(portionId)
    , FirstIntervalIdx(first)
    , LastIntervalIdx(last)
{
    AFL_VERIFY(FirstIntervalIdx <= LastIntervalIdx);
}

auto TIntervalsIterator::TPortionSpan::operator==(const TPortionSpan& other) const {
    return std::tie(PortionId, FirstIntervalIdx, LastIntervalIdx) ==
           std::tie(other.PortionId, other.FirstIntervalIdx, other.LastIntervalIdx);
}

bool TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder::operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const {
    return std::tie(lhs.FirstIntervalIdx, lhs.LastIntervalIdx, lhs.PortionId) <
           std::tie(rhs.FirstIntervalIdx, rhs.LastIntervalIdx, rhs.PortionId);
}

bool TIntervalsIterator::TPortionSpan::TComparatorByRightBorder::operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const {
    return std::tie(lhs.LastIntervalIdx, lhs.FirstIntervalIdx, lhs.PortionId) <
           std::tie(rhs.LastIntervalIdx, rhs.FirstIntervalIdx, rhs.PortionId);
}

TIntervalsIterator::TIntervalsIterator(
    std::vector<TIntervalInfo>&& intervals, std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder>&& portions)
    : Intervals(std::move(intervals))
    , Portions(std::move(portions))
{
}

bool TIntervalsIterator::Next() {
    if (NextInterval == Intervals.size()) {
        return false;
    }
    AFL_VERIFY(NextInterval < Intervals.size());

    for (auto it = CurrentPortions.begin(); it != CurrentPortions.end() && it->GetLastIntervalIdx() < NextInterval;) {
        it = CurrentPortions.erase(it);
    }
    for (auto it = Portions.begin(); it != Portions.end() && it->GetFirstIntervalIdx() == NextInterval;) {
        AFL_VERIFY(CurrentPortions.emplace(*it).second);
        it = Portions.erase(it);
    }

    ++NextInterval;
    return true;
}

THashSet<ui64> TIntervalsIterator::GetCurrentPortionIds() const {
    THashSet<ui64> portions;
    for (const auto& portion : CurrentPortions) {
        portions.emplace(portion.GetPortionId());
    }
    return portions;
}

TIntervalInfo TIntervalsIterator::GetCurrentInterval() const {
    AFL_VERIFY(NextInterval <= Intervals.size());
    AFL_VERIFY(NextInterval > 0);
    return Intervals[NextInterval - 1];
}

THashSet<ui64> TIntervalsIterator::GetNeededPortions() const {
    THashSet<ui64> portions;
    for (const auto& portion : Portions) {
        AFL_VERIFY(portions.emplace(portion.GetPortionId()).second);
    }
    for (const auto& portion : CurrentPortions) {
        AFL_VERIFY(portions.emplace(portion.GetPortionId()).second);
    }
    return portions;
}

bool TIntervalsIterator::IsDone() const {
    AFL_VERIFY(Portions.empty() == (NextInterval == Intervals.size()))("portions", Portions.size())("current", CurrentPortions.size())(
                                     "next", NextInterval)("intervals", Intervals.size());
    return NextInterval == Intervals.size();
}

void TIntervalsIteratorBuilder::AppendInterval(const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& portions) {
    AFL_VERIFY(Intervals.empty() || Intervals.back().GetEnd().IsEquivalent(begin) || Intervals.back().GetEnd() < begin)(
                                                                                      "last", Intervals.back().GetEnd().DebugString())(
                                                                                      "new", begin.DebugString());
    const ui64 currentIntervalIdx = Intervals.size();
    Intervals.emplace_back(begin, end, portions);

    std::vector<ui64> completedPortions;
    for (const auto& [portion, firstInterval] : FirstIntervalByTrailingPortionId) {
        if (!portions.contains(portion)) {
            completedPortions.emplace_back(portion);
            AFL_VERIFY(Portions.emplace(portion, firstInterval, currentIntervalIdx - 1).second);
        }
    }
    for (const auto& portion : completedPortions) {
        FirstIntervalByTrailingPortionId.erase(portion);
    }

    for (const ui64 portion : portions) {
        Y_UNUSED(FirstIntervalByTrailingPortionId.emplace(portion, currentIntervalIdx));
    }
}

TIntervalsIterator TIntervalsIteratorBuilder::Build() {
    for (const auto& [portion, firstInterval] : FirstIntervalByTrailingPortionId) {
        AFL_VERIFY(Portions.emplace(portion, firstInterval, Intervals.size() - 1).second);
    }
    FirstIntervalByTrailingPortionId.clear();

    return TIntervalsIterator(std::move(Intervals), std::move(Portions));
}

ui64 TIntervalsIteratorBuilder::NumIntervals() const {
    return Intervals.size();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
