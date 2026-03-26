#include "filters.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
    
TFilterAccumulator::TFilterAccumulator(const TEvRequestFilter::TPtr& request, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters)
    : OriginalRequest(request)
    , Counters(counters)
    , StartTime(TInstant::Now())
{
    AFL_VERIFY(!!OriginalRequest);
    Counters->OnRequestStart();
}

TFilterAccumulator::~TFilterAccumulator() {
    AFL_VERIFY(IsDone() || (OriginalRequest->Get()->GetAbortionFlag() && OriginalRequest->Get()->GetAbortionFlag()->Val()) || TActorSystem::IsStopped())("state", DebugString());
    Counters->OnRequestFinish((TInstant::Now() - StartTime).MilliSeconds());
}

void TFilterAccumulator::AddFilter(NArrow::TColumnFilter&& filter) {
    AFL_VERIFY(!IsDone());
    OriginalRequest->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
    Done = true;
    AFL_VERIFY(IsDone());
}

bool TFilterAccumulator::IsDone() const {
    return Done;
}

void TFilterAccumulator::Abort(const TString& error) {
    OriginalRequest->Get()->GetSubscriber()->OnFailure(error);
    Done = true;
}

const TEvRequestFilter::TPtr& TFilterAccumulator::GetRequest() const {
    return OriginalRequest;
}

TString TFilterAccumulator::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "Portion=" << OriginalRequest->Get()->GetPortionId() << ";";
    sb << "Done=" << Done << ";";
    sb << "}";
    return sb;
}

void TFiltersBuilder::AddImpl(const ui64 portionId, const bool value) {
    auto filterIt = Filters.find(portionId);
    AFL_VERIFY(filterIt != Filters.end());
    auto& filterInfo = filterIt->second;
    filterInfo.Filter.Add(value);
    if (filterInfo.RowsCount != filterInfo.Filter.GetRecordsCount().value_or(0)) {
        return;
    }
    auto waitingIt = WaitingPortions.find(portionId);
    if (waitingIt != WaitingPortions.end()) {
        waitingIt->second->AddFilter(std::move(filterInfo.Filter));
        WaitingPortions.erase(waitingIt);
        Filters.erase(filterIt);
    }
}

void TFiltersBuilder::AddRecord(const NArrow::NMerger::TBatchIterator& cursor) {
    AddImpl(cursor.GetSourceId(), true);
    ++RowsAdded;
}

void TFiltersBuilder::SkipRecord(const NArrow::NMerger::TBatchIterator& cursor) {
    AddImpl(cursor.GetSourceId(), false);
    ++RowsSkipped;
}

void TFiltersBuilder::ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const {
}

bool TFiltersBuilder::IsBufferExhausted() const {
    return false;
}

bool TFiltersBuilder::NotifyReadyFilter(std::shared_ptr<TFilterAccumulator>& constructor) {
    const ui64 portionId = constructor->GetRequest()->Get()->GetPortionId();
    auto filterIt = Filters.find(portionId);

    if (filterIt == Filters.end()) {
        return false;
    }
    
    auto& filterInfo = filterIt->second;
    if (filterInfo.RowsCount != filterInfo.Filter.GetRecordsCount().value_or(0)) {
        return false;
    }

    constructor->AddFilter(std::move(filterInfo.Filter));
    Filters.erase(filterIt);
    return true;
}

void TFiltersBuilder::AddSource(const ui64 portionId, ui64 rowsCount) {
    AFL_VERIFY(Filters.emplace(portionId, TFilterInfo{rowsCount, NArrow::TColumnFilter::BuildAllowFilter()}).second);
}

void TFiltersBuilder::AddWaitingPortion(const ui64 portionId, std::shared_ptr<TFilterAccumulator>& constructor) {
    AFL_VERIFY(WaitingPortions.emplace(portionId, constructor).second);
}

void TFiltersBuilder::Abort(const TString& error) {
    for (const auto& [_, constructor] : WaitingPortions) {
        constructor->Abort(error);
    }
}

}
