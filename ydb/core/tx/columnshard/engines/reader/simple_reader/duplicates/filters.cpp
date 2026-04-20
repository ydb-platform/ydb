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
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("component", "duplicates_manager")
        ("type", "filter_ready")
        ("info", DebugString())
        ("portion_id", OriginalRequest->Get()->GetPortionId())
        ("filter", filter.DebugString());
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
    ReadyFilters.emplace(portionId, std::move(filterInfo.Filter));
    Filters.erase(filterIt);
}

TString TFiltersBuilder::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "ReadyFilters=" << ReadyFilters.size() << ";";
    sb << "Filters=" << Filters.size() << ";";
    sb << "RowsAdded=" << RowsAdded << ";";
    sb << "RowsSkipped=" << RowsSkipped << ";";
    sb << "}";
    return sb;
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

void TFiltersBuilder::AddSource(const ui64 portionId, ui64 rowsCount) {
    AFL_VERIFY(Filters.emplace(portionId, TFilterInfo{rowsCount, NArrow::TColumnFilter::BuildAllowFilter()}).second);
}

ui64 TFiltersBuilder::CountSources() const {
    return Filters.size();
}

THashMap<ui64, NArrow::TColumnFilter>&& TFiltersBuilder::ExtractReadyFilters() {
    return std::move(ReadyFilters);
}

TFiltersStore::TFiltersStore(const bool reverse, const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters)
    : IsReverse(reverse)
    , Counters(counters) {
}

NArrow::TColumnFilter TFiltersStore::MakeOrderedFilter(NArrow::TColumnFilter&& filter) {
    if (IsReverse) {
        return filter.CreateReversed();
    }
    return std::move(filter);
}

bool TFiltersStore::NotifyReadyFilter(std::shared_ptr<TFilterAccumulator>& constructor) {
    const ui64 portionId = constructor->GetRequest()->Get()->GetPortionId();
    auto filterIt = ReadyFilters.find(portionId);

    if (filterIt == ReadyFilters.end()) {
        return false;
    }

    auto& filter = filterIt->second;
    Counters->OnReadyFilters(-1, -static_cast<i64>(filter.GetDataSize()));
    constructor->AddFilter(MakeOrderedFilter(std::move(filter)));
    ReadyFilters.erase(filterIt);
    return true;
}

void TFiltersStore::AddReadyFilter(const ui64 portionId, NArrow::TColumnFilter&& filter) {
    auto waitingIt = WaitingPortions.find(portionId);
    if (waitingIt != WaitingPortions.end()) {
        waitingIt->second->AddFilter(MakeOrderedFilter(std::move(filter)));
        WaitingPortions.erase(waitingIt);
        return;
    }
    Counters->OnReadyFilters(1, filter.GetDataSize());
    AFL_VERIFY(ReadyFilters.emplace(portionId, std::move(filter)).second);
}

void TFiltersStore::AddWaitingPortion(const ui64 portionId, std::shared_ptr<TFilterAccumulator>& constructor) {
    AFL_VERIFY(WaitingPortions.emplace(portionId, constructor).second);
}

void TFiltersStore::Abort(const TString& error) {
    for (const auto& [_, constructor] : WaitingPortions) {
        constructor->Abort(error);
    }
}

TFiltersStore::~TFiltersStore() {
    for (const auto& [_, filter] : ReadyFilters) {
        Counters->OnReadyFilters(-1, -static_cast<i64>(filter.GetDataSize()));
    }
}

}
