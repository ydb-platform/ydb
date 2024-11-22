#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceResult(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard,
    const std::optional<NArrow::TShardedRecordBatch>& newBatch, const std::shared_ptr<arrow::RecordBatch>& lastPK,
    std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, const ui32 intervalIdx, TPlainReadData& reader) {
    if (Context->GetReadMetadata()->Limit && (!newBatch || newBatch->GetRecordsCount() == 0) && InFlightLimit < MaxInFlight) {
        InFlightLimit = std::min<ui32>(MaxInFlight, InFlightLimit * 4);
    }
    auto itInterval = FetchingIntervals.find(intervalIdx);
    AFL_VERIFY(itInterval != FetchingIntervals.end());
    itInterval->second->SetMerger(std::move(merger));
    AFL_VERIFY(Context->GetCommonContext()->GetReadMetadata()->IsSorted());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result_received")("interval_idx", intervalIdx)(
        "intervalId", itInterval->second->GetIntervalId());
    if (newBatch && newBatch->GetRecordsCount()) {
        std::optional<ui32> callbackIdxSubscriber;
        std::shared_ptr<NGroupedMemoryManager::TGroupGuard> gGuard;
        if (itInterval->second->HasMerger()) {
            callbackIdxSubscriber = intervalIdx;
        } else {
            gGuard = itInterval->second->GetGroupGuard();
        }
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, std::make_shared<TPartialReadResult>(std::move(allocationGuard), std::move(gGuard), *newBatch, lastPK, callbackIdxSubscriber)).second);
    } else {
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, nullptr).second);
    }
    Y_ABORT_UNLESS(FetchingIntervals.size());
    while (FetchingIntervals.size()) {
        const auto interval = FetchingIntervals.begin()->second;
        const ui32 intervalIdx = interval->GetIntervalIdx();
        auto it = ReadyIntervals.find(intervalIdx);
        if (it == ReadyIntervals.end()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result_absent")("interval_idx", intervalIdx)(
                "merger", interval->HasMerger())("interval_id", interval->GetIntervalId());
            break;
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count",
                it->second ? it->second->GetRecordsCount() : 0)("merger", interval->HasMerger())("interval_id", interval->GetIntervalId());
        }
        auto result = it->second;
        ReadyIntervals.erase(it);
        if (result) {
            reader.OnIntervalResult(result);
        }
        if (!interval->HasMerger()) {
            FetchingIntervals.erase(FetchingIntervals.begin());
        } else if (result) {
            break;
        } else {
            interval->OnPartSendingComplete();
        }
    }
    if (FetchingIntervals.empty()) {
        AFL_VERIFY(ReadyIntervals.empty());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "intervals_finished");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_interval")("remained", FetchingIntervals.size())(
            "interval_idx", FetchingIntervals.begin()->first);
    }
}

TConclusionStatus TScanHead::Start() {
    for (auto&& i : FetchingSources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i));
    }
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
    if (HasAppData()) {
        if (AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
            MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
        }
    }
    if (Context->GetReadMetadata()->Limit) {
        InFlightLimit = 1;
    } else {
        InFlightLimit = MaxInFlight;
    }
    for (auto&& i : sources) {
        if (!context->GetCommonContext()->GetScanCursor()->CheckPortionUsage(i)) {
            continue;
        }
        FetchingSources.emplace(source);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (Context->IsAborted()) {
        return false;
    }
    while (SortedSources.size() && FetchingSources.size() < InFlightLimit) {
        SortedSources.front()->Start(SortedSources.front());
        FetchingSources.emplace(SortedSources.front());
        SortedSources.pop_front();
        return true;
    }
    return false;
}

const TReadContext& TScanHead::GetContext() const {
    return *Context->GetCommonContext();
}

bool TScanHead::IsReverse() const {
    return GetContext().GetReadMetadata()->IsDescSorted();
}

void TScanHead::Abort() {
    AFL_VERIFY(Context->IsAborted());
    for (auto&& i : FetchingSources) {
        i->Abort();
    }
    for (auto&& i : SortedSources) {
        i->Abort();
    }
    FetchingSources.clear();
    SortedSources.clear();
    Y_ABORT_UNLESS(IsFinished());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
