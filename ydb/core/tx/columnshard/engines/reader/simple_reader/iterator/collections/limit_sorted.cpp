#include "limit_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IDataSource> TScanWithLimitCollection::DoExtractNext() {
    AFL_VERIFY(HeapSources.size());
    std::pop_heap(HeapSources.begin(), HeapSources.end());
    auto result = NextSource ? NextSource : HeapSources.back().Construct(SourceIdxCurrent++, Context);
    AFL_VERIFY(FetchingInFlightSources.emplace(result->GetSourceId()).second);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractNext")("source_id", result->GetSourceId());
    HeapSources.pop_back();
    if (HeapSources.size()) {
        NextSource = HeapSources.front().Construct(SourceIdxCurrent++, Context);
    } else {
        NextSource = nullptr;
    }
    return result;
}

void TScanWithLimitCollection::DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceFinished")("source_id", source->GetSourceId())("limit", Limit)(
        "max", GetMaxInFlight())("in_flight_limit", InFlightLimit)("count", FetchingInFlightSources.size());
    if (!source->GetResultRecordsCount() && InFlightLimit < GetMaxInFlight()) {
        InFlightLimit = 2 * InFlightLimit;
    }
    AFL_VERIFY(Cleared || Aborted || FetchingInFlightSources.erase(source->GetSourceId()))("source_id", source->GetSourceId());
}

TScanWithLimitCollection::TScanWithLimitCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources, const std::shared_ptr<IScanCursor>& cursor)
    : TBase(context)
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust()) {
    HeapSources = std::move(sources);
    std::make_heap(HeapSources.begin(), HeapSources.end());
    if (cursor && cursor->IsInitialized()) {
        while (HeapSources.size()) {
            bool usage = false;
            if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(HeapSources.front(), usage)) {
                std::pop_heap(HeapSources.begin(), HeapSources.end());
                HeapSources.pop_back();
                continue;
            }
            if (usage) {
                HeapSources.front().SetIsStartedByCursor();
            } else {
                std::pop_heap(HeapSources.begin(), HeapSources.end());
                HeapSources.pop_back();
            }
            break;
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
