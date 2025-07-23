#include "limit_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<NCommon::IDataSource> TScanWithLimitCollection::DoExtractNext() {
    if (!NextSource) {
        if (!SourcesConstructor->IsFinished()) {
            NextSource = SourcesConstructor->ExtractNext(Context, InFlightLimit);
            if (!NextSource) {
                return nullptr;
            }
        }
    }
    std::shared_ptr<NCommon::IDataSource> localNext;
    {
        if (!SourcesConstructor->IsFinished()) {
            localNext = SourcesConstructor->ExtractNext(Context, InFlightLimit);
            if (!localNext) {
                return nullptr;
            }
        } else {
            localNext = nullptr;
        }
        auto result = std::move(NextSource);
        NextSource = std::move(localNext);
        AFL_VERIFY(FetchingInFlightSources.emplace(result->GetSourceId()).second);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractNext")("source_id", result->GetSourceId());
        return result;
    }
}

void TScanWithLimitCollection::DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceFinished")("source_id", source->GetSourceId())("limit", Limit)(
        "max", GetMaxInFlight())("in_flight_limit", InFlightLimit)("count", FetchingInFlightSources.size());
    if (!source->GetAs<IDataSource>()->GetResultRecordsCount() && InFlightLimit < GetMaxInFlight()) {
        InFlightLimit = 2 * InFlightLimit;
    }
    AFL_VERIFY(Cleared || Aborted || FetchingInFlightSources.erase(source->GetSourceId()))("source_id", source->GetSourceId());
}

TScanWithLimitCollection::TScanWithLimitCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : TBase(context, std::move(sourcesConstructor))
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust()) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple
