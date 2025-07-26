#include "limit_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IDataSource> TScanWithLimitCollection::DoExtractNext() {
    auto result = NextSource ? NextSource : static_pointer_cast<IDataSource>(SourcesConstructor->ExtractNext(Context));
    AFL_VERIFY(FetchingInFlightSources.emplace(result->GetSourceId()).second);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractNext")("source_id", result->GetSourceId());
    if (!SourcesConstructor->IsFinished()) {
        NextSource = static_pointer_cast<IDataSource>(SourcesConstructor->ExtractNext(Context));
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
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : TBase(context)
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())
    , SourcesConstructor(std::move(sourcesConstructor)) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple
