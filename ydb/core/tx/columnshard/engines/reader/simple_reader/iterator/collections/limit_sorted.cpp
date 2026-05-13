#include "limit_sorted.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<NCommon::IDataSource> TScanWithLimitCollection::DoTryExtractNext() {
    if (!NextSource) {
        if (!SourcesConstructor->IsFinished()) {
            NextSource = SourcesConstructor->TryExtractNext(Context, InFlightLimit);
            if (!NextSource) {
                YDB_LOG_DEBUG("",
                    {"event", "DoTryExtractNextSkip"});
                return nullptr;
            }
        }
    }
    {
        std::shared_ptr<NCommon::IDataSource> localNext;
        if (!SourcesConstructor->IsFinished()) {
            localNext = SourcesConstructor->TryExtractNext(Context, InFlightLimit);
            if (!localNext) {
                YDB_LOG_DEBUG("",
                    {"event", "DoTryExtractNextSkip"});
                return nullptr;
            }
        } else {
            localNext = nullptr;
        }
        auto result = std::move(NextSource);
        NextSource = std::move(localNext);
        AFL_VERIFY(Cleared || Aborted || GetSourcesInFlightCount() == FetchingInFlightSources.size())("in_flight",
                                                                    GetSourcesInFlightCount())("fetching", FetchingInFlightSources.size());
        AFL_VERIFY(FetchingInFlightSources.emplace(result->GetSourceIdx()).second);
        YDB_LOG_DEBUG("",
            {"event", "DoTryExtractNext"},
            {"source_idx", result->GetSourceIdx()});
        return result;
    }
}

void TScanWithLimitCollection::DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) {
    YDB_LOG_DEBUG("",
        {"event", "DoOnSourceFinished"},
        {"source_idx", source->GetSourceIdx()},
        {"limit", Limit},
        {"max", GetMaxInFlight()},
        {"in_flight_limit", InFlightLimit},
        {"count", GetSourcesInFlightCount()});
    if (source->GetAs<IDataSource>()->GetResultRecordsCount() < Limit && InFlightLimit < GetMaxInFlight()) {
        InFlightLimit = Min(2 * InFlightLimit, GetMaxInFlight());
    }
    AFL_VERIFY(Cleared || Aborted || GetSourcesInFlightCount() == FetchingInFlightSources.size())("in_flight", GetSourcesInFlightCount())("fetching",
                                                                FetchingInFlightSources.size());
    AFL_VERIFY(FetchingInFlightSources.erase(source->GetSourceIdx()) || Cleared || Aborted)("source_idx", source->GetSourceIdx());
}

TScanWithLimitCollection::TScanWithLimitCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : TBase(context, std::move(sourcesConstructor))
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())
{
    if (HasAppData()) {
        InFlightLimit = AppData()->ColumnShardConfig.GetLimitSortedStartInFlight();
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
