#include "ordered_result_with_limit.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NTrivial {

std::unique_ptr<NCommon::TDataSourceLease> TOrderedResultWithLimitCollection::DoTryExtractNext() {
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
        std::unique_ptr<NCommon::TDataSourceLease> localNext;
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
        const ui32 sourceIdx = result->GetSource().GetSourceIdx();
        AFL_VERIFY(Cleared || Aborted || GetSourcesInFlightCount() <= FetchingInFlightSources.size())("in_flight",
                                                                    GetSourcesInFlightCount())("fetching", FetchingInFlightSources.size());
        AFL_VERIFY(FetchingInFlightSources.emplace(sourceIdx).second);
        YDB_LOG_DEBUG("",
            {"event", "DoTryExtractNext"},
            {"sourceIdx", sourceIdx});
        return result;
    }
}

void TOrderedResultWithLimitCollection::DoOnSourceFinished(const NCommon::IDataSource& source) {
    YDB_LOG_DEBUG("",
        {"event", "DoOnSourceFinished"},
        {"sourceIdx", source.GetSourceIdx()},
        {"limit", Limit},
        {"max", GetMaxInFlight()},
        {"inFlightLimit", InFlightLimit},
        {"count", GetSourcesInFlightCount()});
    if (source.GetAs<IDataSource>()->GetResultRecordsCount() < Limit && InFlightLimit < GetMaxInFlight()) {
        InFlightLimit = Min(2 * InFlightLimit, GetMaxInFlight());
    }
    AFL_VERIFY(Cleared || Aborted || GetSourcesInFlightCount() <= FetchingInFlightSources.size())("in_flight", GetSourcesInFlightCount())("fetching",
                                                                FetchingInFlightSources.size());
    AFL_VERIFY(FetchingInFlightSources.erase(source.GetSourceIdx()) || Cleared || Aborted)("source_idx", source.GetSourceIdx());
}

TOrderedResultWithLimitCollection::TOrderedResultWithLimitCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
    : TBase(context, std::move(sourcesConstructor))
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())
{
    if (HasAppData()) {
        InFlightLimit = AppData()->ColumnShardConfig.GetLimitSortedStartInFlight();
    }
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
