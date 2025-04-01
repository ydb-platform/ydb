#include "collections.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IDataSource> TScanWithLimitCollection::DoExtractNext() {
    AFL_VERIFY(HeapSources.size());
    std::pop_heap(HeapSources.begin(), HeapSources.end());
    auto result = HeapSources.back().Construct(Context);
    AFL_VERIFY(FetchingInFlightSources.emplace(TCompareKeyForScanSequence::FromFinish(result)).second);
    auto predPosition = std::move(HeapSources.back());
    HeapSources.pop_back();
    if (HeapSources.size()) {
        FullIntervalsFetchingCount.Add(GetInFlightIntervalsCount(predPosition.GetStart(), HeapSources.front().GetStart()));
    } else {
        FullIntervalsFetchingCount = FetchingInFlightSources.size() + FinishedSources.size();
    }
    FetchingInFlightCount.Inc();
    return result;
}

void TScanWithLimitCollection::DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) {
    FetchingInFlightCount.Dec();
    AFL_VERIFY(FetchingInFlightSources.erase(TCompareKeyForScanSequence::FromFinish(source)));
    AFL_VERIFY(FinishedSources.emplace(TCompareKeyForScanSequence::FromFinish(source), TFinishedDataSource(source)).second);
    while (FinishedSources.size() && (HeapSources.empty() || FinishedSources.begin()->first < HeapSources.front().GetStart())) {
        auto finishedSource = FinishedSources.begin()->second;
        if (!finishedSource.GetRecordsCount() && InFlightLimit < GetMaxInFlight()) {
            InFlightLimit = 2 * InFlightLimit;
        }
        FetchedCount += finishedSource.GetRecordsCount();
        FinishedSources.erase(FinishedSources.begin());
        if (Context->IsActive()) {
            --FullIntervalsFetchingCount;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "source_finished")("source_id", finishedSource.GetSourceId())(
            "source_idx", finishedSource.GetSourceIdx())("limit", Limit)("fetched", finishedSource.GetRecordsCount());
        if (Limit <= FetchedCount && HeapSources.size()) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "limit_exhausted")("limit", Limit)("fetched", FetchedCount);
            HeapSources.clear();
            FullIntervalsFetchingCount = FinishedSources.size() + FetchingInFlightSources.size();
        }
    }
}

ui32 TScanWithLimitCollection::GetInFlightIntervalsCount(const TCompareKeyForScanSequence& from, const TCompareKeyForScanSequence& to) const {
    AFL_VERIFY(from < to);
    ui32 inFlightCountLocal = 0;
    {
        auto itUpperFinishedFrom = FinishedSources.upper_bound(from);
        auto itUpperFinishedTo = FinishedSources.upper_bound(to);
        for (auto&& it = itUpperFinishedFrom; it != itUpperFinishedTo; ++it) {
            ++inFlightCountLocal;
        }
    }
    {
        auto itUpperFetchingFrom = FetchingInFlightSources.upper_bound(from);
        auto itUpperFetchingTo = FetchingInFlightSources.upper_bound(to);
        for (auto&& it = itUpperFetchingFrom; it != itUpperFetchingTo; ++it) {
            ++inFlightCountLocal;
        }
    }
    return inFlightCountLocal;
}

TScanWithLimitCollection::TScanWithLimitCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources)
    : TBase(context)
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust()) {
    HeapSources = std::move(sources);
    std::make_heap(HeapSources.begin(), HeapSources.end());
}

ISourcesCollection::ISourcesCollection(const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
