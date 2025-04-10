#include "limit_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IDataSource> TScanWithLimitCollection::DoExtractNext() {
    AFL_VERIFY(HeapSources.size());
    std::pop_heap(HeapSources.begin(), HeapSources.end());
    auto result = NextSource ? NextSource : HeapSources.back().Construct(Context);
    AFL_VERIFY(FetchingInFlightSources.emplace(result->GetSourceId()).second);
    HeapSources.pop_back();
    if (!NextSource) {
        Iterators.emplace_back(TSourceIterator(result));
    }
    std::push_heap(Iterators.begin(), Iterators.end());

    if (HeapSources.size()) {
        NextSource = HeapSources.front().Construct(Context);
        Iterators.emplace_back(TSourceIterator(NextSource));
        std::push_heap(Iterators.begin(), Iterators.end());
    } else {
        NextSource = nullptr;
    }

    return result;
}

void TScanWithLimitCollection::DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceFinished")("source_id", source->GetSourceId())("fetched", FetchedCount)(
        "limit", Limit)("max", GetMaxInFlight())("in_flight_limit", InFlightLimit)("count", FetchingInFlightSources.size());
    if (!source->GetResultRecordsCount() && InFlightLimit < GetMaxInFlight()) {
        InFlightLimit = 2 * InFlightLimit;
    }
    WaitingToFinish.erase(source->GetSourceId());
    AFL_VERIFY(FetchingInFlightSources.erase(source->GetSourceId()) || Limit <= FetchedCount);
}

TScanWithLimitCollection::TScanWithLimitCollection(
    const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources, const std::shared_ptr<IScanCursor>& cursor)
    : TBase(context)
    , Limit((ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust()) {
    if (cursor && cursor->IsInitialized()) {
        for (auto&& i : sources) {
            bool usage = false;
            if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(i, usage)) {
                sources.pop_front();
                continue;
            }
            if (usage) {
                i.SetIsStartedByCursor();
            }
            break;
        }
    }

    HeapSources = std::move(sources);
    std::make_heap(HeapSources.begin(), HeapSources.end());
}

void TScanWithLimitCollection::DoOnSourceCheckLimit(const std::shared_ptr<IDataSource>& source) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceCheckLimit")("source_id", source->GetSourceId())("fetched", FetchedCount)(
        "limit", Limit);
    AFL_VERIFY(source->IsSyncSection());
    if (FetchedCount >= Limit) {
        AFL_VERIFY(IsFinished());
        return;
    }
    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = *source->GetStageResult().GetBatch();
    AFL_VERIFY(g.GetRecordsCount());
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrs;
    for (auto&& i : rk.fields()) {
        auto acc = g.GetAccessorByNameOptional(i->name());
        if (!acc) {
            break;
        }
        arrs.emplace_back(acc);
    }
    AFL_VERIFY(arrs.size());
    if (!PKPrefixSize) {
        PKPrefixSize = arrs.size();
    } else {
        AFL_VERIFY(*PKPrefixSize == arrs.size())("prefix", PKPrefixSize)("arr", arrs.size());
    }
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceCheckLimitFillIterator")("source_id", source->GetSourceId())("fetched", FetchedCount)(
        "limit", Limit);
    AFL_VERIFY(FilledIterators.emplace(source->GetSourceId(),
            TSourceIterator(arrs, source->GetStageResult().GetNotAppliedFilter(), source)).second);
    DrainToLimit();
}

void TScanWithLimitCollection::DrainToLimit() {
    while (Iterators.size()) {
        if (!Iterators.front().IsFilled()) {
            auto it = FilledIterators.find(Iterators.front().GetSourceId());
            if (it == FilledIterators.end()) {
                if (HeapSources.size() && HeapSources.front().GetSourceId() == Iterators.front().GetSourceId()) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "UnExistsFilledIterator")(
                        "source_id", Iterators.front().GetSourceId())("fetched", FetchedCount)("limit", Limit);
                    HeapSources.front().SetNeedForceToExtract();
                    break;
                } else if (Iterators.front().GetSource()->IsSyncSection()) {
                    AFL_VERIFY(Iterators.front().GetSource()->GetStageResult().IsEmpty())("source_id", Iterators.front().GetSourceId());
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "UnExistsFilledIterator")(
                        "source_id", Iterators.front().GetSourceId())("fetched", FetchedCount)("limit", Limit);
                    std::pop_heap(Iterators.begin(), Iterators.end());
                    Iterators.pop_back();
                } else {
                    break;
                }
            } else {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "RemoveFilledIterator")("source_id", Iterators.front().GetSourceId())(
                    "fetched", FetchedCount)("limit", Limit);
                std::pop_heap(Iterators.begin(), Iterators.end());
                AFL_VERIFY(it->second.IsFilled());
                Iterators.back() = std::move(it->second);
                AFL_VERIFY(Iterators.back().IsValid());
                std::push_heap(Iterators.begin(), Iterators.end());
                FilledIterators.erase(it);
            }
        } else {
            std::pop_heap(Iterators.begin(), Iterators.end());
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "LimitIteratorNext")("source_id", Iterators.back().GetSourceId())(
                "fetched", FetchedCount)("limit", Limit)("max", GetMaxInFlight())("in_flight_limit", InFlightLimit)(
                "count", FetchingInFlightSources.size())("iterators", Iterators.size());
            if (Iterators.back().StartWithLimit()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "AddToWait")("source_id", Iterators.back().GetSourceId())(
                    "fetched", FetchedCount)("limit", Limit)("max", GetMaxInFlight())("in_flight_limit", InFlightLimit)(
                    "count", FetchingInFlightSources.size())("iterators", Iterators.size());
                WaitingToFinish.emplace(Iterators.back().GetSourceId());
            }
            if (!Iterators.back().Next()) {
                Iterators.pop_back();
            } else {
                std::push_heap(Iterators.begin(), Iterators.end());
                if (++FetchedCount >= Limit) {
                    Clear();
                    break;
                }
            }
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
