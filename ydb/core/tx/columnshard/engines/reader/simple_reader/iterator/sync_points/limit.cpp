#include "limit.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/limit_sorted.h>

namespace NKikimr::NOlap::NReader::NSimple {

TSyncPointLimitControl::TSyncPointLimitControl(const ui32 limit, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
    const std::shared_ptr<TScanWithLimitCollection>& collection)
    : TBase(pointIndex, "SYNC_LIMIT", context, collection)
    , Limit(limit)
    , Collection(collection) {
    AFL_VERIFY(Collection);
}

bool TSyncPointLimitControl::DrainToLimit() {
    std::optional<TSourceIterator> nextInHeap;
    if (Collection->GetNextSource()) {
        nextInHeap = TSourceIterator(Collection->GetNextSource());
    }
    if (Iterators.empty() || (nextInHeap && Iterators.front() < *nextInHeap)) {
        return false;
    }

    while (Iterators.size()) {
        if (!Iterators.front().IsFilled()) {
            return false;
        }
        std::pop_heap(Iterators.begin(), Iterators.end());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "LimitIteratorNext")("source_id", Iterators.back().GetSourceId())(
            "fetched", FetchedCount)("limit", Limit)("iterators", Iterators.size());
        if (!Iterators.back().Next()) {
            Iterators.pop_back();
        } else {
            std::push_heap(Iterators.begin(), Iterators.end());
            if (++FetchedCount >= Limit) {
                return true;
            }
        }
    }
    return false;
}

ISyncPoint::ESourceAction TSyncPointLimitControl::OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) {
    if (FetchedCount >= Limit) {
        return ESourceAction::Finish;
    }
    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source->GetStageResult().GetBatch();
    AFL_VERIFY(Iterators.size());
    AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId())("front", Iterators.front().DebugString())("source",
                                                    source->GetAs<IDataSource>()->GetStart().DebugString())("source_id", source->GetSourceId());
    std::pop_heap(Iterators.begin(), Iterators.end());
    if (!g || !g->GetRecordsCount()) {
        Iterators.pop_back();
    } else {
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrs;
        for (auto&& i : rk.fields()) {
            auto acc = g->GetAccessorByNameOptional(i->name());
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceCheckLimitFillIterator")("source_id", source->GetSourceId())(
            "fetched", FetchedCount)("limit", Limit);
        Iterators.back() = TSourceIterator(arrs, source->GetStageResult().GetNotAppliedFilter(), source);
        AFL_VERIFY(Iterators.back().IsFilled());
        std::push_heap(Iterators.begin(), Iterators.end());
    }
    if (DrainToLimit()) {
        Collection->Clear();
    }
    if (source->GetStageResult().IsEmpty()) {
        return ESourceAction::Finish;
    } else {
        return ESourceAction::ProvideNext;
    }
}

TString TSyncPointLimitControl::TSourceIterator::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "id=" << Source->GetSourceId() << ";";
    sb << "f=" << IsFilled() << ";";
    sb << "record=" << SortableRecord->DebugJson() << ";";
    sb << "start=" << Source->GetAs<IDataSource>()->GetStart().DebugString() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
