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

<<<<<<< HEAD
    while (Iterators.size() && (!nextInHeap || !(Iterators.front() < *nextInHeap))) {
        if (!Iterators.front().IsFilled()) {
            return false;
        }
        std::pop_heap(Iterators.begin(), Iterators.end());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "LimitIteratorNext")("source_id", Iterators.back().GetSourceId())(
            "fetched", FetchedCount)("limit", Limit)("iterators", Iterators.size());
        if (!Iterators.back().Next()) {
            Iterators.pop_back();
=======
    while (FilledIterators.size() &&
        (!nextInHeap || FilledIterators.front().ComparePrefix(*nextInHeap, *PKPrefixSize) == std::partial_ordering::less) &&
        (!UnfilledIterators.size() || FilledIterators.front().ComparePrefix(UnfilledIterators.front(), *PKPrefixSize) == std::partial_ordering::less)) {

        std::pop_heap(FilledIterators.begin(), FilledIterators.end());

        if (!FilledIterators.back().Next()) {
            FilledIterators.pop_back();
>>>>>>> 48e2293186d (Order by pk with limit final fix (#33610))
        } else {
            std::push_heap(FilledIterators.begin(), FilledIterators.end());
        }
        if (++FetchedCount >= Limit) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<NCommon::IDataSource> TSyncPointLimitControl::OnAddSource(const std::shared_ptr<NCommon::IDataSource>& source) {
    AFL_VERIFY(FetchedCount < Limit)("fetched", FetchedCount)("limit", Limit);
    UnfilledIterators.emplace_back(TSourceIterator(source));

    return TBase::OnAddSource(source);
}

ISyncPoint::ESourceAction TSyncPointLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) {
    if (FetchedCount >= Limit) {
        return ESourceAction::Finish;
    }
<<<<<<< HEAD
    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source->GetStageResult().GetBatch();
    AFL_VERIFY(Iterators.size());
<<<<<<< HEAD
    AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId())("front", Iterators.front().DebugString())("source",
                                                    source->GetAs<TPortionDataSource>()->GetStart().DebugString())("source_id", source->GetSourceId());
    std::pop_heap(Iterators.begin(), Iterators.end());
    if (!g || !g->GetRecordsCount()) {
        Iterators.pop_back();
    } else {
=======
    if (Iterators.front().GetSourceId() != source->GetSourceId()) {
        for (auto it : Iterators) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("Iterator", it.DebugString());
=======

    AFL_VERIFY(UnfilledIterators.size());

    if (UnfilledIterators.front().GetSourceIdx() != source->GetSourceIdx()) {
        for (auto it : UnfilledIterators) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("UnfilledIterators", it.DebugString());
>>>>>>> 48e2293186d (Order by pk with limit final fix (#33610))
        }
        for (auto it : FilledIterators) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("FilledIterators", it.DebugString());
        }
        for (auto it : SourcesSequentially) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("SourcesSequentially", it->GetSourceId());
        }
<<<<<<< HEAD
        if (FindIf(Iterators, [&](const auto& item) { return item.GetSourceId() == source->GetSourceId(); }) != Iterators.end()) {
            AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId())("issue #28037", "portion is in heap")
                ("front", Iterators.front().DebugString())
                ("back", Iterators.back().DebugString())
                ("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())
                ("source_id", source->GetSourceId());
        }
        else if (FindIf(DebugOrder, [&](const auto& item) { return item.GetSourceId() == source->GetSourceId(); }) != DebugOrder.end()) {
            AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId())("issue #28037", "known portion, not in heap")
                ("front", Iterators.front().DebugString())
                ("back", Iterators.back().DebugString())
                ("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())
                ("source_id", source->GetSourceId());
        }
        else {
            AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId())("issue #28037", "unknown portion")
                ("front", Iterators.front().DebugString())
                ("back", Iterators.back().DebugString())
                ("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())
                ("source_id", source->GetSourceId());
=======
        if (FindIf(UnfilledIterators, [&](const auto& item) {
                return item.GetSourceIdx() == source->GetSourceIdx();
            }) != UnfilledIterators.end()) {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in UnfilledIterators")("front", UnfilledIterators.front().DebugString())
                ("back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())("source_idx", source->GetSourceIdx());
        } else if (FindIf(FilledIterators, [&](const auto& item) {
                return item.GetSourceIdx() == source->GetSourceIdx();
            }) != FilledIterators.end()) {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in FilledIterators")("front", UnfilledIterators.front().DebugString())
                ("back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())("source_idx", source->GetSourceIdx());
        } else {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "unknown portion")("front", UnfilledIterators.front().DebugString())
                ("back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())("source_idx", source->GetSourceIdx());
>>>>>>> 48e2293186d (Order by pk with limit final fix (#33610))
        }
    }

    UnfilledIterators.pop_front();

    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source->GetStageResult().GetBatch();

    if (g && g->GetRecordsCount()) {
>>>>>>> e8c978d1427 (BACKPORT-CONFLICT: manual resolution required for commit 48e2293)
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
        FilledIterators.emplace_back(arrs, source->GetStageResult().GetNotAppliedFilter(), source);
        AFL_VERIFY(FilledIterators.back().IsFilled());
        std::push_heap(FilledIterators.begin(), FilledIterators.end());
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
    sb << "start=" << Source->GetAs<TPortionDataSource>()->GetStart().DebugString() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
