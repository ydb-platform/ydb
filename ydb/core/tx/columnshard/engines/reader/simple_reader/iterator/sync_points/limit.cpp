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

    while (FilledIterators.size() &&
        (!nextInHeap || FilledIterators.front().ComparePrefix(*nextInHeap, *PKPrefixSize) == std::partial_ordering::less) &&
        (!UnfilledIterators.size() || FilledIterators.front().ComparePrefix(UnfilledIterators.front(), *PKPrefixSize) == std::partial_ordering::less)) {

        std::pop_heap(FilledIterators.begin(), FilledIterators.end());

        if (!FilledIterators.back().Next()) {
            FilledIterators.pop_back();
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
    if (FetchedCount < Limit) {
        UnfilledIterators.emplace_back(TSourceIterator(source));
    }

    return TBase::OnAddSource(source);
}

ISyncPoint::ESourceAction TSyncPointLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) {
    if (FetchedCount >= Limit) {
        return ESourceAction::Finish;
    }

    AFL_VERIFY(UnfilledIterators.begin()->GetSourceId() == source->GetSourceId())("unexpected_source", source->GetSourceId());
    UnfilledIterators.pop_front();

    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source->GetStageResult().GetBatch();

    if (g && g->GetRecordsCount()) {
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
    sb << "finish=" << Source->GetAs<TPortionDataSource>()->GetFinish().DebugString() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
