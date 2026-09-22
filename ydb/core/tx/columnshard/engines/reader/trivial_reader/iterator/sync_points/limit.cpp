#include "limit.h"

#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/collections/ordered_result_with_limit.h>

namespace NKikimr::NOlap::NReader::NTrivial {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

TSyncPointLimitControl::TSyncPointLimitControl(const ui32 limit, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
    const std::shared_ptr<TOrderedResultWithLimitCollection>& collection)
    : TBase(pointIndex, "SYNC_LIMIT", context, collection)
    , Limit(limit)
    , Collection(collection)
{
    AFL_VERIFY(Collection);
}

bool TSyncPointLimitControl::DrainToLimit() {
    std::optional<TSourceIterator> nextInHeap;
    if (const auto& nextSource = Collection->GetNextSource()) {
        nextInHeap = TSourceIterator(nextSource->ShareReadOnly());
    }

    while (FilledIterators.size() &&
           (!nextInHeap || FilledIterators.front().ComparePrefix(*nextInHeap, *PKPrefixSize) == std::partial_ordering::less) &&
           (!UnfilledIterators.size() ||
               FilledIterators.front().ComparePrefix(UnfilledIterators.front(), *PKPrefixSize) == std::partial_ordering::less)) {
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

std::unique_ptr<NCommon::TDataSourceLease> TSyncPointLimitControl::OnAddSource(std::unique_ptr<NCommon::TDataSourceLease> lease) {
    AFL_VERIFY(FetchedCount < Limit)("fetched", FetchedCount)("limit", Limit);
    UnfilledIterators.emplace_back(TSourceIterator(lease->ShareReadOnly()));

    return TBase::OnAddSource(std::move(lease));
}

ISyncPoint::ESourceAction TSyncPointLimitControl::OnSourceReady(const NCommon::TDataSourceLease& lease, TPlainReadData& /*reader*/) {
    auto& source = lease.GetSource();
    const NActors::TLogContextGuard verifyContext =
        NActors::TLogContextBuilder::Build()("source_schema", source.GetSourceSchema()->DebugString());
    LWTRACK(LimitSyncPoint, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        GetPointName(), source.GetFilteredRowsCount(), source.GetReservedMemory(), source.GetSourcesAheadQueueWaitDuration(),
        source.GetSourcesAhead(), DebugString());
    if (FetchedCount >= Limit) {
        return ESourceAction::Finish;
    }

    AFL_VERIFY(UnfilledIterators.size());

    if (UnfilledIterators.front().GetSourceIdx() != source.GetSourceIdx()) {
        for (auto it : UnfilledIterators) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"unfilledIterators", it.DebugString()});
        }
        for (auto it : FilledIterators) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"filledIterators", it.DebugString()});
        }
        for (const auto& it : SourcesSequentially) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"sourcesSequentially", it.SourceIdx});
        }
        if (FindIf(UnfilledIterators, [&](const auto& item) {
                return item.GetSourceIdx() == source.GetSourceIdx();
            }) != UnfilledIterators.end()) {
<<<<<<< HEAD
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in UnfilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())(
                    "source_idx", source->GetSourceIdx());
=======
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source.GetSourceIdx())("issue #28037", "portion is in UnfilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source.GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source.GetSourceIdx());
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
        } else if (FindIf(FilledIterators, [&](const auto& item) {
                       return item.GetSourceIdx() == source.GetSourceIdx();
                   }) != FilledIterators.end()) {
<<<<<<< HEAD
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in FilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())(
                    "source_idx", source->GetSourceIdx());
        } else {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "unknown portion")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<TPortionDataSource>()->GetStart().DebugString())(
                    "source_idx", source->GetSourceIdx());
=======
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source.GetSourceIdx())("issue #28037", "portion is in FilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source.GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source.GetSourceIdx());
        } else {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source.GetSourceIdx())("issue #28037", "unknown portion")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source.GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source.GetSourceIdx());
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
        }
    }

    UnfilledIterators.pop_front();

<<<<<<< HEAD
    const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source->GetStageResult().GetBatch();

=======
    const auto& rk = *source.GetSourceSchema()->GetIndexInfo().GetReplaceKey();
    const auto& g = source.GetStageResult().GetBatch();
    bool hasRows = false;
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "DoOnSourceCheckLimitFillIterator"},
            {"sourceIdx", source.GetSourceIdx()},
            {"fetched", FetchedCount},
            {"limit", Limit});
<<<<<<< HEAD
        FilledIterators.emplace_back(arrs, source->GetStageResult().GetNotAppliedFilter(), source);
        AFL_VERIFY(FilledIterators.back().IsFilled());
        std::push_heap(FilledIterators.begin(), FilledIterators.end());
=======
        TSourceIterator iterator(arrs, source.GetStageResult().GetNotAppliedFilter(), lease.ShareReadOnly());
        AFL_VERIFY(iterator.IsFilled());
        if (iterator.IsValid()) {
            hasRows = true;
            FilledIterators.emplace_back(std::move(iterator));
            std::push_heap(FilledIterators.begin(), FilledIterators.end());
        }
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
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
    sb << "idx=" << Source->GetSourceIdx() << ";";
    sb << "f=" << IsFilled() << ";";
    sb << "record=" << SortableRecord->DebugJson() << ";";
    sb << "start=" << Source->GetAs<TPortionDataSource>()->GetStart().DebugString() << ";";
    sb << "finish=" << Source->GetAs<TPortionDataSource>()->GetFinish().DebugString() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
