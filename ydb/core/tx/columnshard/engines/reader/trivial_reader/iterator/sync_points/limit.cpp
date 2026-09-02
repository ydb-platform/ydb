#include "limit.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/collections/limit_sorted.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NTrivial {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

namespace {
ui32 GetSysViewMaxHeldPortions() {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetLimitSyncPointConfig().GetSysViewMaxHeldPortions() : 0;
}
}   // namespace

TSyncPointLimitControl::TSyncPointLimitControl(const ui32 limit, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
    const std::shared_ptr<TScanWithLimitCollection>& collection)
    : TBase(pointIndex, "SYNC_LIMIT", context, collection)
    , Limit(limit)
    , Collection(collection)
    , SysViewMaxHeldPortions(GetSysViewMaxHeldPortions())
{
    AFL_VERIFY(Collection);
}

bool TSyncPointLimitControl::DrainToLimit() {
    std::optional<TSourceIterator> nextInHeap;
    if (Collection->GetNextSource()) {
        nextInHeap = TSourceIterator(Collection->GetNextSource());
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

std::shared_ptr<NCommon::IDataSource> TSyncPointLimitControl::OnAddSource(const std::shared_ptr<NCommon::IDataSource>& source) {
    if (!Passthrough) {
        AFL_VERIFY(FetchedCount < Limit)("fetched", FetchedCount)("limit", Limit);
        UnfilledIterators.emplace_back(TSourceIterator(source));
    }

    return TBase::OnAddSource(source);
}

ISyncPoint::ESourceAction TSyncPointLimitControl::OnSourceReady(
    const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) {
    const NActors::TLogContextGuard verifyContext =
        NActors::TLogContextBuilder::Build()("source_schema", source->GetSourceSchema()->DebugString());
    LWTRACK(LimitSyncPoint, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(), source->GetTxId(),
        source->GetDeprecatedPortionId(), GetPointName(), source->GetFilteredRowsCount(), source->GetReservedMemory(),
        source->GetSourcesAheadQueueWaitDuration(), source->GetSourcesAhead(), DebugString());
    if (FetchedCount >= Limit) {
        return ESourceAction::Finish;
    }

    if (Passthrough) {
        return source->GetStageResult().IsEmpty() ? ESourceAction::Finish : ESourceAction::ProvideNext;
    }

    AFL_VERIFY(UnfilledIterators.size());

    if (UnfilledIterators.front().GetSourceIdx() != source->GetSourceIdx()) {
        for (auto it : UnfilledIterators) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"unfilledIterators", it.DebugString()});
        }
        for (auto it : FilledIterators) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"filledIterators", it.DebugString()});
        }
        for (auto it : SourcesSequentially) {
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"sourcesSequentially", it->GetSourceIdx()});
        }
        if (FindIf(UnfilledIterators, [&](const auto& item) {
                return item.GetSourceIdx() == source->GetSourceIdx();
            }) != UnfilledIterators.end()) {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in UnfilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source->GetSourceIdx());
        } else if (FindIf(FilledIterators, [&](const auto& item) {
                       return item.GetSourceIdx() == source->GetSourceIdx();
                   }) != FilledIterators.end()) {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "portion is in FilledIterators")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source->GetSourceIdx());
        } else {
            AFL_VERIFY(UnfilledIterators.front().GetSourceIdx() == source->GetSourceIdx())("issue #28037", "unknown portion")("front", UnfilledIterators.front().DebugString())(
                    "back", UnfilledIterators.back().DebugString())("source", source->GetAs<IDataSource>()->GetFirstPK().DebugString())(
                    "source_idx", source->GetSourceIdx());
        }
    }

    UnfilledIterators.pop_front();

    const auto& rk = *Context->GetReadMetadata()->GetResultSchema()->GetIndexInfo().GetReplaceKey();
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "DoOnSourceCheckLimitFillIterator"},
            {"sourceIdx", source->GetSourceIdx()},
            {"fetched", FetchedCount},
            {"limit", Limit});
        FilledIterators.emplace_back(arrs, source->GetStageResult().GetNotAppliedFilter(), source);
        AFL_VERIFY(FilledIterators.back().IsFilled());
        std::push_heap(FilledIterators.begin(), FilledIterators.end());
    }
    // SysViewMaxHeldPortions is the max portions we reorder here; the next one trips passthrough (transient peak +1
    // before the clear below). Once we stop reordering, correctness relies on KQP's per-shard TopSort re-sorting/re-limiting.
    if (SysViewMaxHeldPortions && source->GetType() == IDataSource::EType::SimpleSysInfo && FilledIterators.size() > SysViewMaxHeldPortions) {
        Passthrough = true;
        // FetchedCount is intentionally abandoned here: in passthrough it never gates emission again (KQP re-limits)
        FilledIterators.clear();
        UnfilledIterators.clear();
        NYDBTest::TControllers::GetColumnShardController()->OnSysViewLimitSyncPointPassthrough();
    }
    if (!Passthrough && DrainToLimit()) {
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
    sb << "start=" << Source->GetAs<IDataSource>()->GetFirstPK().DebugString() << ";";
    sb << "finish=" << Source->GetAs<IDataSource>()->GetLastPK().DebugString() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
