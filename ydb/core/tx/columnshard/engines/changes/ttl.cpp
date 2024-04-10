#include "ttl.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap {

void TTTLColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "eviction=" << PortionsToEvict.size() << ";address=" << RWAddress.DebugString() << ";";
}

void TTTLColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    Y_ABORT_UNLESS(PortionsToEvict.size() || PortionsToRemove.size());
    THashMap<TString, THashSet<TBlobRange>> blobRanges;
    auto& engine = self.MutableIndexAs<TColumnEngineForLogs>();
    auto& index = engine.GetVersionedIndex();
    for (const auto& p : PortionsToEvict) {
        Y_ABORT_UNLESS(!p.GetPortionInfo().Empty());
        p.GetPortionInfo().FillBlobRangesByStorage(blobRanges, index);
    }
    for (auto&& i : blobRanges) {
        auto action = BlobsAction.GetReading(i.first);
        for (auto&& b : i.second) {
            action->AddRange(b);
        }
    }
    engine.GetActualizationController()->StartActualization(RWAddress);
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    auto& engine = self.MutableIndexAs<TColumnEngineForLogs>();
    engine.GetActualizationController()->FinishActualization(RWAddress);
    if (IsAborted()) {
        THashMap<ui64, THashSet<ui64>> restoreIndexAddresses;
        for (auto&& i : PortionsToEvict) {
            AFL_VERIFY(restoreIndexAddresses[i.GetPortionInfo().GetPathId()].emplace(i.GetPortionInfo().GetPortionId()).second);
        }
        for (auto&& i : PortionsToRemove) {
            AFL_VERIFY(restoreIndexAddresses[i.first.GetPathId()].emplace(i.first.GetPortionId()).second);
        }
        engine.ReturnToIndexes(restoreIndexAddresses);
    }
}

std::optional<TPortionInfoWithBlobs> TTTLColumnEngineChanges::UpdateEvictedPortion(TPortionForEviction& info, NBlobOperations::NRead::TCompositeReadBlobs& srcBlobs,
    TConstructionContext& context) const
{
    const TPortionInfo& portionInfo = info.GetPortionInfo();
    auto& evictFeatures = info.GetFeatures();
    auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetMinSnapshot());
    Y_ABORT_UNLESS(portionInfo.GetMeta().GetTierName() != evictFeatures.GetTargetTierName() || blobSchema->GetVersion() < evictFeatures.GetTargetScheme()->GetVersion());

    auto portionWithBlobs = TPortionInfoWithBlobs::RestorePortion(portionInfo, srcBlobs, blobSchema->GetIndexInfo(), SaverContext.GetStoragesManager());
    TPortionInfoWithBlobs result = TPortionInfoWithBlobs::SyncPortion(
        std::move(portionWithBlobs), blobSchema, evictFeatures.GetTargetScheme(), evictFeatures.GetTargetTierName(), SaverContext.GetStoragesManager(), context.Counters.SplitterCounters);

    result.GetPortionInfo().MutableMeta().SetTierName(evictFeatures.GetTargetTierName());
    return std::move(result);
}

NKikimr::TConclusionStatus TTTLColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(!Blobs.IsEmpty());
    Y_ABORT_UNLESS(!PortionsToEvict.empty());

    for (auto&& info : PortionsToEvict) {
        if (auto pwb = UpdateEvictedPortion(info, Blobs, context)) {
            info.MutablePortionInfo().SetRemoveSnapshot(info.MutablePortionInfo().GetMinSnapshot());
            AFL_VERIFY(PortionsToRemove.emplace(info.GetPortionInfo().GetAddress(), info.GetPortionInfo()).second);
            AppendedPortions.emplace_back(std::move(*pwb));
        }
    }

    PortionsToEvict.clear();
    return TConclusionStatus::Success();
}

NColumnShard::ECumulativeCounters TTTLColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_TTL_SUCCESS : NColumnShard::COUNTER_TTL_FAIL;
}

}
