#include "ttl.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap {

void TTTLColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "eviction=" << PortionsToEvict.size() << ";";
}

void TTTLColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    Y_ABORT_UNLESS(PortionsToEvict.size() || PortionsToRemove.size());
    THashMap<TString, THashSet<TBlobRange>> blobRanges;
    auto& index = self.GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
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
    self.BackgroundController.StartTtl();
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishTtl();
}

std::optional<TPortionInfoWithBlobs> TTTLColumnEngineChanges::UpdateEvictedPortion(TPortionForEviction& info, NBlobOperations::NRead::TCompositeReadBlobs& srcBlobs,
    TConstructionContext& context) const {
    const TPortionInfo& portionInfo = info.GetPortionInfo();
    auto& evictFeatures = info.GetFeatures();
    Y_ABORT_UNLESS(portionInfo.GetMeta().GetTierName() != evictFeatures.TargetTierName);

    auto* tiering = Tiering.FindPtr(evictFeatures.PathId);
    Y_ABORT_UNLESS(tiering);
    auto serializer = tiering->GetSerializer(evictFeatures.TargetTierName);
    auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetMinSnapshot());
    auto portionWithBlobs = TPortionInfoWithBlobs::RestorePortion(portionInfo, srcBlobs, blobSchema->GetIndexInfo(), SaverContext.GetStoragesManager());
    portionWithBlobs.GetPortionInfo().MutableMeta().SetTierName(evictFeatures.TargetTierName);
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    TSaverContext saverContext(SaverContext.GetStoragesManager());
    if (serializer) {
        saverContext.SetExternalSerializer(*serializer);
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portion_for_eviction", portionInfo.DebugString());
    return portionWithBlobs.ChangeSaver(resultSchema, saverContext);
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
