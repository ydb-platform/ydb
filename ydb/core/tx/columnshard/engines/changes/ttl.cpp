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
    for (const auto& p : PortionsToEvict) {
        Y_ABORT_UNLESS(!p.GetPortionInfo().Empty());

        auto agent = BlobsAction.GetReading(p.GetPortionInfo());
        for (const auto& rec : p.GetPortionInfo().Records) {
            agent->AddRange(rec.BlobRange);
        }
    }
    self.BackgroundController.StartTtl();
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishTtl();
}

std::optional<TPortionInfoWithBlobs> TTTLColumnEngineChanges::UpdateEvictedPortion(TPortionForEviction& info, THashMap<TBlobRange, TString>& srcBlobs,
    TConstructionContext& context) const {
    const TPortionInfo& portionInfo = info.GetPortionInfo();
    auto& evictFeatures = info.GetFeatures();
    Y_ABORT_UNLESS(portionInfo.GetMeta().GetTierName() != evictFeatures.TargetTierName);

    auto* tiering = Tiering.FindPtr(evictFeatures.PathId);
    Y_ABORT_UNLESS(tiering);
    auto serializer = tiering->GetSerializer(evictFeatures.TargetTierName);
    if (!serializer) {
        // Nothing to recompress. We have no other kinds of evictions yet.
        evictFeatures.DataChanges = false;
        auto result = TPortionInfoWithBlobs::RestorePortion(portionInfo, srcBlobs);
        result.GetPortionInfo().InitOperator(evictFeatures.StorageOperator, true);
        result.GetPortionInfo().MutableMeta().SetTierName(evictFeatures.TargetTierName);
        return result;
    }

    auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetMinSnapshot());
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portion_for_eviction", portionInfo.DebugString());

    TSaverContext saverContext(evictFeatures.StorageOperator, SaverContext.GetStoragesManager());
    saverContext.SetTierName(evictFeatures.TargetTierName).SetExternalSerializer(*serializer);
    auto withBlobs = TPortionInfoWithBlobs::RestorePortion(portionInfo, srcBlobs);
    withBlobs.GetPortionInfo().InitOperator(evictFeatures.StorageOperator, true);
    withBlobs.GetPortionInfo().MutableMeta().SetTierName(evictFeatures.TargetTierName);
    return withBlobs.ChangeSaver(resultSchema, saverContext);
}

NKikimr::TConclusionStatus TTTLColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(!Blobs.empty());
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
