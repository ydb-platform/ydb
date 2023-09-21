#include "ttl.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap {

void TTTLColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    if (PortionsToEvict.size()) {
        out << "eviction=(count=" << PortionsToEvict.size() << ";portions=[";
        for (auto& info : PortionsToEvict) {
            out << info.GetPortionInfo() << ";to=" << info.GetFeatures().TargetTierName << ";";
        }
        out << "];";
    }
}

void TTTLColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    Y_VERIFY(PortionsToEvict.size() || PortionsToRemove.size());
    for (const auto& p : PortionsToEvict) {
        Y_VERIFY(!p.GetPortionInfo().Empty());
        PortionsToRemove.emplace_back(p.GetPortionInfo());

        auto agent = BlobsAction.GetReading(p.GetPortionInfo());
        for (const auto& rec : p.GetPortionInfo().Records) {
            agent->AddRange(rec.BlobRange);
        }
    }
    self.BackgroundController.StartTtl(*this);
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishTtl();
}

std::optional<TPortionInfoWithBlobs> TTTLColumnEngineChanges::UpdateEvictedPortion(TPortionForEviction& info, const THashMap<TBlobRange, TString>& srcBlobs,
    TConstructionContext& context) const {
    const TPortionInfo& portionInfo = info.GetPortionInfo();
    auto& evictFeatures = info.GetFeatures();
    Y_VERIFY(portionInfo.GetMeta().GetTierName() != evictFeatures.TargetTierName);

    auto* tiering = Tiering.FindPtr(evictFeatures.PathId);
    Y_VERIFY(tiering);
    auto compression = tiering->GetCompression(evictFeatures.TargetTierName);
    if (!compression) {
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
    auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, srcBlobs);

    TSaverContext saverContext(evictFeatures.StorageOperator, SaverContext.GetStoragesManager());
    saverContext.SetTierName(evictFeatures.TargetTierName).SetExternalCompression(compression);
    auto withBlobs = TPortionInfoWithBlobs::RestorePortion(portionInfo, srcBlobs);
    withBlobs.GetPortionInfo().InitOperator(evictFeatures.StorageOperator, true);
    withBlobs.GetPortionInfo().MutableMeta().SetTierName(evictFeatures.TargetTierName);
    return withBlobs.ChangeSaver(resultSchema, saverContext);
}

NKikimr::TConclusionStatus TTTLColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_VERIFY(!Blobs.empty());
    Y_VERIFY(!PortionsToEvict.empty());

    for (auto&& info : PortionsToEvict) {
        if (auto pwb = UpdateEvictedPortion(info, Blobs, context)) {
            PortionsToRemove.emplace_back(info.GetPortionInfo());
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
