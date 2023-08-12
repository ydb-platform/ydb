#include "ttl.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>

namespace NKikimr::NOlap {

void TTTLColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    if (ui32 evicted = PortionsToEvict.size()) {
        out << "evict " << evicted << " portions";
        for (auto& [portionInfo, evictionFeatures] : PortionsToEvict) {
            out << portionInfo << " (to " << evictionFeatures.TargetTierName << ")";
        }
        out << "; ";
    }
}

THashMap<NKikimr::NOlap::TUnifiedBlobId, std::vector<NKikimr::NOlap::TBlobRange>> TTTLColumnEngineChanges::GetGroupedBlobRanges() const {
    return GroupedBlobRanges(PortionsToEvict);
}

bool TTTLColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    if (!TBase::DoApplyChanges(self, context)) {
        return false;
    }

    for (auto& [portionInfo, _] : PortionsToEvict) {
        const ui64 granule = portionInfo.GetGranule();
        const ui64 portion = portionInfo.GetPortion();
        if (!self.IsPortionExists(granule, portion)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot evict unknown portion")("portion", portionInfo.DebugString());
            return false;
        }

        const TPortionInfo& oldInfo = self.GetGranuleVerified(granule).GetPortionVerified(portion);
        Y_VERIFY(oldInfo.IsActive());
        Y_VERIFY(portionInfo.TierName != oldInfo.TierName);

        self.UpsertPortion(portionInfo, &oldInfo);

        for (auto& record : portionInfo.Records) {
            self.ColumnsTable->Write(context.DB, portionInfo, record);
        }
    }

    return true;
}

void TTTLColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    TBase::DoWriteIndex(self, context);
    THashMap<TUnifiedBlobId, NOlap::TPortionEvictionFeatures> blobsToExport;
    THashSet<TUnifiedBlobId> protectedBlobs;

    self.IncCounter(NColumnShard::COUNTER_EVICTION_PORTIONS_WRITTEN, PortionsToEvict.size());
    for (auto& [portionInfo, evictionFeatures] : PortionsToEvict) {
        // Mark exported blobs
        if (evictionFeatures.NeedExport) {
            auto& tierName = portionInfo.TierName;
            Y_VERIFY(!tierName.empty());

            for (auto& rec : portionInfo.Records) {
                auto& blobId = rec.BlobRange.BlobId;
                if (!blobsToExport.contains(blobId)) {
                    NKikimrTxColumnShard::TEvictMetadata meta;
                    meta.SetTierName(tierName);

                    NOlap::TEvictedBlob evict{
                        .State = EEvictState::EVICTING,
                        .Blob = blobId,
                        .ExternBlob = blobId.MakeS3BlobId(evictionFeatures.PathId)
                    };
                    if (self.BlobManager->ExportOneToOne(std::move(evict), meta, *context.BlobManagerDb)) {
                        blobsToExport.emplace(blobId, evictionFeatures);
                    } else {
                        // TODO: support S3 -> S3 eviction
                        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Prevent evict evicted blob")("blob_id", blobId);
                        protectedBlobs.insert(blobId);
                    }
                }
            }
        }
    }
    // Note: RAW_BYTES_ERASED and BYTES_ERASED counters are not in sync for evicted data
    THashSet<TUnifiedBlobId> blobsToDrop;
    for (const auto& rec : EvictedRecords) {
        const auto& blobId = rec.BlobRange.BlobId;
        if (blobsToExport.contains(blobId)) {
            // Eviction to S3. TTxExportFinish will delete src blob when dst blob get EEvictState::EXTERN state.
        } else if (!protectedBlobs.contains(blobId)) {
            // We could drop the blob immediately
            if (blobsToDrop.emplace(blobId).second) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "Delete evicted blob")("blob_id", blobId);
            }

        }
    }

    if (blobsToExport.size()) {
        for (auto& [blobId, evFeatures] : blobsToExport) {
            ExportTierBlobs[evFeatures.TargetTierName][evFeatures.PathId].emplace(blobId);
        }
        blobsToExport.clear();

        ui32 numExports = 0;
        for (auto& [tierName, pathBlobs] : ExportTierBlobs) {
            numExports += pathBlobs.size();
        }

        ExportNo = self.LastExportNo;
        self.LastExportNo += numExports;

        // Do not start new TTL till we finish current tx. TODO: check if this protection needed
        Y_VERIFY(!self.ActiveEvictions, "Unexpected active evictions count at tablet %lu", self.TabletID());
        self.ActiveEvictions += numExports;

        NIceDb::TNiceDb db(context.Txc.DB);
        NColumnShard::Schema::SaveSpecialValue(db, NColumnShard::Schema::EValueIds::LastExportNumber, self.LastExportNo);
    }
}

void TTTLColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    TBase::DoCompile(context);
    for (auto& [portionInfo, _] : PortionsToEvict) {
        portionInfo.UpdateRecordsMeta(TPortionMeta::EProduced::EVICTED);
    }
}

void TTTLColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartTtl(*this);
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishTtl();
}

void TTTLColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    for (auto& [tierName, pathBlobs] : ExportTierBlobs) {
        for (auto& [pathId, blobs] : pathBlobs) {
            ++ExportNo;
            Y_VERIFY(pathId);
            auto event = std::make_unique<NColumnShard::TEvPrivate::TEvExport>(ExportNo, tierName, pathId, std::move(blobs));
            self.ExportBlobs(context.ActorContext, std::move(event));
        }
        self.ActiveEvictions -= pathBlobs.size();
    }
    if (ExportTierBlobs.size()) {
        Y_VERIFY(!self.ActiveEvictions, "Unexpected active evictions count at tablet %lu", self.TabletID());
    }

    self.IncCounter(NColumnShard::COUNTER_EVICTION_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_EVICTION_BYTES_WRITTEN, context.BytesWritten);
}

bool TTTLColumnEngineChanges::UpdateEvictedPortion(TPortionInfo& portionInfo, TPortionEvictionFeatures& evictFeatures,
    const THashMap<TBlobRange, TString>& srcBlobs, std::vector<TColumnRecord>& evictedRecords, std::vector<TString>& newBlobs,
    TConstructionContext& context) const {
    Y_VERIFY(portionInfo.TierName != evictFeatures.TargetTierName);

    auto* tiering = Tiering.FindPtr(evictFeatures.PathId);
    Y_VERIFY(tiering);
    auto compression = tiering->GetCompression(evictFeatures.TargetTierName);
    if (!compression) {
        // Noting to recompress. We have no other kinds of evictions yet.
        portionInfo.TierName = evictFeatures.TargetTierName;
        evictFeatures.DataChanges = false;
        return true;
    }

    Y_VERIFY(!evictFeatures.NeedExport);

    TPortionInfo undo = portionInfo;

    auto blobSchema = context.SchemaVersions.GetSchema(undo.GetMinSnapshot());
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, srcBlobs);

    size_t undoSize = newBlobs.size();
    TSaverContext saverContext;
    saverContext.SetTierName(evictFeatures.TargetTierName).SetExternalCompression(compression);
    for (auto& rec : portionInfo.Records) {
        auto pos = resultSchema->GetFieldIndex(rec.ColumnId);
        Y_VERIFY(pos >= 0);
        auto field = resultSchema->GetFieldByIndex(pos);
        auto columnSaver = resultSchema->GetColumnSaver(rec.ColumnId, saverContext);

        auto blob = columnSaver.Apply(batch->GetColumnByName(field->name()), field);
        if (blob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
            portionInfo = undo;
            newBlobs.resize(undoSize);
            return false;
        }
        newBlobs.emplace_back(std::move(blob));
        rec.BlobRange = TBlobRange{};
    }

    for (auto& rec : undo.Records) {
        evictedRecords.emplace_back(std::move(rec));
    }

    portionInfo.AddMetadata(*resultSchema, batch, evictFeatures.TargetTierName);
    return true;
}

NKikimr::TConclusion<std::vector<TString>> TTTLColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_VERIFY(!Blobs.empty());           // src data
    Y_VERIFY(!PortionsToEvict.empty()); // src meta
    Y_VERIFY(EvictedRecords.empty());   // dst meta

    auto baseResult = TBase::DoConstructBlobs(context);
    Y_VERIFY(baseResult.IsSuccess() && baseResult.GetResult().empty());

    std::vector<TString> newBlobs;
    std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>> evicted;
    evicted.reserve(PortionsToEvict.size());

    for (auto& [portionInfo, evictFeatures] : PortionsToEvict) {
        if (UpdateEvictedPortion(portionInfo, evictFeatures, Blobs,
            EvictedRecords, newBlobs, context)) {
            Y_VERIFY(portionInfo.TierName == evictFeatures.TargetTierName);
            evicted.emplace_back(std::move(portionInfo), evictFeatures);
        }
    }

    PortionsToEvict.swap(evicted);
    return newBlobs;
}

NColumnShard::ECumulativeCounters TTTLColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_TTL_SUCCESS : NColumnShard::COUNTER_TTL_FAIL;
}

}
