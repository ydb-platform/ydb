#include "with_appended.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/splitter/rb_splitter.h>

namespace NKikimr::NOlap {

void TChangesWithAppend::DoDebugString(TStringOutput& out) const {
    if (ui32 added = AppendedPortions.size()) {
        out << "portions_count:" << added << ";portions=(";
        for (auto& portionInfo : AppendedPortions) {
            out << portionInfo;
        }
        out << "); ";
    }
}

void TChangesWithAppend::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& /*context*/) {
    for (auto& portionInfo : AppendedPortions) {
        switch (portionInfo.GetPortionInfo().GetMeta().Produced) {
            case NOlap::TPortionMeta::EProduced::UNSPECIFIED:
                Y_ABORT_UNLESS(false); // unexpected
            case NOlap::TPortionMeta::EProduced::INSERTED:
                self.IncCounter(NColumnShard::COUNTER_INDEXING_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::EProduced::COMPACTED:
                self.IncCounter(NColumnShard::COUNTER_COMPACTION_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::EProduced::SPLIT_COMPACTED:
                self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::EProduced::EVICTED:
                Y_ABORT("Unexpected evicted case");
                break;
            case NOlap::TPortionMeta::EProduced::INACTIVE:
                Y_ABORT("Unexpected inactive case");
                break;
        }
    }
    self.IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, PortionsToRemove.size());

    THashSet<TUnifiedBlobId> blobsDeactivated;
    for (auto& [_, portionInfo] : PortionsToRemove) {
        for (auto& rec : portionInfo.Records) {
            blobsDeactivated.insert(rec.BlobRange.BlobId);
        }
        self.IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo.RawBytesSum());
    }

    self.IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, blobsDeactivated.size());
    for (auto& blobId : blobsDeactivated) {
        self.IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
    }
}

bool TChangesWithAppend::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    // Save new portions (their column records)
    {
        auto g = self.GranulesStorage->StartPackModification();

        for (auto& [_, portionInfo] : PortionsToRemove) {
            Y_ABORT_UNLESS(!portionInfo.Empty());
            Y_ABORT_UNLESS(portionInfo.HasRemoveSnapshot());

            const TPortionInfo& oldInfo = self.GetGranuleVerified(portionInfo.GetPathId()).GetPortionVerified(portionInfo.GetPortion());

            self.UpsertPortion(portionInfo, &oldInfo);

            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Write(context.DB, portionInfo, record);
            }
        }
        for (auto& portionInfoWithBlobs : AppendedPortions) {
            auto& portionInfo = portionInfoWithBlobs.GetPortionInfo();
            Y_ABORT_UNLESS(!portionInfo.Empty());
            self.UpsertPortion(portionInfo);
            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Write(context.DB, portionInfo, record);
            }
        }
    }

    for (auto& [_, portionInfo] : PortionsToRemove) {
        self.CleanupPortions[portionInfo.GetRemoveSnapshot()].emplace_back(portionInfo);
    }

    return true;
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    for (auto&& i : AppendedPortions) {
        i.GetPortionInfo().SetPortion(context.NextPortionId());
        i.GetPortionInfo().UpdateRecordsMeta(TPortionMeta::EProduced::INSERTED);
    }
    for (auto& [_, portionInfo] : PortionsToRemove) {
        if (!portionInfo.HasRemoveSnapshot()) {
            portionInfo.SetRemoveSnapshot(context.GetSnapshot());
        }
    }
}

std::vector<TPortionInfoWithBlobs> TChangesWithAppend::MakeAppendedPortions(const std::shared_ptr<arrow::RecordBatch> batch,
    const ui64 granule, const TSnapshot& snapshot, const TGranuleMeta* granuleMeta, TConstructionContext& context) const {
    Y_ABORT_UNLESS(batch->num_rows());

    auto resultSchema = context.SchemaVersions.GetSchema(snapshot);
    std::vector<TPortionInfoWithBlobs> out;

    std::shared_ptr<NOlap::TSerializationStats> stats = std::make_shared<NOlap::TSerializationStats>();
    if (granuleMeta) {
        stats = granuleMeta->BuildSerializationStats(resultSchema);
    }
    auto schema = std::make_shared<TDefaultSchemaDetails>(resultSchema, SaverContext, stats);
    TRBSplitLimiter limiter(context.Counters.SplitterCounters, schema, batch, SplitSettings);

    std::vector<std::vector<IPortionColumnChunk::TPtr>> chunkByBlobs;
    std::shared_ptr<arrow::RecordBatch> portionBatch;
    while (limiter.Next(chunkByBlobs, portionBatch)) {
        AFL_VERIFY(NArrow::IsSortedAndUnique(portionBatch, resultSchema->GetIndexInfo().GetReplaceKey()));
        TPortionInfoWithBlobs infoWithBlob = TPortionInfoWithBlobs::BuildByBlobs(chunkByBlobs, nullptr, granule, snapshot, SaverContext.GetStorageOperator());
        infoWithBlob.GetPortionInfo().AddMetadata(*resultSchema, portionBatch, SaverContext.GetTierName());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portion_appended", infoWithBlob.GetPortionInfo().DebugString());
        out.emplace_back(std::move(infoWithBlob));
    }

    return out;
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& /*self*/) {
}

}
