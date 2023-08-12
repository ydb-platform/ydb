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
        switch (portionInfo.GetPortionInfo().Meta.Produced) {
            case NOlap::TPortionMeta::EProduced::UNSPECIFIED:
                Y_VERIFY(false); // unexpected
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
                Y_FAIL("Unexpected evicted case");
                break;
            case NOlap::TPortionMeta::EProduced::INACTIVE:
                Y_FAIL("Unexpected inactive case");
                break;
        }

    }
}

bool TChangesWithAppend::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    // Save new granules
    for (auto& [granule, p] : NewGranules) {
        ui64 pathId = p.first;
        TMark mark = p.second;
        TGranuleRecord rec(pathId, granule, context.Snapshot, mark.GetBorder());
        self.SetGranule(rec);
        self.GranulesTable->Write(context.DB, rec);
    }
    // Save new portions (their column records)

    for (auto& portionInfoWithBlobs : AppendedPortions) {
        auto& portionInfo = portionInfoWithBlobs.GetPortionInfo();
        Y_VERIFY(!portionInfo.Empty());

        self.UpsertPortion(portionInfo);
        for (auto& record : portionInfo.Records) {
            self.ColumnsTable->Write(context.DB, portionInfo, record);
        }
    }

    return true;
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    for (auto&& i : AppendedPortions) {
        i.GetPortionInfo().SetPortion(context.NextPortionId());
        i.GetPortionInfo().UpdateRecordsMeta(TPortionMeta::EProduced::INSERTED);
    }
}

std::vector<TPortionInfoWithBlobs> TChangesWithAppend::MakeAppendedPortions(
    const ui64 pathId, const std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot,
    const TGranuleMeta* granuleMeta, TConstructionContext& context) const {
    Y_VERIFY(batch->num_rows());

    auto resultSchema = context.SchemaVersions.GetSchema(snapshot);
    std::vector<TPortionInfoWithBlobs> out;

    TString tierName;
    std::optional<NArrow::TCompression> compression;
    if (pathId) {
        if (auto* tiering = TieringInfo.FindPtr(pathId)) {
            tierName = tiering->GetHottestTierName();
            if (const auto& tierCompression = tiering->GetCompression(tierName)) {
                compression = *tierCompression;
            }
        }
    }
    TSaverContext saverContext;
    saverContext.SetTierName(tierName).SetExternalCompression(compression);

    NOlap::TSerializationStats stats;
    if (granuleMeta) {
        stats = granuleMeta->BuildSerializationStats(resultSchema);
    }
    auto schema = std::make_shared<TDefaultSchemaDetails>(resultSchema, saverContext, std::move(stats));
    TRBSplitLimiter limiter(context.Counters.SplitterCounters, schema, batch);

    std::vector<std::vector<TOrderedColumnChunk>> portionBlobs;
    std::shared_ptr<arrow::RecordBatch> portionBatch;
    while (limiter.Next(portionBlobs, portionBatch)) {
        TPortionInfo portionInfo(granule, 0, snapshot);
        portionInfo.AddMetadata(*resultSchema, portionBatch, tierName);

        TPortionInfoWithBlobs infoWithBlob(std::move(portionInfo), portionBlobs.size());
        std::map<ui32, ui32> chunkIds;
        THashMap<TBlobRange, TString> srcBlobs;
        for (auto& blob : portionBlobs) {
            auto& blobInfo = infoWithBlob.StartBlob(blob.size());
            for (auto&& chunk : blob) {
                const TString data = chunk.GetData();
                srcBlobs.emplace(blobInfo.AddChunk(infoWithBlob, std::move(chunk)).BlobRange, data);
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portion_appended", infoWithBlob.GetPortionInfo().DebugString());
        out.emplace_back(std::move(infoWithBlob));
    }

    return out;
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& self) {
    if (self.Tiers) {
        TieringInfo = self.Tiers->GetTiering();
    }
}

}
