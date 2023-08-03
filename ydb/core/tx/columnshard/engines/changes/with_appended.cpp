#include "with_appended.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/splitter/splitter.h>

namespace NKikimr::NOlap {

void TChangesWithAppend::DoDebugString(TStringOutput& out) const {
    if (ui32 added = AppendedPortions.size()) {
        out << "add " << added << " portions";
        for (auto& portionInfo : AppendedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
}

void TChangesWithAppend::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& /*context*/) {
    for (auto& portionInfo : AppendedPortions) {
        switch (portionInfo.Meta.Produced) {
            case NOlap::TPortionMeta::UNSPECIFIED:
                Y_VERIFY(false); // unexpected
            case NOlap::TPortionMeta::INSERTED:
                self.IncCounter(NColumnShard::COUNTER_INDEXING_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::COMPACTED:
                self.IncCounter(NColumnShard::COUNTER_COMPACTION_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::SPLIT_COMPACTED:
                self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_PORTIONS_WRITTEN);
                break;
            case NOlap::TPortionMeta::EVICTED:
                Y_FAIL("Unexpected evicted case");
                break;
            case NOlap::TPortionMeta::INACTIVE:
                Y_FAIL("Unexpected inactive case");
                break;
        }

    }
}

bool TChangesWithAppend::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
    // Save new granules
    for (auto& [granule, p] : NewGranules) {
        ui64 pathId = p.first;
        TMark mark = p.second;
        TGranuleRecord rec(pathId, granule, context.Snapshot, mark.GetBorder());
        if (!self.SetGranule(rec, !dryRun)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot insert granule")("record", rec.DebugString());
            return false;
        }
        if (!dryRun) {
            self.GranulesTable->Write(context.DB, rec);
        }
    }
    // Save new portions (their column records)

    for (auto& portionInfo : AppendedPortions) {
        Y_VERIFY(!portionInfo.Empty());

        const ui64 granule = portionInfo.GetGranule();
        if (dryRun) {
            auto granulePtr = self.GetGranuleOptional(granule);
            if (!granulePtr && !NewGranules.contains(granule)) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot write portion with unknown granule")("portion", portionInfo.DebugString())("granule", granule);
                return false;
            }

            // granule vs portion minPK
            NArrow::TReplaceKey granuleStart = granulePtr ? granulePtr->Record.Mark : NewGranules.find(granule)->second.second.GetBorder();

            const auto& portionStart = portionInfo.IndexKeyStart();
            if (portionStart < granuleStart) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot insert invalid portion")
                    ("portion", portionInfo.DebugString())("granule", granule)
                    ("start", TMark(portionStart).ToString())("granule_start", TMark(granuleStart).ToString());
                return false;
            }
        }

        if (!self.UpsertPortion(portionInfo, !dryRun)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot insert portion")("portion", portionInfo.DebugString())("granule", granule);
            return false;
        }

        if (!dryRun) {
            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Write(context.DB, portionInfo, record);
            }
        }
    }

    return true;
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    for (auto&& i : AppendedPortions) {
        i.SetPortion(context.NextPortionId());
        i.UpdateRecordsMeta(TPortionMeta::INSERTED);
    }
}

std::vector<TPortionInfo> TChangesWithAppend::MakeAppendedPortions(
    const ui64 pathId, const std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot,
    std::vector<TString>& blobs, const TGranuleMeta* granuleMeta, TConstructionContext& context) const {
    Y_VERIFY(batch->num_rows());

    auto resultSchema = context.SchemaVersions.GetSchema(snapshot);
    std::vector<TPortionInfo> out;

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

    TSplitLimiter limiter(granuleMeta, context.Counters, resultSchema, batch);

    std::vector<TString> portionBlobs;
    std::shared_ptr<arrow::RecordBatch> portionBatch;
    while (limiter.Next(portionBlobs, portionBatch, saverContext)) {
        TPortionInfo portionInfo(granule, 0, snapshot);
        portionInfo.Records.reserve(resultSchema->GetSchema()->num_fields());
        for (auto&& f : resultSchema->GetSchema()->fields()) {
            const ui32 columnId = resultSchema->GetIndexInfo().GetColumnId(f->name());
            portionInfo.AppendOneChunkColumn(TColumnRecord::Make(columnId));
        }
        for (auto&& i : portionBlobs) {
            blobs.emplace_back(i);
        }
        portionInfo.AddMetadata(*resultSchema, portionBatch, tierName);
        out.emplace_back(std::move(portionInfo));
    }

    return out;
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& self) {
    if (self.Tiers) {
        TieringInfo = self.Tiers->GetTiering();
    }
}

}
