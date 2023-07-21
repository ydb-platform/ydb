#include "with_appended.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

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

        const ui64 granule = portionInfo.Granule();
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
                self.ColumnsTable->Write(context.DB, record);
            }
        }
    }

    return true;
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    for (auto&& i : AppendedPortions) {
        i.UpdatePortionId(context.NextPortionId());
        i.UpdateRecordsMeta(TPortionMeta::INSERTED);
    }
}

}
