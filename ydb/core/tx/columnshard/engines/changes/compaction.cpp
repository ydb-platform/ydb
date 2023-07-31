#include "compaction.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap {

void TCompactColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    if (ui32 switched = SwitchedPortions.size()) {
        out << "switch " << switched << " portions";
        for (auto& portionInfo : SwitchedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
    if (ui32 moved = PortionsToMove.size()) {
        out << "move " << moved << " portions";
        for (auto& [portionInfo, granule] : PortionsToMove) {
            out << portionInfo << " (to " << granule << ")";
        }
        out << "; ";
    }
}

THashMap<NKikimr::NOlap::TUnifiedBlobId, std::vector<NKikimr::NOlap::TBlobRange>> TCompactColumnEngineChanges::GetGroupedBlobRanges() const {
    return GroupedBlobRanges(SwitchedPortions);
}

void TCompactColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    TBase::DoCompile(context);
    auto granuleRemap = TmpToNewGranules(context, NewGranules);
    for (auto& [_, id] : PortionsToMove) {
        Y_VERIFY(granuleRemap.contains(id));
        id = granuleRemap[id];
    }

    const TPortionMeta::EProduced producedClassResultCompaction = GetResultProducedClass();
    for (auto& portionInfo : AppendedPortions) {
        if (granuleRemap.size()) {
            auto it = granuleRemap.find(portionInfo.Granule());
            Y_VERIFY(it != granuleRemap.end());
            portionInfo.UpdateGranuleId(it->second);
        }

        TPortionMeta::EProduced produced = TPortionMeta::INSERTED;
        // If it's a split compaction with moves appended portions are INSERTED (could have overlaps with others)
        if (PortionsToMove.empty()) {
            produced = producedClassResultCompaction;
        }
        portionInfo.UpdateRecordsMeta(produced);
    }
    for (auto& portionInfo : SwitchedPortions) {
        Y_VERIFY(portionInfo.IsActive());
        portionInfo.SetStale(context.GetSnapshot());
    }
}

bool TCompactColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
    if (!TBase::DoApplyChanges(self, context, dryRun)) {
        return false;
    }
    auto g = self.GranulesStorage->StartPackModification();
    for (auto& portionInfo : SwitchedPortions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(!portionInfo.IsActive());

        ui64 granule = portionInfo.Granule();
        ui64 portion = portionInfo.Portion();
        if (!self.Granules.contains(granule)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot update unknown granule")("granule_id", granule);
            return false;
        }
        if (!self.IsPortionExists(granule, portion)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot update unknown portion")("portion", portionInfo.DebugString());
            return false;
        }

        auto& granuleStart = self.Granules[granule]->Record.Mark;

        if (dryRun) { // granule vs portion minPK
            const auto& portionStart = portionInfo.IndexKeyStart();
            if (portionStart < granuleStart) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot update invalid portion")
                    ("granule_id", granule)("portion", portionInfo.DebugString())("start", TMark(portionStart).ToString())
                    ("granule start", TMark(granuleStart).ToString());
                return false;
            }
        }

        // In case of race with eviction portion could become evicted
        const TPortionInfo& oldInfo = self.Granules[granule]->GetPortionVerified(portion);

        if (!self.UpsertPortion(portionInfo, !dryRun, &oldInfo)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot update portion")("portion", portionInfo.DebugString());
            return false;
        }

        if (!dryRun) {
            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Write(context.DB, record);
            }
        }
    }
    // Move portions in granules (zero-copy switch + append into new granules)

    for (auto& [info, dstGranule] : PortionsToMove) {
        const auto& portionInfo = info;

        ui64 granule = portionInfo.Granule();
        ui64 portion = portionInfo.Portion();
        if (!self.IsPortionExists(granule, portion)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot move unknown portion")("portion", portionInfo.DebugString());
            return false;
        }

        // In case of race with eviction portion could become evicted
        const TPortionInfo oldInfo = self.GetGranuleVerified(granule).GetPortionVerified(portion);

        if (!self.ErasePortion(portionInfo, !dryRun, false)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase moved portion")("portion", portionInfo.DebugString());
            return false;
        }

        TPortionInfo moved = portionInfo;
        moved.SetGranule(dstGranule);
        if (!self.UpsertPortion(moved, !dryRun, &oldInfo)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot insert moved portion")("portion", portionInfo.DebugString());
            return false;
        }
        if (!dryRun) {
            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Erase(context.DB, record);
            }
            for (auto& record : moved.Records) {
                self.ColumnsTable->Write(context.DB, record);
            }
        }
    }

    if (!dryRun) {
        for (auto& portionInfo : SwitchedPortions) {
            self.CleanupPortions.insert(portionInfo.GetAddress());
        }
    }

    return true;
}

ui32 TCompactColumnEngineChanges::NumSplitInto(const ui32 srcRows) const {
    Y_VERIFY(srcRows > 1);
    const ui64 totalBytes = TotalBlobsSize();
    const ui32 numSplitInto = (totalBytes / Limits.GranuleSizeForOverloadPrevent) + 1;
    return std::max<ui32>(2, numSplitInto);
}

void TCompactColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& /*context*/) {
    self.IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, SwitchedPortions.size());

    THashSet<TUnifiedBlobId> blobsDeactivated;
    for (auto& portionInfo : SwitchedPortions) {
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

THashMap<ui64, ui64> TCompactColumnEngineChanges::TmpToNewGranules(TFinalizationContext& context, THashMap<ui64, std::pair<ui64, TMark>>& newGranules) const {
    THashMap<ui64, ui64> granuleRemap;
    for (const auto& [mark, counter] : TmpGranuleIds) {
        if (mark == SrcGranule.Mark) {
            Y_VERIFY(!counter);
            granuleRemap[counter] = GranuleMeta->GetGranuleId();
        } else {
            Y_VERIFY(counter);
            auto it = granuleRemap.find(counter);
            if (it == granuleRemap.end()) {
                it = granuleRemap.emplace(counter, context.NextGranuleId()).first;
            }
            newGranules.emplace(it->second, std::make_pair(GranuleMeta->GetPathId(), mark));
        }
    }
    return granuleRemap;
}

bool TCompactColumnEngineChanges::IsMovedPortion(const TPortionInfo& info) {
    for (auto&& i : PortionsToMove) {
        if (i.first.GetAddress() == info.GetAddress()) {
            return true;
        }
    }
    return false;
}

void TCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    self.BackgroundController.StartCompaction(NKikimr::NOlap::TPlanCompactionInfo(GranuleMeta->GetPathId(), !IsSplit()));
    NeedGranuleStatusProvide = true;
    GranuleMeta->OnCompactionStarted(!IsSplit());
}

void TCompactColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    self.IncCounter(NColumnShard::COUNTER_COMPACTION_TIME, context.Duration.MilliSeconds());
}

void TCompactColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    self.BackgroundController.FinishCompaction(TPlanCompactionInfo(GranuleMeta->GetPathId(), !IsSplit()));
    GranuleMeta->AllowedInsertion();
    Y_VERIFY(NeedGranuleStatusProvide);
    if (context.FinishedSuccessfully) {
        GranuleMeta->OnCompactionFinished();
    } else {
        GranuleMeta->OnCompactionFailed(context.ErrorMessage);
    }
    NeedGranuleStatusProvide = false;
}

ui64 TCompactColumnEngineChanges::SetTmpGranule(ui64 pathId, const TMark& mark) {
    Y_VERIFY(pathId == GranuleMeta->GetPathId());
    if (!TmpGranuleIds.contains(mark)) {
        TmpGranuleIds[mark] = FirstGranuleId;
        ++FirstGranuleId;
    }
    return TmpGranuleIds[mark];
}

TCompactColumnEngineChanges::TCompactColumnEngineChanges(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const TCompactionSrcGranule& srcGranule)
    : Limits(limits)
    , GranuleMeta(granule)
    , SrcGranule(srcGranule)
{
    Y_VERIFY(GranuleMeta);

    SwitchedPortions.reserve(GranuleMeta->GetPortions().size());
    for (const auto& [_, portionInfo] : GranuleMeta->GetPortions()) {
        if (portionInfo.IsActive()) {
            SwitchedPortions.push_back(portionInfo);
            Y_VERIFY(portionInfo.Granule() == GranuleMeta->GetGranuleId());
        }
    }
    Y_VERIFY(SwitchedPortions.size());
}

TCompactColumnEngineChanges::~TCompactColumnEngineChanges() {
    Y_VERIFY_DEBUG(!NActors::TlsActivationContext || !NeedGranuleStatusProvide);
}

}
