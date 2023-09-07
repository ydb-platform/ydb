#include "compaction.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap {

void TCompactColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "original_granule=" << GranuleMeta->GetGranuleId() << ";";
    if (ui32 switched = SwitchedPortions.size()) {
        out << "switch " << switched << " portions:(";
        for (auto& portionInfo : SwitchedPortions) {
            out << portionInfo;
        }
        out << "); ";
    }
}

THashMap<NKikimr::NOlap::TUnifiedBlobId, std::vector<NKikimr::NOlap::TBlobRange>> TCompactColumnEngineChanges::GetGroupedBlobRanges() const {
    return GroupedBlobRanges(SwitchedPortions);
}

void TCompactColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    TBase::DoCompile(context);

    const TPortionMeta::EProduced producedClassResultCompaction = GetResultProducedClass();
    for (auto& portionInfo : AppendedPortions) {
        portionInfo.GetPortionInfo().UpdateRecordsMeta(producedClassResultCompaction);
    }
    for (auto& portionInfo : SwitchedPortions) {
        Y_VERIFY(portionInfo.IsActive());
        portionInfo.SetRemoveSnapshot(context.GetSnapshot());
    }
}

bool TCompactColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    Y_VERIFY(TBase::DoApplyChanges(self, context));
    auto g = self.GranulesStorage->StartPackModification();
    for (auto& portionInfo : SwitchedPortions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(!portionInfo.IsActive());

        const ui64 granule = portionInfo.GetGranule();
        const ui64 portion = portionInfo.GetPortion();

        const TPortionInfo& oldInfo = self.GetGranulePtrVerified(granule)->GetPortionVerified(portion);

        auto& granuleStart = self.Granules[granule]->Record.Mark;

        Y_VERIFY(granuleStart <= portionInfo.IndexKeyStart());
        self.UpsertPortion(portionInfo, &oldInfo);

        for (auto& record : portionInfo.Records) {
            self.ColumnsTable->Write(context.DB, portionInfo, record);
        }
    }

    for (auto& portionInfo : SwitchedPortions) {
        self.CleanupPortions.insert(portionInfo.GetAddress());
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

void TCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    self.BackgroundController.StartCompaction(NKikimr::NOlap::TPlanCompactionInfo(GranuleMeta->GetPathId()), *this);
    NeedGranuleStatusProvide = true;
    GranuleMeta->OnCompactionStarted();
}

void TCompactColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    self.IncCounter(NColumnShard::COUNTER_COMPACTION_TIME, context.Duration.MilliSeconds());
}

void TCompactColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    self.BackgroundController.FinishCompaction(TPlanCompactionInfo(GranuleMeta->GetPathId()));
    Y_VERIFY(NeedGranuleStatusProvide);
    if (context.FinishedSuccessfully) {
        GranuleMeta->OnCompactionFinished();
    } else {
        GranuleMeta->OnCompactionFailed(context.ErrorMessage);
    }
    NeedGranuleStatusProvide = false;
}

TCompactColumnEngineChanges::TCompactColumnEngineChanges(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const TCompactionSrcGranule& srcGranule)
    : TBase(limits.GetSplitSettings())
    , Limits(limits)
    , GranuleMeta(granule)
    , SrcGranule(srcGranule)
{
    Y_VERIFY(GranuleMeta);

    SwitchedPortions.reserve(GranuleMeta->GetPortions().size());
    for (const auto& [_, portionInfo] : GranuleMeta->GetPortions()) {
        if (portionInfo->IsActive()) {
            SwitchedPortions.push_back(*portionInfo);
            Y_VERIFY(portionInfo->GetGranule() == GranuleMeta->GetGranuleId());
        }
    }
    Y_VERIFY(SwitchedPortions.size());
}

TCompactColumnEngineChanges::TCompactColumnEngineChanges(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const std::map<ui64, std::shared_ptr<TPortionInfo>>& portions)
    : TBase(limits.GetSplitSettings())
    , Limits(limits)
    , GranuleMeta(granule)
{
//    Y_VERIFY(GranuleMeta);

    SwitchedPortions.reserve(portions.size());
    for (const auto& [_, portionInfo] : portions) {
        Y_VERIFY(portionInfo->IsActive());
        SwitchedPortions.push_back(*portionInfo);
        Y_VERIFY(!GranuleMeta || portionInfo->GetGranule() == GranuleMeta->GetGranuleId());
    }
    Y_VERIFY(SwitchedPortions.size());
}

TCompactColumnEngineChanges::~TCompactColumnEngineChanges() {
    Y_VERIFY_DEBUG(!NActors::TlsActivationContext || !NeedGranuleStatusProvide);
}

THashSet<TPortionAddress> TCompactColumnEngineChanges::GetTouchedPortions() const {
    THashSet<TPortionAddress> result = TBase::GetTouchedPortions();
    for (auto&& i : SwitchedPortions) {
        result.emplace(i.GetAddress());
    }
    return result;
}

}
