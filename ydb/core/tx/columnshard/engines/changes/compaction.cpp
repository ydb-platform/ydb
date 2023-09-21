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

void TCompactColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    TBase::DoCompile(context);

    const TPortionMeta::EProduced producedClassResultCompaction = GetResultProducedClass();
    for (auto& portionInfo : AppendedPortions) {
        portionInfo.GetPortionInfo().UpdateRecordsMeta(producedClassResultCompaction);
    }
}

bool TCompactColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    return TBase::DoApplyChanges(self, context);
}

ui32 TCompactColumnEngineChanges::NumSplitInto(const ui32 srcRows) const {
    Y_VERIFY(srcRows > 1);
    const ui64 totalBytes = TotalBlobsSize();
    const ui32 numSplitInto = (totalBytes / Limits.GranuleSizeForOverloadPrevent) + 1;
    return std::max<ui32>(2, numSplitInto);
}

void TCompactColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    TBase::DoWriteIndex(self, context);
}

void TCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);

    Y_VERIFY(SwitchedPortions.size());
    for (const auto& p : SwitchedPortions) {
        Y_VERIFY(!p.Empty());
        auto action = BlobsAction.GetReading(p);
        for (const auto& rec : p.Records) {
            action->AddRange(rec.BlobRange);
        }
    }

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

TCompactColumnEngineChanges::TCompactColumnEngineChanges(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const std::map<ui64, std::shared_ptr<TPortionInfo>>& portions, const TSaverContext& saverContext)
    : TBase(limits.GetSplitSettings(), saverContext)
    , Limits(limits)
    , GranuleMeta(granule)
{
    Y_VERIFY(GranuleMeta);

    SwitchedPortions.reserve(portions.size());
    for (const auto& [_, portionInfo] : portions) {
        Y_VERIFY(portionInfo->IsActive());
        SwitchedPortions.emplace_back(*portionInfo);
        PortionsToRemove.emplace_back(*portionInfo);
        Y_VERIFY(portionInfo->GetGranule() == GranuleMeta->GetGranuleId());
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
