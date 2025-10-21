#include "compaction.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap {

void TCompactColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "original_granule=" << GranuleMeta->GetPathId() << ";";
    if (ui32 switched = SwitchedPortions.size()) {
        out << "switch " << switched << " portions:(";
        for (auto& portionInfo : SwitchedPortions) {
            out << portionInfo->DebugString(false);
        }
        out << "); ";
    }
}

void TCompactColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    TBase::DoCompile(context);

    for (auto& portionInfo : AppendedPortions) {
        auto& constructor = portionInfo.GetPortionConstructor().MutablePortionConstructor();
        constructor.MutableMeta().SetCompactionLevel(
            GranuleMeta->GetOptimizerPlanner().GetAppropriateLevel(GetPortionsToMove().GetTargetCompactionLevel().value_or(0),
                portionInfo.GetPortionConstructor().GetCompactionInfo()));
    }
}

void TCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);

    self.BackgroundController.StartCompaction(GranuleMeta->GetPathId(), GetTaskIdentifier());
    NeedGranuleStatusProvide = true;
    GranuleMeta->OnCompactionStarted();
}

void TCompactColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_COMPACTION_TIME, context.Duration.MilliSeconds());
    }
}

void TCompactColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    self.BackgroundController.FinishCompaction(GranuleMeta->GetPathId());
    Y_ABORT_UNLESS(NeedGranuleStatusProvide);
    if (context.FinishedSuccessfully) {
        GranuleMeta->OnCompactionFinished();
    } else {
        GranuleMeta->OnCompactionFailed(context.ErrorMessage);
    }
    NeedGranuleStatusProvide = false;
}

TCompactColumnEngineChanges::TCompactColumnEngineChanges(
    std::shared_ptr<TGranuleMeta> granule, const std::vector<TPortionInfo::TConstPtr>& portions, const TSaverContext& saverContext)
    : TBase(saverContext, NBlobOperations::EConsumer::GENERAL_COMPACTION)
    , GranuleMeta(granule) {
    Y_ABORT_UNLESS(GranuleMeta);

    for (const auto& portionInfo : portions) {
        Y_ABORT_UNLESS(!portionInfo->HasRemoveSnapshot());
        SwitchedPortions.emplace_back(portionInfo);
        AddPortionToRemove(portionInfo);
        Y_ABORT_UNLESS(portionInfo->GetPathId() == GranuleMeta->GetPathId());
    }
    //    Y_ABORT_UNLESS(SwitchedPortions.size());
}

TCompactColumnEngineChanges::~TCompactColumnEngineChanges() {
    Y_DEBUG_ABORT_UNLESS(!NActors::TlsActivationContext || !NeedGranuleStatusProvide || !IsActive());
}

}   // namespace NKikimr::NOlap
