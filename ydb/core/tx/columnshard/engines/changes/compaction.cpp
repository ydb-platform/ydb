#include "compaction.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap {

void TCompactColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "original_granule=" << GranuleMeta->GetPathId() << ";";
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
        portionInfo.GetPortionConstructor().MutableMeta().UpdateRecordsMeta(producedClassResultCompaction);
    }
}

void TCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);

    Y_ABORT_UNLESS(SwitchedPortions.size());
    THashMap<TString, THashSet<TBlobRange>> blobRanges;
    auto& index = self.GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    for (const auto& p : SwitchedPortions) {
        Y_ABORT_UNLESS(!p.Empty());
        p.FillBlobRangesByStorage(blobRanges, index);
    }

    for (const auto& p : blobRanges) {
        auto action = BlobsAction.GetReading(p.first);
        for (auto&& b: p.second) {
            action->AddRange(b);
        }
    }

    self.BackgroundController.StartCompaction(NKikimr::NOlap::TPlanCompactionInfo(GranuleMeta->GetPathId()));
    NeedGranuleStatusProvide = true;
    GranuleMeta->OnCompactionStarted();
}

void TCompactColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        self->Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_COMPACTION_TIME, context.Duration.MilliSeconds());
    }
}

void TCompactColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    self.BackgroundController.FinishCompaction(TPlanCompactionInfo(GranuleMeta->GetPathId()));
    Y_ABORT_UNLESS(NeedGranuleStatusProvide);
    if (context.FinishedSuccessfully) {
        GranuleMeta->OnCompactionFinished();
    } else {
        GranuleMeta->OnCompactionFailed(context.ErrorMessage);
    }
    NeedGranuleStatusProvide = false;
}

TCompactColumnEngineChanges::TCompactColumnEngineChanges(std::shared_ptr<TGranuleMeta> granule, const std::vector<std::shared_ptr<TPortionInfo>>& portions, const TSaverContext& saverContext)
    : TBase(saverContext, NBlobOperations::EConsumer::GENERAL_COMPACTION)
    , GranuleMeta(granule) {
    Y_ABORT_UNLESS(GranuleMeta);

    SwitchedPortions.reserve(portions.size());
    for (const auto& portionInfo : portions) {
        Y_ABORT_UNLESS(!portionInfo->HasRemoveSnapshot());
        SwitchedPortions.emplace_back(*portionInfo);
        AddPortionToRemove(*portionInfo);
        Y_ABORT_UNLESS(portionInfo->GetPathId() == GranuleMeta->GetPathId());
    }
    Y_ABORT_UNLESS(SwitchedPortions.size());
}

TCompactColumnEngineChanges::~TCompactColumnEngineChanges() {
    Y_DEBUG_ABORT_UNLESS(!NActors::TlsActivationContext || !NeedGranuleStatusProvide);
}

}
