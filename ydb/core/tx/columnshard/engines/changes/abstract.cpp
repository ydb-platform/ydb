#include "abstract.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>

namespace NKikimr::NOlap {

TString TColumnEngineChanges::DebugString() const {
    TStringStream sb;
    sb << TypeString() << ":";
    DoDebugString(sb);
    return sb.Str();
}

NKikimr::TConclusion<std::vector<TString>> TColumnEngineChanges::ConstructBlobs(TConstructionContext& context) {
    Y_VERIFY(Stage == EStage::Started);

    {
        ui64 readBytes = 0;
        for (auto&& i : Blobs) {
            readBytes += i.first.Size;
        }
        context.Counters.CompactionInputSize(readBytes);
    }
    const TMonotonic start = TMonotonic::Now();
    TConclusion<std::vector<TString>> result = DoConstructBlobs(context);
    if (result.IsSuccess()) {
        context.Counters.CompactionDuration->Collect((TMonotonic::Now() - start).MilliSeconds());
    } else {
        context.Counters.CompactionFails->Add(1);
    }
    Stage = EStage::Constructed;
    return result;
}

bool TColumnEngineChanges::ApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
    Y_VERIFY(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Applied) {
        return true;
    }
    Y_VERIFY(Stage == EStage::Compiled);

    if (!DoApplyChanges(self, context, dryRun)) {
        if (dryRun) {
            OnChangesApplyFailed("problems on apply");
        }
        Y_VERIFY(dryRun);
        return false;
    } else if (!dryRun) {
        OnChangesApplyFinished();
        Stage = EStage::Applied;
    }
    return true;
}

void TColumnEngineChanges::WriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    Y_VERIFY(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Written) {
        return;
    }
    Y_VERIFY(Stage == EStage::Applied);

    DoWriteIndex(self, context);
    Stage = EStage::Written;
}

void TColumnEngineChanges::WriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    Y_VERIFY(Stage == EStage::Aborted || Stage == EStage::Written);
    if (Stage == EStage::Aborted) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("stage", Stage);
        return;
    }
    if (Stage == EStage::Written) {
        Stage = EStage::Finished;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("type", TypeString())("success", context.FinishedSuccessfully);
        DoWriteIndexComplete(self, context);
        DoOnFinish(self, context);
    }
}

void TColumnEngineChanges::Compile(TFinalizationContext& context) noexcept {
    Y_VERIFY(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Compiled) {
        return;
    }
    Y_VERIFY(Stage == EStage::Constructed);

    DoCompile(context);

    Stage = EStage::Compiled;
}

TWriteIndexContext::TWriteIndexContext(NTabletFlatExecutor::TTransactionContext& txc, IDbWrapper& dbWrapper)
    : Txc(txc)
    , BlobManagerDb(std::make_shared<NColumnShard::TBlobManagerDb>(txc.DB))
    , DBWrapper(dbWrapper)
{

}

}
