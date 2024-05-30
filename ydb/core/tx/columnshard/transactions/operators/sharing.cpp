#include "sharing.h"
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/formats/arrow/serializer/native.h>

namespace NKikimr::NColumnShard {

bool TSharingTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrColumnShardDataSharingProto::TDestinationSession txBody;
    SharingSessionsManager = owner.GetSharingSessionsManager();

    if (!txBody.ParseFromString(data)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("reason", "cannot parse string as proto");
        return false;
    }
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvProposeFromInitiator");
    SharingTask = std::make_shared<NOlap::NDataSharing::TDestinationSession>();
    auto conclusion = SharingTask->DeserializeDataFromProto(txBody, owner.GetIndexAs<NOlap::TColumnEngineForLogs>());
    if (!conclusion) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_start_data_sharing_from_initiator")("error", conclusion.GetErrorMessage());
        return false;
    }

    auto currentSession = SharingSessionsManager->GetDestinationSession(SharingTask->GetSessionId());
    if (currentSession) {
        SessionExistsFlag = true;
        SharingTask = currentSession;
    } else {
        SharingTask->Confirm();
    }

    TxPropose = SharingSessionsManager->ProposeDestSession(&owner, SharingTask);

    return true;
}

TSharingTransactionOperator::TProposeResult TSharingTransactionOperator::DoStartProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!SessionExistsFlag) {
        AFL_VERIFY(!!TxPropose);
        AFL_VERIFY(TxPropose->Execute(txc, NActors::TActivationContext::AsActorContext()));
    }
    return TProposeResult();
}

void TSharingTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!SessionExistsFlag) {
        AFL_VERIFY(!!TxPropose);
        TxPropose->Complete(ctx);
    }
    TxPropose.release();
}

bool TSharingTransactionOperator::ExecuteOnProgress(TColumnShard& /*owner*/, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
    return true;
}

bool TSharingTransactionOperator::CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) {
    for (TActorId subscriber : NotifySubscribers) {
        auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
        ctx.Send(subscriber, event.Release(), 0, 0);
    }
    return true;
}

bool TSharingTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!SharingTask) {
        return true;
    }
    if (!TxAbort) {
        TxAbort = SharingTask->AckInitiatorFinished(&owner, SharingTask).DetachResult();
    }
    TxAbort->Execute(txc, NActors::TActivationContext::AsActorContext());
    return true;
}

bool TSharingTransactionOperator::CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!SharingTask) {
        return true;
    }
    AFL_VERIFY(!!TxAbort);
    TxAbort->Complete(ctx);
    return true;
}

}
