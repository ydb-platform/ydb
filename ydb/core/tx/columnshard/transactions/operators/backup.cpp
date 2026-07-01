#include "backup.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>

namespace NKikimr::NColumnShard {

class TTxStartBackupTask: public TExtendedTransactionBase {
private:
    using TBase = TExtendedTransactionBase;
    const ui64 TxId;

    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const NActors::TActorContext& /*ctx*/) override {
        return true;
    }

    virtual void DoComplete(const NActors::TActorContext& ctx) override {
        auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TBackupTransactionOperator>(TxId, true);
        if (!op) {
            return;
        }
        op->StartTask(ctx);
        Self->EnqueueProgressTx(ctx, TxId);
    }

public:
    TTxStartBackupTask(TColumnShard* owner, const ui64 txId)
        : TBase(owner, "start_backup_task")
        , TxId(txId)
    {
    }
};

bool TBackupTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrTxColumnShard::TBackupTxBody txBody;
    if (!txBody.ParseFromString(data)) {
        return false;
    }
    if (!txBody.HasBackupTask()) {
        return false;
    }
    TConclusion<NOlap::NExport::TIdentifier> id = NOlap::NExport::TIdentifier::BuildFromProto(txBody);
    if (!id) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_id")("problem", id.GetErrorMessage());
        return false;
    }
    auto schema = owner.TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaVerified(
        NKikimr::NOlap::TSnapshot{ txBody.GetBackupTask().GetSnapshotStep(), txBody.GetBackupTask().GetSnapshotTxId() });
    const auto& indexInfo = schema->GetIndexInfo();
    const auto& columnsMap = indexInfo.GetColumns();
    std::vector<NOlap::TNameTypeInfo> columns;
    columns.reserve(columnsMap.size());
    for (const ui32 columnId : indexInfo.GetColumnIds(false)) {
        auto it = columnsMap.find(columnId);
        AFL_VERIFY(it != columnsMap.end())("column_id", columnId);
        columns.emplace_back(it->second);
    }
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), columns, txBody.GetBackupTask(), GetTxId());
    NOlap::NBackground::TTask task(::ToString(ExportTask->GetIdentifier().GetSchemeShardLocalPathId().GetRawValue()),
        std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ExportTask);
    if (!owner.GetBackgroundSessionsManager()->HasTask(task)) {
        TxAddTask = owner.GetBackgroundSessionsManager()->TxAddTask(task);
        if (!TxAddTask) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_task");
            return false;
        }
    } else {
        TaskExists = true;
    }
    return true;
}

TBackupTransactionOperator::TProposeResult TBackupTransactionOperator::DoStartProposeOnExecute(
    TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TaskExists && TxAddTask) {
        AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    }
    return TProposeResult();
}

void TBackupTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!TaskExists && TxAddTask) {
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }
}

bool TBackupTransactionOperator::ProgressOnExecute(
    TColumnShard& owner, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(!TxRemove);
    const auto schemeShardLocalPathId = ExportTask->GetIdentifier().GetSchemeShardLocalPathId();
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(ExportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));

    NKikimrTxColumnShard::TCompletedBackupTransaction backupTx;
    backupTx.SetTxId(GetTxId());
    auto& opResult = *backupTx.MutableOpResult();
    opResult.SetSuccess(status.Success);
    opResult.SetExplain(status.ErrorMessage);

    TxRemove = owner.GetBackgroundSessionsManager()->TxRemove(ExportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));
    NIceDb::TNiceDb db(txc.DB);

    const auto tableId = owner.TablesManager.ResolveInternalPathIdVerified(schemeShardLocalPathId, false);
    if (const auto* previousBackupTx = owner.LastCompletedBackupTransactions.FindPtr(schemeShardLocalPathId)) {
        owner.LastCompletedBackupTransactionsByTxId.erase(previousBackupTx->GetTxId());
    }
    const TString serializedBackupTx = backupTx.SerializeAsString();
    owner.LastCompletedBackupTransactions[schemeShardLocalPathId] = backupTx;
    owner.LastCompletedBackupTransactionsByTxId[backupTx.GetTxId()] = backupTx;
    owner.TablesManager.SetLastCompletedBackupTransaction(schemeShardLocalPathId, serializedBackupTx);

    db.Table<Schema::TableInfoV1>()
        .Key(tableId.GetRawValue(), schemeShardLocalPathId.GetRawValue())
        .Update(NIceDb::TUpdate<Schema::TableInfoV1::LastCompletedBackupTransaction>(serializedBackupTx));

    return TxRemove->Execute(txc, NActors::TActivationContext::AsActorContext());
}

bool TBackupTransactionOperator::ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) {
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(
        ExportTask->GetClassName(), ::ToString(ExportTask->GetIdentifier().GetSchemeShardLocalPathId().GetRawValue()));
    for (TActorId subscriber : NotifySubscribers) {
        auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
        auto& opResult = *event->Record.MutableOpResult();
        opResult.SetSuccess(status.Success);
        opResult.SetExplain(status.ErrorMessage);
        ctx.Send(subscriber, event.Release(), 0, 0);
    }
    AFL_VERIFY(!!TxRemove);
    TxRemove->Complete(NActors::TActivationContext::AsActorContext());
    return true;
}

bool TBackupTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TxAbort) {
        auto control = ExportTask->BuildAbortControl();
        TxAbort = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }
    if (!TxAbort) {
        return true;
    }
    return TxAbort->Execute(txc, NActors::TActivationContext::AsActorContext());
}

bool TBackupTransactionOperator::CompleteOnAbort(TColumnShard& owner, const TActorContext& ctx) {
    if (TxAbort) {
        TxAbort->Complete(ctx);
    }
    for (TActorId subscriber : NotifySubscribers) {
        auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
        auto& opResult = *event->Record.MutableOpResult();
        opResult.SetSuccess(false);
        opResult.SetExplain("Cancelled");
        ctx.Send(subscriber, event.Release(), 0, 0);
    }
    return true;
}

void TBackupTransactionOperator::RegisterSubscriber(const TActorId& actorId) {
    NotifySubscribers.insert(actorId);
}

TString TBackupTransactionOperator::DoDebugString() const {
    return "BACKUP";
}

bool TBackupTransactionOperator::DoIsAsync() const {
    return false;
}

TString TBackupTransactionOperator::DoGetOpType() const {
    return "Backup";
}

void TBackupTransactionOperator::DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!TaskExists && TxAddTask) {
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
        TaskStarted = true;
    } else if (TaskExists) {
        TaskStarted = true;
    }
}

void TBackupTransactionOperator::DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
}

bool TBackupTransactionOperator::DoIsInProgress() const {
    return TaskStarted && !TaskCompleted;
}

void TBackupTransactionOperator::DoOnTabletInit(TColumnShard& owner) {
    if (TaskExists) {
        TaskStarted = true;
        const auto key = ::ToString(ExportTask->GetIdentifier().GetSchemeShardLocalPathId().GetRawValue());
        auto status = owner.GetBackgroundSessionsManager()->GetStatus(ExportTask->GetClassName(), key);
        if (status.Success || !status.ErrorMessage.empty()) {
            TaskCompleted = true;
        }
    }
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TBackupTransactionOperator::DoBuildTxPrepareForProgress(TColumnShard* owner) const {
    return std::make_unique<TTxStartBackupTask>(owner, GetTxId());
}

void TBackupTransactionOperator::StartTask(const TActorContext& ctx) {
    if (!TaskStarted && !TaskExists && TxAddTask) {
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }
    TaskStarted = true;
}

void TBackupTransactionOperator::OnBackgroundTaskCompleted() {
    TaskCompleted = true;
}

}   // namespace NKikimr::NColumnShard
