#include "execution_unit.h"
#include "execution_unit_ctors.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

THolder<TExecutionUnit> CreateExecutionUnit(EExecutionUnitKind kind,
                                            TDataShard &dataShard,
                                            TPipeline &pipeline)
{
    switch (kind) {
    case EExecutionUnitKind::CheckDataTx:
        return CreateCheckDataTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckWrite:
        return CreateCheckWriteUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckSchemeTx:
        return CreateCheckSchemeTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckSnapshotTx:
        return CreateCheckSnapshotTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckDistributedEraseTx:
        return CreateCheckDistributedEraseTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckCommitWritesTx:
        return CreateCheckCommitWritesTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreDataTx:
        return CreateStoreDataTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreWrite:
        return CreateStoreWriteUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreSchemeTx:
        return CreateStoreSchemeTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreSnapshotTx:
        return CreateStoreSnapshotTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreDistributedEraseTx:
        return CreateStoreDistributedEraseTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreCommitWritesTx:
        return CreateStoreCommitWritesTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildAndWaitDependencies:
        return CreateBuildAndWaitDependenciesUnit(dataShard, pipeline);
    case EExecutionUnitKind::FinishPropose:
        return CreateFinishProposeUnit(dataShard, pipeline);
    case EExecutionUnitKind::FinishProposeWrite:
        return CreateFinishProposeWriteUnit(dataShard, pipeline);
    case EExecutionUnitKind::CompletedOperations:
        return CreateCompletedOperationsUnit(dataShard, pipeline);
    case EExecutionUnitKind::WaitForPlan:
        return CreateWaitForPlanUnit(dataShard, pipeline);
    case EExecutionUnitKind::PlanQueue:
        return CreatePlanQueueUnit(dataShard, pipeline);
    case EExecutionUnitKind::LoadTxDetails:
        return CreateLoadTxDetailsUnit(dataShard, pipeline);
    case EExecutionUnitKind::LoadWriteDetails:
        return CreateLoadWriteDetailsUnit(dataShard, pipeline);
    case EExecutionUnitKind::FinalizeDataTxPlan:
        return CreateFinalizeDataTxPlanUnit(dataShard, pipeline);
    case EExecutionUnitKind::ProtectSchemeEchoes:
        return CreateProtectSchemeEchoesUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildDataTxOutRS:
        return CreateBuildDataTxOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildDistributedEraseTxOutRS:
        return CreateBuildDistributedEraseTxOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildKqpDataTxOutRS:
        return CreateBuildKqpDataTxOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildWriteOutRS:
        return CreateBuildWriteOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreAndSendOutRS:
        return CreateStoreAndSendOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::StoreAndSendWriteOutRS:
        return CreateStoreAndSendWriteOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::PrepareDataTxInRS:
        return CreatePrepareDataTxInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::PrepareKqpDataTxInRS:
        return CreatePrepareKqpDataTxInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::PrepareWriteTxInRS:
        return CreatePrepareWriteTxInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::PrepareDistributedEraseTxInRS:
        return CreatePrepareDistributedEraseTxInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::LoadAndWaitInRS:
        return CreateLoadAndWaitInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteDataTx:
        return CreateExecuteDataTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteKqpDataTx:
        return CreateExecuteKqpDataTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteDistributedEraseTx:
        return CreateExecuteDistributedEraseTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteCommitWritesTx:
        return CreateExecuteCommitWritesTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::CompleteOperation:
        return CreateCompleteOperationUnit(dataShard, pipeline);
    case EExecutionUnitKind::CompleteWrite:
        return CreateCompleteWriteUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteKqpScanTx:
        return CreateExecuteKqpScanTxUnit(dataShard, pipeline);
    case EExecutionUnitKind::MakeScanSnapshot:
        return CreateMakeScanSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::WaitForStreamClearance:
        return CreateWaitForStreamClearanceUnit(dataShard, pipeline);
    case EExecutionUnitKind::ReadTableScan:
        return CreateReadTableScanUnit(dataShard, pipeline);
    case EExecutionUnitKind::MakeSnapshot:
        return CreateMakeSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::BuildSchemeTxOutRS:
        return CreateBuildSchemeTxOutRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::PrepareSchemeTxInRS:
        return CreatePrepareSchemeTxInRSUnit(dataShard, pipeline);
    case EExecutionUnitKind::Backup:
        return CreateBackupUnit(dataShard, pipeline);
    case EExecutionUnitKind::Restore:
        return CreateRestoreUnit(dataShard, pipeline);
    case EExecutionUnitKind::CreateTable:
        return CreateCreateTableUnit(dataShard, pipeline);
    case EExecutionUnitKind::ReceiveSnapshot:
        return CreateReceiveSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::ReceiveSnapshotCleanup:
        return CreateReceiveSnapshotCleanupUnit(dataShard, pipeline);
    case EExecutionUnitKind::AlterMoveShadow:
        return CreateAlterMoveShadowUnit(dataShard, pipeline);
    case EExecutionUnitKind::AlterTable:
        return CreateAlterTableUnit(dataShard, pipeline);
    case EExecutionUnitKind::DropTable:
        return CreateDropTableUnit(dataShard, pipeline);
    case EExecutionUnitKind::DirectOp:
        return CreateDirectOpUnit(dataShard, pipeline);
    case EExecutionUnitKind::CreatePersistentSnapshot:
        return CreateCreatePersistentSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::DropPersistentSnapshot:
        return CreateDropPersistentSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::CreateVolatileSnapshot:
        return CreateCreateVolatileSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::DropVolatileSnapshot:
        return CreateDropVolatileSnapshotUnit(dataShard, pipeline);
    case EExecutionUnitKind::InitiateBuildIndex:
        return CreateInitiateBuildIndexUnit(dataShard, pipeline);
    case EExecutionUnitKind::FinalizeBuildIndex:
        return CreateFinalizeBuildIndexUnit(dataShard, pipeline);
    case EExecutionUnitKind::DropIndexNotice:
        return CreateDropIndexNoticeUnit(dataShard, pipeline);
    case EExecutionUnitKind::MoveTable:
        return CreateMoveTableUnit(dataShard, pipeline);
    case EExecutionUnitKind::CreateCdcStream:
        return CreateCreateCdcStreamUnit(dataShard, pipeline);
    case EExecutionUnitKind::AlterCdcStream:
        return CreateAlterCdcStreamUnit(dataShard, pipeline);
    case EExecutionUnitKind::DropCdcStream:
        return CreateDropCdcStreamUnit(dataShard, pipeline);
    case EExecutionUnitKind::RotateCdcStream:
        return CreateRotateCdcStreamUnit(dataShard, pipeline);
    case EExecutionUnitKind::MoveIndex:
        return CreateMoveIndexUnit(dataShard, pipeline);
    case EExecutionUnitKind::CheckRead:
        return CreateCheckReadUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteRead:
        return CreateReadUnit(dataShard, pipeline);
    case EExecutionUnitKind::ExecuteWrite:
        return CreateExecuteWriteUnit(dataShard, pipeline);
    case EExecutionUnitKind::CreateIncrementalRestoreSrc:
        return CreateIncrementalRestoreSrcUnit(dataShard, pipeline);
    default:
        Y_ENSURE(false, "Unexpected execution kind " << kind << " (" << (ui32)kind << ")");
    }

    return nullptr;
}

bool TExecutionUnit::CheckRejectDataTx(TOperation::TPtr op, const TActorContext& ctx) {
    TWriteOperation* writeOp = TWriteOperation::TryCastWriteOperation(op);

    // Reject operations after receiving EvSplit
    // This is to avoid races when split is in progress
    if (DataShard.GetState() == TShardState::SplitSrcWaitForNoTxInFlight ||
        DataShard.GetState() == TShardState::SplitSrcMakeSnapshot ||
        DataShard.GetState() == TShardState::SplitSrcSendingSnapshot ||
        DataShard.GetState() == TShardState::SplitSrcWaitForPartitioningChanged)
    {
        TString err = TStringBuilder()
            << "Wrong shard state: " << (TShardState)DataShard.GetState()
            << " tablet id: " << DataShard.TabletID();

        if (writeOp) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, err);
        } else {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED)
                ->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
            "Tablet " << DataShard.TabletID() << " rejecting tx due to split");

        op->Abort();
        return true;
    }

    // Check if shard state is not appropriate for tx execution
    if (DataShard.GetState() != TShardState::Ready &&
        DataShard.GetState() != TShardState::Readonly &&
        !(DataShard.GetState() == TShardState::Frozen && op->IsReadOnly()))
    {
        TString err = TStringBuilder()
            << "Wrong shard state: " << (TShardState)DataShard.GetState()
            << " tablet id: " << DataShard.TabletID();
        // TODO: Return SCHEME_CHANGED if the shard has been split
        if (writeOp) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, err);
        } else {        
            BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

        op->Abort();
        return true;
    }

    if (DataShard.IsStopping()) {
        TString err = TStringBuilder()
            << "Tablet " << DataShard.TabletID() << " is restarting";

        if (writeOp) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, err);
        } else {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED)
                ->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

        op->Abort();
        return true;
    }

    if (!op->IsReadOnly() && DataShard.CheckChangesQueueOverflow()) {
        TString err = TStringBuilder()
                << "Can't execute at blocked shard: " << " tablet id: " << DataShard.TabletID();

        if (writeOp) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, err);

            DataShard.SetOverloadSubscribed(writeOp->GetWriteTx()->GetOverloadSubscribe(), writeOp->GetRecipient(), op->GetTarget(), ERejectReasons::ChangesQueueOverflow, writeOp->GetWriteResult()->Record);
        } else {                
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED)
                    ->AddError(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED, err);
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                     "Tablet " << DataShard.TabletID() << " rejecting tx due to changes queue overflow");

        op->Abort();
        return true;
    }

    if (DataShard.IsReplicated() && !(op->IsReadOnly() || op->IsCommitWritesTx())) {
        TString err = TStringBuilder()
            << "Can't execute write tx at replicated table:"
            << " tablet id: " << DataShard.TabletID();

        if (writeOp) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, err);
        } else {            
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR)
                ->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

        op->Abort();
        return true;
    }

    return false;
}

bool TExecutionUnit::WillRejectDataTx(TOperation::TPtr op) const {
    if (DataShard.GetState() == TShardState::SplitSrcWaitForNoTxInFlight ||
        DataShard.GetState() == TShardState::SplitSrcMakeSnapshot ||
        DataShard.GetState() == TShardState::SplitSrcSendingSnapshot ||
        DataShard.GetState() == TShardState::SplitSrcWaitForPartitioningChanged)
    {
        return true;
    }

    if (DataShard.GetState() != TShardState::Ready &&
        DataShard.GetState() != TShardState::Readonly &&
        !(DataShard.GetState() == TShardState::Frozen && op->IsReadOnly()))
    {
        return true;
    }

    if (DataShard.IsStopping()) {
        return true;
    }

    if (!op->IsReadOnly() && DataShard.CheckChangesQueueOverflow()) {
        return true;
    }

    if (DataShard.IsReplicated() && !(op->IsReadOnly() || op->IsCommitWritesTx())) {
        return true;
    }

    return false;
}

TOutputOpData::TResultPtr &TExecutionUnit::BuildResult(TOperation::TPtr op,
                                                       NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status)
{
    auto kind = static_cast<NKikimrTxDataShard::ETransactionKind>(op->GetKind());
    op->Result().Reset(new TEvDataShard::TEvProposeTransactionResult(kind,
                                                                 DataShard.TabletID(),
                                                                 op->GetTxId(),
                                                                 status));
    if (DataShard.GetProcessingParams())
        op->Result()->SetDomainCoordinators(*DataShard.GetProcessingParams());

    return op->Result();
}

} // namespace NDataShard
} // namespace NKikimr
