#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <ydb/core/tablet/tablet_exception.h>

namespace NKikimr {
namespace NDataShard {

class TCheckSchemeTxUnit : public TExecutionUnit {
public:
    TCheckSchemeTxUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TCheckSchemeTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    bool CheckSchemeTx(TActiveTransaction *activeTx);
    bool CheckCreate(TActiveTransaction *activeTx);
    bool CheckDrop(TActiveTransaction *activeTx);
    bool CheckAlter(TActiveTransaction *activeTx);
    bool CheckBackup(TActiveTransaction *activeTx);
    bool CheckRestore(TActiveTransaction *activeTx);
    bool CheckCopy(TActiveTransaction *activeTx);
    bool CheckCreatePersistentSnapshot(TActiveTransaction *activeTx);
    bool CheckDropPersistentSnapshot(TActiveTransaction *activeTx);
    bool CheckInitiateBuildIndex(TActiveTransaction *activeTx);
    bool CheckFinalizeBuildIndex(TActiveTransaction *activeTx);
    bool CheckDropIndexNotice(TActiveTransaction *activeTx);
    bool CheckMoveTable(TActiveTransaction *activeTx);
    bool CheckMoveIndex(TActiveTransaction *activeTx);
    bool CheckCreateCdcStream(TActiveTransaction *activeTx);
    bool CheckAlterCdcStream(TActiveTransaction *activeTx);
    bool CheckDropCdcStream(TActiveTransaction *activeTx);

    bool CheckSchemaVersion(TActiveTransaction *activeTx, ui64 proposedSchemaVersion, ui64 currentSchemaVersion, ui64 expectedSchemaVersion);

    using TPipelineHasSmthFunc = std::function<bool(TPipeline* const)>;
    bool HasDuplicate(TActiveTransaction *activeTx, const TStringBuf kind, TPipelineHasSmthFunc checker);
    bool HasConflicts(TActiveTransaction *activeTx, const TStringBuf kind, const THashMap<TString, TPipelineHasSmthFunc>& checkers);

    template <typename T> bool HasPathId(TActiveTransaction *activeTx, const T &op, const TStringBuf kind);
    template <typename T> TPathId GetPathId(const T &op) const;
    template <typename T> bool CheckSchemaVersion(TActiveTransaction *activeTx, ui64 tableId, const T &op);
    template <typename T> bool CheckSchemaVersion(TActiveTransaction *activeTx, const T &op);
};

TCheckSchemeTxUnit::TCheckSchemeTxUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CheckSchemeTx, false, dataShard, pipeline)
{
}

TCheckSchemeTxUnit::~TCheckSchemeTxUnit()
{
}

bool TCheckSchemeTxUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCheckSchemeTxUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    Y_ABORT_UNLESS(op->IsSchemeTx());
    Y_ABORT_UNLESS(!op->IsAborted());

    Pipeline.RemoveWaitingSchemeOp(op);

    TActiveTransaction *activeTx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(activeTx, "cannot cast operation of kind " << op->GetKind());
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();
    bool unfreezeTx = false;
    if (tx.HasAlterTable() && tx.GetAlterTable().HasPartitionConfig() &&
            tx.GetAlterTable().GetPartitionConfig().HasFreezeState())
    {
        auto cmd = tx.GetAlterTable().GetPartitionConfig().GetFreezeState();
        unfreezeTx = cmd == NKikimrSchemeOp::EFreezeState::Unfreeze;
    }

    // Check state is proper for scheme transactions.
    if (!DataShard.IsStateActive() || (DataShard.IsStateFrozen() && !unfreezeTx)) {
        TString error = TStringBuilder()
            << "Wrong shard state for scheme transaction at tablet "
            << DataShard.TabletID() << " state: " << DataShard.GetState() << " txId: "
            << op->GetTxId() << " ssId: " << activeTx->GetSchemeShardId();

        BuildResult(op)->SetProcessError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE,
                                         error);
        op->Abort(EExecutionUnitKind::FinishPropose);

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, error);

        return EExecutionStatus::Executed;
    }

    TSchemeOpSeqNo seqNo(tx.GetSeqNo());
    TSchemeOpSeqNo lastSeqNo = DataShard.GetLastSchemeOpSeqNo();

    // Check if scheme tx is outdated.
    if (lastSeqNo > seqNo) {
        TString error = TStringBuilder()
                << "Ignoring outdated schema Tx proposal at tablet "
                << DataShard.TabletID() << " txId " << op->GetTxId()
                << " ssId " << activeTx->GetSchemeShardId()
                << " seqNo " << seqNo.Generation << ":" << seqNo.Round
                << " lastSeqNo " << lastSeqNo.Generation << ":"
                << lastSeqNo.Round;

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, error);

        BuildResult(op)->SetProcessError(NKikimrTxDataShard::TError::SCHEME_CHANGED, error);
        op->Abort(EExecutionUnitKind::FinishPropose);

        return EExecutionStatus::Executed;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Propose scheme transaction at tablet " << DataShard.TabletID()
                << " txId " << op->GetTxId() << " ssId " << activeTx->GetSchemeShardId()
                << " seqNo "  << seqNo.Generation << ":" << seqNo.Round);

    // Preserve new seqno to correctly filter out tx duplicates.
    DataShard.UpdateLastSchemeOpSeqNo(seqNo, txc);

    if (seqNo > lastSeqNo) {
        // Activate older scheme ops, they are expected to fail seqno check
        Pipeline.ActivateWaitingSchemeOps(ctx);
    }

    // Check if existing transaction matches the proposal
    auto existingOp = Pipeline.FindOp(activeTx->GetTxId());
    if (existingOp && existingOp->IsSchemeTx()) {
        // Check if we have propose for the same transaction type
        auto *schemaOp = Pipeline.FindSchemaTx(activeTx->GetTxId());
        Y_VERIFY_S(schemaOp, "Unexpected failure to find schema tx " << activeTx->GetTxId());

        // N.B. cannot use existingOp as it may not be loaded yet
        if (activeTx->GetSchemeTxType() != schemaOp->Type) {
            BuildResult(op)->SetProcessError(NKikimrTxDataShard::TError::BAD_ARGUMENT,
                TStringBuilder()
                    << "Existing transaction "
                    << activeTx->GetTxId()
                    << " has a mismatching schema transaction type "
                    << ui32(schemaOp->Type)
                    << " expected "
                    << ui32(activeTx->GetSchemeTxType()));
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::ExecutedNoMoreRestarts;
        }

        // Store the most recent tx body
        if (Y_LIKELY(!existingOp->GetStep())) {
            // 1. If transaction has not yet been planned, then tx details are
            //    currently cleared and would be loaded as soon as transaction
            //    is planned and ready to execute. We need to store the updated
            //    SeqNo, since schemeshard may restart multiple times before
            //    planning the transaction. We want to protect against the most
            //    recent echo that is possible.
            // 2. If transaction has already been planned, then we expect all
            //    duplicates with matching SeqNo to be perfect duplicates, as
            //    retries from earlier schemeshard generations are already
            //    rejected by SeqNo above. We don't want to update tx body
            //    in that case, assuming that race is not harmful.
            Pipeline.UpdateSchemeTxBody(activeTx->GetTxId(), activeTx->GetTxBody(), txc);
        }

        // Correctly fill matching MinStep and MaxStep (KIKIMR-9616)
        op->SetMinStep(schemaOp->MinStep);
        op->SetMaxStep(schemaOp->MaxStep);
        BuildResult(op)->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

        op->Abort(EExecutionUnitKind::FinishPropose);
        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    // Check scheme tx content.
    if (!CheckSchemeTx(activeTx)) {
        Y_ABORT_UNLESS(op->Result());
        op->Abort(EExecutionUnitKind::FinishPropose);

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    if (!op->IsReadOnly() && DataShard.TxPlanWaiting() > 0) {
        // Eagerly stop any waiting volatile transactions
        // Note: this may immediate reactivate this operation
        std::vector<std::unique_ptr<IEventHandle>> replies;
        Pipeline.CleanupWaitingVolatile(ctx, replies);
        // Technically we would like to wait until everything is committed, but
        // since volatile transactions are only stored in memory, sending
        // replies with confirmed read-only lease should be enough.
        if (!replies.empty()) {
            DataShard.SendConfirmedReplies(DataShard.ConfirmReadOnlyLease(), std::move(replies));
        }
        // Cleanup call above may have changed the number of waiting transactions
        if (DataShard.TxPlanWaiting() > 0) {
            // We must wait until there are no transactions waiting for plan
            Pipeline.AddWaitingSchemeOp(op);
            return EExecutionStatus::Continue;
        }
    }

    op->SetMinStep(Pipeline.AllowedSchemaStep());
    op->SetMaxStep(Max<ui64>());

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Prepared scheme transaction txId " << op->GetTxId() << " at tablet "
                << DataShard.TabletID());

    BuildResult(op)->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

    return EExecutionStatus::ExecutedNoMoreRestarts;
}

bool TCheckSchemeTxUnit::CheckSchemaVersion(TActiveTransaction *activeTx,
    ui64 proposedSchemaVersion, ui64 currentSchemaVersion, ui64 expectedSchemaVersion)
{
    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                "Check scheme tx, proposed scheme version# " << proposedSchemaVersion <<
                " current version# " << currentSchemaVersion <<
                " expected version# " << expectedSchemaVersion <<
                " at tablet# " << DataShard.TabletID() << " txId# " << activeTx->GetTxId());

    // Allow scheme tx if proposed or current schema version is zero. This simplify migration a lot.
    if (proposedSchemaVersion && currentSchemaVersion && expectedSchemaVersion != proposedSchemaVersion) {
        TString err = TStringBuilder()
            << "Wrong schema version: proposed# " << proposedSchemaVersion <<
               " current version# " << currentSchemaVersion <<
               " expected version# " << expectedSchemaVersion <<
               " at tablet# " << DataShard.TabletID() << " txId# " << activeTx->GetTxId();

        LOG_CRIT_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, err);
        return false;
    }

    return true;
}

bool TCheckSchemeTxUnit::HasDuplicate(TActiveTransaction *activeTx, const TStringBuf kind, TPipelineHasSmthFunc checker) {
    if (!checker(&Pipeline)) {
        return false;
    }

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
        "Ignoring " << kind << " duplicate"
            << " at tablet " << DataShard.TabletID()
            << " txId " << activeTx->GetTxId()
            << " currentTxId " << Pipeline.CurrentSchemaTxId());
    BuildResult(activeTx)->SetSchemeTxDuplicate(Pipeline.CurrentSchemaTxId() == activeTx->GetTxId());

    return true;
}

bool TCheckSchemeTxUnit::HasConflicts(TActiveTransaction *activeTx, const TStringBuf kind, const THashMap<TString, TPipelineHasSmthFunc>& checkers) {
    for (const auto& kv : checkers) {
        const auto& conflicting = kv.first;
        const auto& checker = kv.second;

        if (!checker(&Pipeline)) {
            continue;
        }

        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
            "Ignoring " << kind << " during ongoing " << conflicting
                << " at tablet " << DataShard.TabletID()
                << " txId " << activeTx->GetTxId()
                << " currentTxId " << Pipeline.CurrentSchemaTxId());
        BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);

        return true;
    }

    return false;
}

template <typename T>
bool TCheckSchemeTxUnit::HasPathId(TActiveTransaction *activeTx, const T &op, const TStringBuf kind) {
    if (op.HasPathId()) {
        return true;
    }

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
        kind << " description has no PathId"
            << " at tablet " << DataShard.TabletID()
            << " txId " << activeTx->GetTxId()
            << " currentTxId " << Pipeline.CurrentSchemaTxId());
    BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);

    return false;
}

template <typename T>
TPathId TCheckSchemeTxUnit::GetPathId(const T &op) const {
    auto pathId = PathIdFromPathId(op.GetPathId());
    Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == pathId.OwnerId);
    return pathId;
}

template <typename T>
bool TCheckSchemeTxUnit::CheckSchemaVersion(TActiveTransaction *activeTx, ui64 tableId, const T &op) {
    const auto tablePtr = DataShard.GetUserTables().FindPtr(tableId);

    Y_ABORT_UNLESS(tablePtr);
    const auto &table = **tablePtr;

    const auto current = table.GetTableSchemaVersion();
    const auto proposed = op.HasTableSchemaVersion() ? op.GetTableSchemaVersion() : 0;

    const auto res = CheckSchemaVersion(activeTx, proposed, current, current + 1);
    Y_DEBUG_ABORT_UNLESS(res, "Unexpected schema version mutation");

    return true;
}

template <typename T>
bool TCheckSchemeTxUnit::CheckSchemaVersion(TActiveTransaction *activeTx, const T &op) {
    return CheckSchemaVersion(activeTx, GetPathId(op).LocalPathId, op);
}

bool TCheckSchemeTxUnit::CheckSchemeTx(TActiveTransaction *activeTx)
{
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();

    bool res = false;
    switch (activeTx->GetSchemeTxType()) {
    case TSchemaOperation::ETypeCreate:
        res = CheckCreate(activeTx);
        break;
    case TSchemaOperation::ETypeDrop:
        res = CheckDrop(activeTx);
        break;
    case TSchemaOperation::ETypeAlter:
        res = CheckAlter(activeTx);
        break;
    case TSchemaOperation::ETypeBackup:
        res = CheckBackup(activeTx);
        break;
    case TSchemaOperation::ETypeRestore:
        res = CheckRestore(activeTx);
        break;
    case TSchemaOperation::ETypeCopy:
        res = CheckCopy(activeTx);
        break;
    case TSchemaOperation::ETypeCreatePersistentSnapshot:
        res = CheckCreatePersistentSnapshot(activeTx);
        break;
    case TSchemaOperation::ETypeDropPersistentSnapshot:
        res = CheckDropPersistentSnapshot(activeTx);
        break;
    case TSchemaOperation::ETypeInitiateBuildIndex:
        res = CheckInitiateBuildIndex(activeTx);
        break;
    case TSchemaOperation::ETypeFinalizeBuildIndex:
        res = CheckFinalizeBuildIndex(activeTx);
        break;
    case TSchemaOperation::ETypeDropIndexNotice:
        res = CheckDropIndexNotice(activeTx);
        break;
    case TSchemaOperation::ETypeMoveTable:
        res = CheckMoveTable(activeTx);
        break;
    case TSchemaOperation::ETypeMoveIndex:
        res = CheckMoveIndex(activeTx);
        break;
    case TSchemaOperation::ETypeCreateCdcStream:
        res = CheckCreateCdcStream(activeTx);
        break;
    case TSchemaOperation::ETypeAlterCdcStream:
        res = CheckAlterCdcStream(activeTx);
        break;
    case TSchemaOperation::ETypeDropCdcStream:
        res = CheckDropCdcStream(activeTx);
        break;
    default:
        LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Unknown scheme tx type detected at tablet "
                    << DataShard.TabletID() << " txId " << activeTx->GetTxId()
                    << " txBody " << tx.ShortDebugString());
        BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
    }

    return res;
}

bool TCheckSchemeTxUnit::CheckCreate(TActiveTransaction *activeTx) {
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();

    if (HasDuplicate(activeTx, "Create", &TPipeline::HasCreate)) {
        return false;
    }

    const auto &create = tx.GetCreateTable();
    ui64 tableId = create.GetId_Deprecated();
    if (create.HasPathId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == create.GetPathId().GetOwnerId() || DataShard.GetPathOwnerId() == INVALID_TABLET_ID);
        tableId = create.GetPathId().GetLocalId();
    }

    Y_ABORT_UNLESS(!DataShard.GetUserTables().contains(tableId));

    return true;
}

bool TCheckSchemeTxUnit::CheckDrop(TActiveTransaction *activeTx) {
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();

    if (HasDuplicate(activeTx, "Drop", &TPipeline::HasDrop)) {
        return false;
    }

    const auto &drop = tx.GetDropTable();
    ui64 tableId = drop.GetId_Deprecated();
    if (drop.HasPathId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == drop.GetPathId().GetOwnerId());
        tableId = drop.GetPathId().GetLocalId();
    }
    Y_ABORT_UNLESS(DataShard.GetUserTables().FindPtr(tableId));

    return true;
}

bool TCheckSchemeTxUnit::CheckAlter(TActiveTransaction *activeTx)
{
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();
    Y_ABORT_UNLESS(!Pipeline.HasDrop());

    const TStringBuf kind = "Alter";
    if (HasDuplicate(activeTx, kind, &TPipeline::HasAlter)) {
        return false;
    }

    const THashMap<TString, TPipelineHasSmthFunc> conflicting = {
        {"Backup", &TPipeline::HasBackup},
        {"Restore", &TPipeline::HasRestore},
    };
    if (HasConflicts(activeTx, kind, conflicting)) {
        return false;
    }

    const auto &alter = tx.GetAlterTable();
    const ui64 proposedSchemaVersion = alter.HasTableSchemaVersion() ? alter.GetTableSchemaVersion() : 0;

    if (alter.HasPartitionConfig() && alter.GetPartitionConfig().HasFreezeState()) {
        if (alter.ColumnsSize() || alter.DropColumnsSize()) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Ignoring alter, combine freeze with other actions is forbiden, tablet " << DataShard.TabletID()
                        << " txId " << activeTx->GetTxId() <<  " currentTxId "
                        << Pipeline.CurrentSchemaTxId());
            BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST);
            return false;
        }

        if (DataShard.IsFollower()) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Ignoring alter, attempt to freeze follower, tablet " << DataShard.TabletID()
                        << " txId " << activeTx->GetTxId() <<  " currentTxId "
                        << Pipeline.CurrentSchemaTxId());
            BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST);
            return false;
        }

        bool freeze = alter.GetPartitionConfig().GetFreezeState() == NKikimrSchemeOp::EFreezeState::Freeze;
        const auto curState = DataShard.GetState();
        bool err = false;

        if (freeze) {
            if (curState != NDataShard::TShardState::Ready) {
                err = true;
            }
        } else {
            if (curState != NDataShard::TShardState::Frozen) {
                err = true;
            }
        }
        if (err) {
            const auto& cmdStr = freeze ? TString("freeze") : TString("unfreeze");
            TString errText = TStringBuilder() << "Ignoring alter, transaction wants to "
                    << cmdStr << " datashard but current state is "
                    << DatashardStateName(curState) << " tablet "<< DataShard.TabletID()
                    << " txId " << activeTx->GetTxId() <<  " currentTxId "
                    << Pipeline.CurrentSchemaTxId();
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, errText);
            BuildResult(activeTx)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, errText);
            return false;
        }
    }

    ui64 tableId = alter.GetId_Deprecated();
    if (alter.HasPathId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == alter.GetPathId().GetOwnerId());
        tableId = alter.GetPathId().GetLocalId();
    }

    const auto tablePtr = DataShard.GetUserTables().FindPtr(tableId);
    Y_VERIFY_S(tablePtr, "tableId: " << tableId);
    const TUserTable &table = **tablePtr;

    auto curSchemaVersion = table.GetTableSchemaVersion();

    for (const auto &col : alter.GetColumns()) {
        Y_ABORT_UNLESS(col.HasId());
        Y_ABORT_UNLESS(col.HasTypeId());
        Y_ABORT_UNLESS(col.HasName());

        ui32 colId = col.GetId();
        if (table.Columns.contains(colId)) {
            const TUserTable::TUserColumn &column = table.Columns.at(colId);
            Y_ABORT_UNLESS(column.Name == col.GetName());
            // TODO: support pg types
            Y_ABORT_UNLESS(column.Type.GetTypeId() == col.GetTypeId());
            Y_ABORT_UNLESS(col.HasFamily());
        }
    }

    for (const auto &col : alter.GetDropColumns()) {
        ui32 colId = col.GetId();
        const TUserTable::TUserColumn *userColumn = table.Columns.FindPtr(colId);
        Y_ABORT_UNLESS(userColumn);
        Y_ABORT_UNLESS(userColumn->Name == col.GetName());
    }

    auto res = CheckSchemaVersion(activeTx, proposedSchemaVersion, curSchemaVersion, curSchemaVersion + 1);
    Y_DEBUG_ABORT_UNLESS(res, "Unexpected schema version mutation");
    return true;
}

bool TCheckSchemeTxUnit::CheckBackup(TActiveTransaction *activeTx)
{
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();
    Y_ABORT_UNLESS(!Pipeline.HasDrop());

    const TStringBuf kind = "Backup";
    if (HasDuplicate(activeTx, kind, &TPipeline::HasBackup)) {
        return false;
    }

    const THashMap<TString, TPipelineHasSmthFunc> conflicting = {
        {"Alter", &TPipeline::HasAlter},
        {"Restore", &TPipeline::HasRestore},
    };
    if (HasConflicts(activeTx, kind, conflicting)) {
        return false;
    }

    const auto &backup = tx.GetBackup();
    Y_ABORT_UNLESS(DataShard.GetUserTables().contains(backup.GetTableId()));

    return true;
}

bool TCheckSchemeTxUnit::CheckRestore(TActiveTransaction *activeTx)
{
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();
    Y_ABORT_UNLESS(!Pipeline.HasDrop());

    const TStringBuf kind = "Restore";
    if (HasDuplicate(activeTx, kind, &TPipeline::HasRestore)) {
        return false;
    }

    const THashMap<TString, TPipelineHasSmthFunc> conflicting = {
        {"Alter", &TPipeline::HasAlter},
        {"Backup", &TPipeline::HasBackup},
    };
    if (HasConflicts(activeTx, kind, conflicting)) {
        return false;
    }

    const auto &restore = tx.GetRestore();
    Y_ABORT_UNLESS(DataShard.GetUserTables().contains(restore.GetTableId()));

    return true;
}

bool TCheckSchemeTxUnit::CheckCopy(TActiveTransaction *activeTx) {
    const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();

    if (HasDuplicate(activeTx, "Copy", &TPipeline::HasCopy)) {
        return false;
    }

    const auto &snap = tx.GetSendSnapshot();
    ui64 tableId = snap.GetTableId_Deprecated();
    if (snap.HasTableId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == snap.GetTableId().GetOwnerId());
        tableId = snap.GetTableId().GetTableId();
    }
    Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));

    return true;
}

bool TCheckSchemeTxUnit::CheckCreatePersistentSnapshot(TActiveTransaction *activeTx) {
    const auto& tx = activeTx->GetSchemeTx();

    if (HasDuplicate(activeTx, "CreatePersistentSnapshot", &TPipeline::HasCreatePersistentSnapshot)) {
        return false;
    }

    Y_UNUSED(tx);

    return true;
}

bool TCheckSchemeTxUnit::CheckDropPersistentSnapshot(TActiveTransaction *activeTx) {
    const auto& tx = activeTx->GetSchemeTx();

    if (HasDuplicate(activeTx, "DropPersistentSnapshot", &TPipeline::HasDropPersistentSnapshot)) {
        return false;
    }

    Y_UNUSED(tx);

    return true;
}

bool TCheckSchemeTxUnit::CheckInitiateBuildIndex(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "InitiateBuildIndex", &TPipeline::HasInitiateBuilIndex)) {
        return false;
    }

    const auto &initiate = activeTx->GetSchemeTx().GetInitiateBuildIndex();
    if (!HasPathId(activeTx, initiate, "InitiateBuildIndex")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, initiate);
}

bool TCheckSchemeTxUnit::CheckFinalizeBuildIndex(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "FinalizeBuildIndex", &TPipeline::HasFinalizeBuilIndex)) {
        return false;
    }

    const auto &finalize = activeTx->GetSchemeTx().GetFinalizeBuildIndex();
    if (!HasPathId(activeTx, finalize, "FinalizeBuildIndex")) {
        return false;
    }

    const auto pathId = GetPathId(finalize);
    const auto snapshotKey = TSnapshotKey(pathId, finalize.GetSnapshotStep(), finalize.GetSnapshotTxId());

    if (DataShard.GetSnapshotManager().FindAvailable(snapshotKey) == nullptr) {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "FinalizeBuildIndex description has unexisting snapshot key."
                        << " Tablet " << DataShard.TabletID()
                        << " txId " << activeTx->GetTxId()
                        << " pathId " << pathId
                        << " snapshotKey " << snapshotKey);
        BuildResult(activeTx, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
        return false;
    }

    return CheckSchemaVersion(activeTx, pathId.LocalPathId, finalize);
}

bool TCheckSchemeTxUnit::CheckDropIndexNotice(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "DropIndexNotice", &TPipeline::HasDropIndexNotice)) {
        return false;
    }

    const auto &notice = activeTx->GetSchemeTx().GetDropIndexNotice();
    if (!HasPathId(activeTx, notice, "DropIndexNotice")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, notice);
}


bool TCheckSchemeTxUnit::CheckMoveTable(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "Move", &TPipeline::HasMove)) {
        return false;
    }

    const auto &mv = activeTx->GetSchemeTx().GetMoveTable();
    if (!HasPathId(activeTx, mv, "Move")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, mv);
}

bool TCheckSchemeTxUnit::CheckMoveIndex(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "MoveIndex", &TPipeline::HasMoveIndex)) {
        return false;
    }

    const auto &mv = activeTx->GetSchemeTx().GetMoveIndex();
    if (!HasPathId(activeTx, mv, "MoveIndex")) {
        return false;
    }

    auto ret = CheckSchemaVersion(activeTx, mv);
    return ret;
}

bool TCheckSchemeTxUnit::CheckCreateCdcStream(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "CreateCdcStream", &TPipeline::HasCreateCdcStream)) {
        return false;
    }

    const auto &notice = activeTx->GetSchemeTx().GetCreateCdcStreamNotice();
    if (!HasPathId(activeTx, notice, "CreateCdcStream")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, notice);
}

bool TCheckSchemeTxUnit::CheckAlterCdcStream(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "AlterCdcStream", &TPipeline::HasAlterCdcStream)) {
        return false;
    }

    const auto &notice = activeTx->GetSchemeTx().GetAlterCdcStreamNotice();
    if (!HasPathId(activeTx, notice, "AlterCdcStream")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, notice);
}

bool TCheckSchemeTxUnit::CheckDropCdcStream(TActiveTransaction *activeTx) {
    if (HasDuplicate(activeTx, "DropCdcStream", &TPipeline::HasDropCdcStream)) {
        return false;
    }

    const auto &notice = activeTx->GetSchemeTx().GetDropCdcStreamNotice();
    if (!HasPathId(activeTx, notice, "DropCdcStream")) {
        return false;
    }

    return CheckSchemaVersion(activeTx, notice);
}

void TCheckSchemeTxUnit::Complete(TOperation::TPtr,
                                  const TActorContext &)
{
}

THolder<TExecutionUnit> CreateCheckSchemeTxUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return THolder(new TCheckSchemeTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
