#include "datashard_impl.h"
#include "datashard_integrity_trails.h"
#include "datashard_pipeline.h"

#include "ydb/core/tx/datashard/datashard_write_operation.h"
#include <ydb/core/tablet/tablet_exception.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

class TCheckWriteUnit: public TExecutionUnit {
public:
    TCheckWriteUnit(TDataShard &dataShard, TPipeline &pipeline);
    ~TCheckWriteUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TCheckWriteUnit::TCheckWriteUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CheckWrite, false, dataShard, pipeline)
{
}

TCheckWriteUnit::~TCheckWriteUnit()
{
}

bool TCheckWriteUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCheckWriteUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx)
{
    Y_ENSURE(!op->IsAborted());

    if (CheckRejectDataTx(op, ctx)) {
        op->Abort(EExecutionUnitKind::FinishProposeWrite);

        return EExecutionStatus::Executed;
    }

    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);
    auto writeTx = writeOp->GetWriteTx();
    Y_ENSURE(writeTx);
    Y_ENSURE(writeTx->Ready() || writeTx->RequirePrepare());

    if (!op->IsImmediate() && writeTx->HasOperations()) {
        TString snapshotError;
        auto snapshotStatus = NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST;
        if (op->IsMvccSnapshotRead()) {
            // Config may force an otherwise valid uncommitted locked write
            // online after key validation. That execution mode is unsupported.
            snapshotError = TStringBuilder()
                << "Operation " << *op << " cannot write at snapshot " << op->GetMvccSnapshot()
                << " using a planned transaction at " << DataShard.TabletID();
        } else if (const auto& snapshot = writeOp->GetReadSnapshot()) {
            const auto& manager = DataShard.GetSnapshotManager();
            const auto& locks = writeTx->GetKqpLocks();
            const bool snapshotIsolation = writeTx->GetLockMode() == TDataShardUserDb::ELockMode::OptimisticSnapshotIsolation;
            if (snapshotIsolation && !manager.IsSnapshotWriteSafe(*snapshot)) {
                snapshotError = TStringBuilder()
                    << "Operation " << *op << " writes at stale snapshot " << *snapshot
                    << " with incomplete write history at " << DataShard.TabletID();
            } else if (snapshotIsolation && locks && locks->GetOp() == NKikimrDataEvents::TKqpLocks::Commit
                && !writeOp->CanAbortPreparedWrite())
            {
                // A final row mutation may discover a conflict when executing.
                // It must be able to abort the other participants.
                snapshotError = TStringBuilder()
                    << "Operation " << *op << " cannot vote on the snapshot write at " << DataShard.TabletID();
            } else if (*snapshot < manager.GetLowWatermark()) {
                // Check history before PREPARED. The queued operation pins it
                // until completion even if the registry registration expires.
                for (const auto& operation : writeTx->GetOperations()) {
                    const auto& tableId = operation.GetTableId();
                    if (!manager.IsSnapshotReadable(txc.DB, tableId, *snapshot)) {
                        snapshotError = TStringBuilder()
                            << "Operation " << *op << " writes at stale snapshot " << *snapshot
                            << " for table " << tableId << " at " << DataShard.TabletID();
                        break;
                    }
                }
            }
            snapshotStatus = NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED;
        }
        if (snapshotError) {
            writeOp->SetError(snapshotStatus, snapshotError);
            op->Abort(EExecutionUnitKind::FinishProposeWrite);
            YDB_LOG_ERROR_CTX(ctx, "TCheckWriteUnit::Execute: cannot write at snapshot",
                {"errorMessage", snapshotError});
            return EExecutionStatus::Executed;
        }
    }

    // Check if we are out of space and tx wants to update user
    // or system table.
    // Unsafe truncate is exempt: it only frees space, and it is needed most exactly when the shard
    // has run out of it.
    if (DataShard.IsAnyChannelYellowStop()
        && (writeTx->HasWrites() || !op->IsImmediate())
        && !writeTx->HasUnsafeTruncate()) {
        TString err = TStringBuilder()
            << "Cannot perform transaction: out of disk space at tablet "
            << DataShard.TabletID() << " txId " << op->GetTxId();

        DataShard.IncCounter(COUNTER_WRITE_DISK_GROUP_OUT_OF_SPACE);

        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE, err);
        op->Abort(EExecutionUnitKind::FinishProposeWrite);

        DataShard.SetOverloadSubscribed(writeOp->GetWriteTx()->GetOverloadSubscribe(), writeOp->GetRecipient(), op->GetTarget(), ERejectReasons::YellowChannels, writeOp->GetWriteResult()->Record);

        if (DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::CheckWriteUnit_Execute).Kick()) {
            YDB_LOG_ERROR_CTX(ctx, err);
        }
        return EExecutionStatus::Executed;
    }

    {
        for (const auto& key : writeTx->TxInfo().Keys) {
            if (key.IsWrite && DataShard.IsUserTable(key.Key->TableId)) {
                if (DataShard.IsSubDomainOutOfSpace()) {
                    switch (key.Key->RowOperation) {
                        case TKeyDesc::ERowOperation::Read:
                        case TKeyDesc::ERowOperation::Erase:
                            // Read and erase are allowed even when we're out of disk space
                            break;

                        default: {
                            // Updates are not allowed when database is out of space
                            TString err = "Cannot perform writes: database is out of disk space";

                            DataShard.IncCounter(COUNTER_WRITE_DATABASE_DISK_SPACE_QUOTA_EXCEEDED);

                            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_DATABASE_DISK_SPACE_QUOTA_EXCEEDED, err);
                            op->Abort(EExecutionUnitKind::FinishProposeWrite);

                            DataShard.SetOverloadSubscribed(writeOp->GetWriteTx()->GetOverloadSubscribe(), writeOp->GetRecipient(), op->GetTarget(), ERejectReasons::YellowChannels, writeOp->GetWriteResult()->Record);

                            if (DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::CheckWriteUnit_Execute).Kick()) {
                               YDB_LOG_ERROR_CTX(ctx, err);
                            }

                            return EExecutionStatus::Executed;
                        }
                    }
                }
            }
        }
    }

    if (!op->IsReadOnly() && op->HasKeysInfo()) {
        const NMiniKQL::IEngineFlat::TValidationInfo& keys = op->GetKeysInfo();
        NDataIntegrity::LogIntegrityTrailsKeys(ctx, DataShard.TabletID(), op->GetGlobalTxId(), keys);
    }

    if (!op->IsImmediate()) {
        if (!Pipeline.AssignPlanInterval(op)) {
            TString err = TStringBuilder() << "Can't propose tx " << op->GetTxId() << " at blocked shard " << DataShard.TabletID();

            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, err);
            op->Abort(EExecutionUnitKind::FinishProposeWrite);

            YDB_LOG_NOTICE_CTX(ctx, "TCheckWriteUnit::Execute: cannot propose tx at blocked shard",
                {"errorMessage", err});

            return EExecutionStatus::Executed;
        }

        writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildPrepared(
            DataShard.TabletID(),
            op->GetTxId(),
            {
                op->GetMinStep(),
                op->GetMaxStep(),
                DataShard.GetProcessingParams() ? DataShard.GetProcessingParams()->GetCoordinators() : google::protobuf::RepeatedField<ui64>{}
            }
            ));
        YDB_LOG_DEBUG_CTX(ctx, "TCheckWriteUnit::Execute: prepared write transaction",
            {"operation", *op},
            {"tabletId", DataShard.TabletID()});
    }

    return EExecutionStatus::Executed;
}

void TCheckWriteUnit::Complete(TOperation::TPtr, const TActorContext &)
{
}

THolder<TExecutionUnit> CreateCheckWriteUnit(TDataShard &dataShard, TPipeline &pipeline)
{
    return THolder(new TCheckWriteUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr


#undef YDB_LOG_THIS_FILE_COMPONENT

