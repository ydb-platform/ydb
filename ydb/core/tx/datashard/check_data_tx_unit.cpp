#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <ydb/core/tablet/tablet_exception.h>

namespace NKikimr {
namespace NDataShard {

class TCheckDataTxUnit : public TExecutionUnit {
public:
    TCheckDataTxUnit(TDataShard &dataShard,
                     TPipeline &pipeline);
    ~TCheckDataTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TCheckDataTxUnit::TCheckDataTxUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CheckDataTx, false, dataShard, pipeline)
{
}

TCheckDataTxUnit::~TCheckDataTxUnit()
{
}

bool TCheckDataTxUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCheckDataTxUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &,
                                           const TActorContext &ctx)
{
    Y_ABORT_UNLESS(op->IsDataTx() || op->IsReadTable());
    Y_ABORT_UNLESS(!op->IsAborted());

    if (CheckRejectDataTx(op, ctx)) {
        op->Abort(EExecutionUnitKind::FinishPropose);

        return EExecutionStatus::Executed;
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
    auto dataTx = tx->GetDataTx();
    Y_ABORT_UNLESS(dataTx);
    Y_ABORT_UNLESS(dataTx->Ready() || dataTx->RequirePrepare());

    if (dataTx->Ready()) {
        DataShard.IncCounter(COUNTER_MINIKQL_PROGRAM_SIZE, dataTx->ProgramSize());
    } else {
        Y_ABORT_UNLESS(dataTx->RequirePrepare());
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Require prepare Tx " << op->GetTxId() <<  " at " << DataShard.TabletID()
                    << ": " << dataTx->GetErrors());
    }

    // Check if we are out of space and tx wants to update user
    // or system table.
    if (DataShard.IsAnyChannelYellowStop()
        && (dataTx->HasWrites() || !op->IsImmediate())) {
        TString err = TStringBuilder()
            << "Cannot perform transaction: out of disk space at tablet "
            << DataShard.TabletID() << " txId " << op->GetTxId();

        DataShard.IncCounter(COUNTER_PREPARE_OUT_OF_SPACE);

        BuildResult(op)->AddError(NKikimrTxDataShard::TError::OUT_OF_SPACE, err);
        op->Abort(EExecutionUnitKind::FinishPropose);

        LOG_LOG_S_THROTTLE(DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::CheckDataTxUnit_Execute), ctx, NActors::NLog::PRI_ERROR, NKikimrServices::TX_DATASHARD, err);

        return EExecutionStatus::Executed;
    }

    if (tx->IsMvccSnapshotRead()) {
        auto snapshot = tx->GetMvccSnapshot();
        if (DataShard.IsFollower()) {
            TString err = TStringBuilder()
                << "Operation " << *op << " cannot read from snapshot " << snapshot
                << " using data tx on a follower " << DataShard.TabletID();

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                ->AddError(NKikimrTxDataShard::TError::BAD_ARGUMENT, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        } else if (!DataShard.IsMvccEnabled()) {
            TString err = TStringBuilder()
                << "Operation " << *op << " reads from snapshot " << snapshot
                << " with MVCC feature disabled at " << DataShard.TabletID();

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                ->AddError(NKikimrTxDataShard::TError::BAD_ARGUMENT, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        } else if (snapshot < DataShard.GetSnapshotManager().GetLowWatermark()) {
            TString err = TStringBuilder()
                << "Operation " << *op << " reads from stale snapshot " << snapshot
                << " at " << DataShard.TabletID();

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                ->AddError(NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        }
    }

    TEngineBay::TSizes txReads;

    if (op->IsDataTx()) {
        bool hasTotalKeysSizeLimit = !!dataTx->PerShardKeysSizeLimitBytes();
        txReads = dataTx->CalcReadSizes(hasTotalKeysSizeLimit);

        if (txReads.ReadSize > DataShard.GetTxReadSizeLimit()) {
            TString err = TStringBuilder()
                << "Transaction read size " << txReads.ReadSize << " exceeds limit "
                << DataShard.GetTxReadSizeLimit() << " at tablet " << DataShard.TabletID()
                << " txId " << op->GetTxId();

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                ->AddError(NKikimrTxDataShard::TError::READ_SIZE_EXECEEDED, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        }

        if (hasTotalKeysSizeLimit
            && txReads.TotalKeysSize > *dataTx->PerShardKeysSizeLimitBytes()) {
            TString err = TStringBuilder()
                << "Transaction total keys size " << txReads.TotalKeysSize
                << " exceeds limit " << *dataTx->PerShardKeysSizeLimitBytes()
                << " at tablet " << DataShard.TabletID() << " txId " << op->GetTxId();

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                ->AddError(NKikimrTxDataShard::TError::READ_SIZE_EXECEEDED, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        }

        for (const auto& key : dataTx->TxInfo().Keys) {
            if (key.IsWrite && DataShard.IsUserTable(key.Key->TableId)) {
                ui64 keySize = 0;
                for (const auto& cell : key.Key->Range.From) {
                    keySize += cell.Size();
                }
                if (keySize > NLimits::MaxWriteKeySize) {
                    TString err = TStringBuilder()
                        << "Operation " << *op << " writes key of " << keySize
                        << " bytes which exceeds limit " << NLimits::MaxWriteKeySize
                        << " bytes at " << DataShard.TabletID();

                    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
                        ->AddError(NKikimrTxDataShard::TError::BAD_ARGUMENT, err);
                    op->Abort(EExecutionUnitKind::FinishPropose);

                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

                    return EExecutionStatus::Executed;
                }
                for (const auto& col : key.Key->Columns) {
                    if (col.Operation == TKeyDesc::EColumnOperation::Set ||
                        col.Operation == TKeyDesc::EColumnOperation::InplaceUpdate)
                    {
                        if (col.ImmediateUpdateSize > NLimits::MaxWriteValueSize) {
                            TString err = TStringBuilder()
                                << "Transaction write column value of " << col.ImmediateUpdateSize
                                << " bytes is larger than the allowed threshold";

                            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR)->AddError(NKikimrTxDataShard::TError::BAD_ARGUMENT, err);
                            op->Abort(EExecutionUnitKind::FinishPropose);

                            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, err);

                            return EExecutionStatus::Executed;
                        }
                    }
                }

                if (DataShard.IsSubDomainOutOfSpace()) {
                    switch (key.Key->RowOperation) {
                        case TKeyDesc::ERowOperation::Read:
                        case TKeyDesc::ERowOperation::Erase:
                            // Read and erase are allowed even when we're out of disk space
                            break;

                        default: {
                            // Updates are not allowed when database is out of space
                            TString err = "Cannot perform writes: database is out of disk space";

                            DataShard.IncCounter(COUNTER_PREPARE_DISK_SPACE_EXHAUSTED);

                            BuildResult(op)->AddError(NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED, err);
                            op->Abort(EExecutionUnitKind::FinishPropose);

                            LOG_LOG_S_THROTTLE(DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::CheckDataTxUnit_Execute), ctx, NActors::NLog::PRI_ERROR, NKikimrServices::TX_DATASHARD, err);

                            return EExecutionStatus::Executed;
                        }
                    }
                }
            }
        }
    }

    if (op->IsReadTable()) {
        const auto& record = dataTx->GetReadTableTransaction();
        const auto& userTables = DataShard.GetUserTables();

        TMaybe<TString> schemaChangedError;
        if (auto it = userTables.find(record.GetTableId().GetTableId()); it != userTables.end()) {
            const auto& tableInfo = *it->second;
            for (const auto& columnRecord : record.GetColumns()) {
                if (auto* columnInfo = tableInfo.Columns.FindPtr(columnRecord.GetId())) {
                    // TODO: column types don't change when bound by id, but we may want to check anyway
                } else {
                    schemaChangedError = TStringBuilder() << "ReadTable cannot find column "
                        << columnRecord.GetName() << " (" << columnRecord.GetId() << ")";
                    break;
                }
            }
            // TODO: validate key ranges?
        } else {
            schemaChangedError = TStringBuilder() << "ReadTable cannot find table "
                << record.GetTableId().GetOwnerId() << ":" << record.GetTableId().GetTableId();
        }

        if (schemaChangedError) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR)
                ->AddError(NKikimrTxDataShard::TError::SCHEME_CHANGED, *schemaChangedError);
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::Executed;
        }

        if (record.HasSnapshotStep() && record.HasSnapshotTxId()) {
            if (!op->IsImmediate()) {
                BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)->AddError(
                    NKikimrTxDataShard::TError::BAD_ARGUMENT,
                    "ReadTable from snapshot must be an immediate transaction");
                op->Abort(EExecutionUnitKind::FinishPropose);
                return EExecutionStatus::Executed;
            }

            const TSnapshotKey key(
                record.GetTableId().GetOwnerId(),
                record.GetTableId().GetTableId(),
                record.GetSnapshotStep(),
                record.GetSnapshotTxId());

            if (!DataShard.GetSnapshotManager().AcquireReference(key)) {
                // TODO: try upgrading to mvcc snapshot when available
                BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)->AddError(
                    NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST,
                    TStringBuilder()
                        << "Shard " << DataShard.TabletID()
                        << " has no snapshot " << key);
                op->Abort(EExecutionUnitKind::FinishPropose);
                return EExecutionStatus::Executed;
            }

            op->SetAcquiredSnapshotKey(key);
            op->SetUsingSnapshotFlag();
        }
    }

    if (!op->IsImmediate()) {
        if (!Pipeline.AssignPlanInterval(op)) {
            TString err = TStringBuilder()
                << "Can't propose tx " << op->GetTxId() << " at blocked shard "
                << DataShard.TabletID();
            BuildResult(op)->AddError(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED, err);
            op->Abort(EExecutionUnitKind::FinishPropose);

            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

            return EExecutionStatus::Executed;
        }

        auto &res = BuildResult(op);
        res->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

        if (op->IsDataTx()) {
            res->Record.SetReadSize(txReads.ReadSize);
            res->Record.SetReplySize(txReads.ReplySize);

            for (const auto& rs : txReads.OutReadSetSize) {
                auto entry = res->Record.AddOutgoingReadSetInfo();
                entry->SetShardId(rs.first);
                entry->SetSize(rs.second);
            }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Prepared " << op->GetKind() << " transaction txId " << op->GetTxId()
                    << " at tablet " << DataShard.TabletID());
    }

    return EExecutionStatus::Executed;
}

void TCheckDataTxUnit::Complete(TOperation::TPtr,
                                const TActorContext &)
{
}

THolder<TExecutionUnit> CreateCheckDataTxUnit(TDataShard &dataShard,
                                              TPipeline &pipeline)
{
    return THolder(new TCheckDataTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
