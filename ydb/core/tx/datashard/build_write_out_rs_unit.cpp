#include "datashard_impl.h"
#include "datashard_kqp.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TBuildWriteOutRSUnit : public TExecutionUnit {
public:
    TBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline);
    ~TBuildWriteOutRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext& ctx) override;

private:
    EExecutionStatus OnTabletNotReady(TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx);
};

TBuildWriteOutRSUnit::TBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline)
    : TExecutionUnit(EExecutionUnitKind::BuildWriteOutRS, true, dataShard, pipeline) {}

TBuildWriteOutRSUnit::~TBuildWriteOutRSUnit() {}

bool TBuildWriteOutRSUnit::IsReadyToExecute(TOperation::TPtr) const {
    return true;
}

EExecutionStatus TBuildWriteOutRSUnit::Execute(TOperation::TPtr op, TTransactionContext& txc,
    const TActorContext& ctx)
{
    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);
    auto writeTx = writeOp->GetWriteTx();
    Y_ENSURE(writeTx);

    DataShard.ReleaseCache(*writeOp);

    if (writeOp->IsTxDataReleased()) {
        switch (Pipeline.RestoreWriteTx(writeOp, txc)) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ENSURE(false, "Failed to restore writeOp data: " << writeTx->GetErrStr());
        }
    }

    TDataShardLocksDb locksDb(DataShard, txc);
    TSetupSysLocks guardLocks(op, DataShard, &locksDb);

    ui64 tabletId = DataShard.TabletID();

    if (writeTx->CheckCancelled()) {
        writeOp->ReleaseTxData(txc);
        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
        DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
        return EExecutionStatus::Executed;
    }

    try {
        const auto& kqpLocks = writeTx->GetKqpLocks() ? writeTx->GetKqpLocks().value() : NKikimrDataEvents::TKqpLocks{};
        KqpFillOutReadSets(op->OutReadSets(), kqpLocks, true, DataShard.SysLocksTable(), tabletId);
    } catch (const TNotReadyTabletException&) {
        YDB_LOG_CTX_CRIT(ctx, "Unexpected TNotReadyTabletException exception at build out rs");
        return OnTabletNotReady(*writeOp, txc, ctx);
    } catch (const yexception& e) {
        YDB_LOG_CTX_CRIT(ctx, "Exception while preparing out-readsets for KQP transaction at",
            {"#_*op", *op},
            {"TabletID", DataShard.TabletID()},
            {"what", e.what()});
        if (op->IsImmediate()) {
            writeOp->ReleaseTxData(txc);
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Tx was terminated: " << e.what());
            return EExecutionStatus::Executed;
        } else {
            throw;
        }
    }

    return EExecutionStatus::Executed;
}

void TBuildWriteOutRSUnit::Complete(TOperation::TPtr, const TActorContext&) {}

EExecutionStatus TBuildWriteOutRSUnit::OnTabletNotReady(TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx)
{
    YDB_LOG_CTX_TRACE(ctx, "Tablet is not ready for execution",
        {"TabletID", DataShard.TabletID()},
        {"writeOp", writeOp});

    DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

    writeOp.ReleaseTxData(txc);
    return EExecutionStatus::Restart;
}

THolder<TExecutionUnit> CreateBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TBuildWriteOutRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
