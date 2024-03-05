#include "datashard_impl.h"
#include "datashard_kqp.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

#define LOG_T(stream) LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_C(stream) LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, stream)

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
    Y_ABORT_UNLESS(writeTx);

    DataShard.ReleaseCache(*writeOp);

    if (writeOp->IsTxDataReleased()) {
        switch (Pipeline.RestoreWriteTx(writeOp, txc)) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ABORT("Failed to restore writeOp data: %s", writeTx->GetErrStr().c_str());
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
        KqpFillOutReadSets(op->OutReadSets(), kqpLocks, true, nullptr, DataShard.SysLocksTable(), tabletId);
    } catch (const TNotReadyTabletException&) {
        LOG_C("Unexpected TNotReadyTabletException exception at build out rs");
        return OnTabletNotReady(*writeOp, txc, ctx);
    } catch (const yexception& e) {
        LOG_C("Exception while preparing out-readsets for KQP transaction " << *op << " at " << DataShard.TabletID() << ": " << e.what());
        if (op->IsImmediate()) {
            writeOp->ReleaseTxData(txc);
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Tx was terminated: " << e.what());
            return EExecutionStatus::Executed;
        } else {
            Y_FAIL_S("Unexpected exception in KQP out-readsets prepare: " << e.what());
        }
    }

    return EExecutionStatus::Executed;
}

void TBuildWriteOutRSUnit::Complete(TOperation::TPtr, const TActorContext&) {}

EExecutionStatus TBuildWriteOutRSUnit::OnTabletNotReady(TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx)
{
    LOG_T("Tablet " << DataShard.TabletID() << " is not ready for " << writeOp << " execution");

    DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

    writeOp.ReleaseTxData(txc);
    return EExecutionStatus::Restart;
}

THolder<TExecutionUnit> CreateBuildWriteOutRSUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TBuildWriteOutRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
