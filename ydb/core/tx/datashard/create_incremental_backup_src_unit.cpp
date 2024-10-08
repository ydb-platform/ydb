#include "defs.h"
#include "execution_unit_ctors.h"
#include "datashard_active_transaction.h"
#include "datashard_impl.h"

// #define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

class TCreateIncrementalBackupSrcUnit : public TExecutionUnit {
public:
    bool IsRelevant(TActiveTransaction* tx) const {
        return tx->GetSchemeTx().HasCreateIncrementalBackupSrc();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override final {
        Y_UNUSED(op, txc, ctx);
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        return EExecutionStatus::Executed;

        auto rv = DataShard.GetMvccTxVersion(EMvccTxMode::ReadWrite, nullptr);

        Cerr << rv.Step << " " << rv.TxId << Endl;;

        if (!IsRelevant(tx)) {
            return EExecutionStatus::Reschedule;
        }

        return EExecutionStatus::Reschedule;
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override final {
        if (!op->IsWaitingForSnapshot()) {
            return true;
        }

        return !op->InputSnapshots().empty();
    }

    void Complete(TOperation::TPtr, const TActorContext&) override final {
    }
public:
    TCreateIncrementalBackupSrcUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateIncrementalBackupSrc, false, self, pipeline)
    {
    }

}; // TBackupIncrementalBackupSrcUnit

THolder<TExecutionUnit> CreateIncrementalBackupSrcUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCreateIncrementalBackupSrcUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
