#pragma once

#include "defs.h"
#include "const.h"
#include "execution_unit_kind.h"
#include "operation.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>

namespace NKikimr {
namespace NDataShard {

using NTabletFlatExecutor::TTransactionContext;

class TDataShard;
class TPipeline;

enum class EExecutionStatus {
    // Transaction should be restarted to get
    // missing data or more memory.
    Restart,
    // Execution completed. Should move to the
    // next execution unit or finish operation
    // processing.
    Executed,
    // Same as Executed  but following units are
    // not allowed to restart current tablet tx.
    ExecutedNoMoreRestarts,
    // Stay on the same execution unit and
    // continue execution when operation is
    // ready again.
    Continue,
    // Stay on the same execution unit and
    // continue execution in another transaction
    // as soon as possible.
    Reschedule,
    // Execution completed. Should wait for tx
    // commit then call Complete and move to
    // to the next execution unit.
    WaitComplete,
    // Execution completed. Move to the next
    // execution unit but call Complete after
    // eventual tx commit.
    DelayComplete,
    // Same as DelayComplete but following units
    // are not allowed to restart current tablet
    // tx.
    DelayCompleteNoMoreRestarts,
};

class TExecutionUnit
{
public:

    TExecutionUnit(EExecutionUnitKind kind,
                   bool executionMightRestart,
                   TDataShard &dataShard,
                   TPipeline &pipeline)
        : Kind(kind)
        , ExecutionMightRestart(executionMightRestart)
        , DataShard(dataShard)
        , Pipeline(pipeline)
    {
    }

    virtual ~TExecutionUnit()
    {
    }

    void AddOperation(TOperation::TPtr op)
    {
        OpsInFly.insert(op);
    }

    void RemoveOperation(TOperation::TPtr op)
    {
        OpsInFly.erase(op);
    }

    ui64 GetInFly() const { return OpsInFly.size(); }

    EExecutionUnitKind GetKind() const { return Kind; }

    // Return true if Execute method might return
    // EExecutionStatus::Restart.
    bool GetExecutionMightRestart() const { return ExecutionMightRestart; }

    virtual bool IsReadyToExecute(TOperation::TPtr op) const = 0;
    virtual TOperation::TPtr FindReadyOperation() const
    {
        Y_FAIL_S("FindReadyOperation is not implemented for execution unit " << Kind);
    }

    virtual EExecutionStatus Execute(TOperation::TPtr op,
                                     TTransactionContext &txc,
                                     const TActorContext &ctx) = 0;
    virtual void Complete(TOperation::TPtr op,
                          const TActorContext &ctx) = 0;
protected:
    // Call during unit execution when it's ok to reject operation before completion
    // Returns true if operation has been rejected as a result of this call
    bool CheckRejectDataTx(TOperation::TPtr op, const TActorContext& ctx);

    // Returns true if CheckRejectDataTx will reject operation when called
    bool WillRejectDataTx(TOperation::TPtr op) const;

    TOutputOpData::TResultPtr &BuildResult(TOperation::TPtr op,
                                           NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status = NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);

    bool MaybeRequestMoreTxMemory(ui64 usage,
                                  NTabletFlatExecutor::TTransactionContext &txc)
    {
        if (usage > txc.GetMemoryLimit()) {
            ui64 request = Max(usage - txc.GetMemoryLimit(), txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);
            txc.RequestMemory(request);
            return true;
        }
        return false;
    }

    const EExecutionUnitKind Kind;
    bool ExecutionMightRestart;
    TDataShard &DataShard;
    TPipeline &Pipeline;
    THashSet<TOperation::TPtr> OpsInFly;
};

THolder<TExecutionUnit> CreateExecutionUnit(EExecutionUnitKind kind,
                                            TDataShard &dataShard,
                                            TPipeline &pipeline);

} // namespace NDataShard
} // namespace NKikimr
