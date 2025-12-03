#pragma once

#include "propose_tx.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/backup/import/task.h>

namespace NKikimr::NColumnShard {

class TRestoreTransactionOperator: public IProposeTxOperator, public TMonitoringObjectsCounter<TRestoreTransactionOperator> {
private:
    using TBase = IProposeTxOperator;

    std::shared_ptr<NOlap::NImport::TImportTask> ImportTask;
    bool TaskExists = false;
    std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAddTask;
    std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAbort;
    using TProposeResult = TTxController::TProposeResult;
    static inline auto Registrator = TFactory::TRegistrator<TRestoreTransactionOperator>(NKikimrTxColumnShard::TX_KIND_RESTORE);

    virtual TTxController::TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override;
    virtual void DoFinishProposeOnExecute(TColumnShard & /*owner*/, NTabletFlatExecutor::TTransactionContext & /*txc*/) override;
    virtual void DoFinishProposeOnComplete(TColumnShard & /*owner*/, const TActorContext & /*ctx*/) override;
    virtual TString DoGetOpType() const override;
    virtual bool DoIsAsync() const override;
    virtual bool DoParse(TColumnShard& owner, const TString& data) override;
    virtual TString DoDebugString() const override;

  public:
    using TBase::TBase;

    virtual bool ProgressOnExecute(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) override;

    virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual bool CompleteOnAbort(TColumnShard & /*owner*/, const TActorContext & /*ctx*/) override;
};
}

