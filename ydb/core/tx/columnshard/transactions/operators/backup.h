#pragma once

#include "propose_tx.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/session/task.h>

namespace NKikimr::NColumnShard {

class TBackupTransactionOperator: public IProposeTxOperator {
private:
    using TBase = IProposeTxOperator;

    std::shared_ptr<NOlap::NExport::TExportTask> ExportTask;
    bool TaskExists = false;
    std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAddTask;
    std::unique_ptr<NTabletFlatExecutor::ITransaction> TxConfirm;
    std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAbort;
    using TProposeResult = TTxController::TProposeResult;
    static inline auto Registrator = TFactory::TRegistrator<TBackupTransactionOperator>(NKikimrTxColumnShard::TX_KIND_BACKUP);

    virtual TTxController::TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override;
    virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
    }
    virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
    }
    virtual bool DoIsAsync() const override {
        return true;
    }
    virtual bool DoParse(TColumnShard& owner, const TString& data) override;
    virtual TString DoDebugString() const override {
        return "BACKUP";
    }

public:
    using TBase::TBase;

    virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) override;

    virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
        return true;
    }
};
}

