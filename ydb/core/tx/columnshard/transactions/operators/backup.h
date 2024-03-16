#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/manager/manager.h>
#include <ydb/core/tx/columnshard/export/session/task.h>

namespace NKikimr::NColumnShard {

    class TBackupTransactionOperator : public TTxController::ITransactionOperatior {
    private:
        std::shared_ptr<NOlap::NExport::TExportTask> ExportTask;
        using TBase = TTxController::ITransactionOperatior;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TBackupTransactionOperator>(NKikimrTxColumnShard::TX_KIND_BACKUP);
    public:
        using TBase::TBase;

        virtual bool Parse(const TString& data) override;

        virtual TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, bool /*proposed*/) const override;

        virtual bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

        virtual bool Complete(TColumnShard& owner, const TActorContext& ctx) override;

        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    };

}
