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

        virtual TProposeResult ExecuteOnPropose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const override;
        virtual bool CompleteOnPropose(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) const override {
            return true;
        }

        virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

        virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) override;

        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    };

}
