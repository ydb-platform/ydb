#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/manager/manager.h>
#include <ydb/core/tx/columnshard/export/session/task.h>

namespace NKikimr::NColumnShard {

    class TBackupTransactionOperator : public TTxController::ITransactionOperator {
    private:
        std::shared_ptr<NOlap::NExport::TExportTask> ExportTask;
        using TBase = TTxController::ITransactionOperator;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TBackupTransactionOperator>(NKikimrTxColumnShard::TX_KIND_BACKUP);
    public:
        using TBase::TBase;

        virtual bool AllowTxDups() const override {
            return true;
        }

        virtual bool Parse(TColumnShard& owner, const TString& data) override;

        virtual TProposeResult ExecuteOnPropose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const override;
        virtual bool CompleteOnPropose(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) const override {
            return true;
        }

        virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

        virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) override;

        virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
        virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
            return true;
        }
    };

}
