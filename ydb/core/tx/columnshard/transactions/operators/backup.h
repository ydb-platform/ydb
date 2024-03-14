#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/backup/manager.h>

namespace NKikimr::NColumnShard {

    class TBackupTransactionOperator : public TTxController::ITransactionOperatior {
        using TBase = TTxController::ITransactionOperatior;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TBackupTransactionOperator>(NKikimrTxColumnShard::TX_KIND_BACKUP);
    public:
        using TBase::TBase;

        virtual bool Parse(const TString& data) override {
            NKikimrTxColumnShard::TBackupTxBody txBody;
            if (!txBody.ParseFromString(data)) {
                return false;
            }
            if (!txBody.HasBackupTask()) {
                return false;
            }
            BackupTask = std::make_shared<TBackupTask>(EBackupStatus::Draft);
            return BackupTask->Parse(txBody.GetBackupTask());
        }

        TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, bool /*proposed*/) const override {
            if (!owner.BackupManager.RegisterBackupTask(BackupTask, txc)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                            TStringBuilder() << "Invalid backup task TxId# " << GetTxId() << ": " << BackupTask->DebugString());
            }
            return TProposeResult();
        }

        virtual bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            Y_UNUSED(version);
            owner.BackupManager.StartBackupTaskExecute(BackupTask, txc);
            return true;
        }

        virtual bool Complete(TColumnShard& owner, const TActorContext& ctx) override {
            owner.BackupManager.StartBackupTaskComplete(BackupTask, ctx);
            auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
                owner.TabletID(), TxInfo.TxKind, GetTxId(), NKikimrTxColumnShard::SUCCESS);
                result->Record.SetStep(TxInfo.PlanStep);
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            owner.BackupManager.RemoveBackupTask(BackupTask, txc);
            return true;
        }
    private:
        TBackupTask::TPtr BackupTask;
    };

}
