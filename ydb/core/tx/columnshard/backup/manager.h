#pragma once

#include "task.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <util/generic/hash.h>


namespace NKikimr::NColumnShard {

    class TBackupManager {
    public:
        bool RegisterBackupTask(const TBackupTask::TPtr& backupTask, NTabletFlatExecutor::TTransactionContext& txc) {
            auto it = Tasks.find(backupTask->GetTaskId());

            NIceDb::TNiceDb db(txc.DB);
            if (it == Tasks.end()) {
                Tasks.emplace(backupTask->GetTaskId(), backupTask);
                db.Table<Schema::Backups>().Key(backupTask->GetPathId()).Update(
                        NIceDb::TUpdate<Schema::Backups::Status>((ui64)EBackupStatus::Draft));
                return true;
            }
            if (*it->second == *backupTask) {
                db.Table<Schema::Backups>().Key(backupTask->GetPathId()).Update(
                        NIceDb::TUpdate<Schema::Backups::Status>((ui64)EBackupStatus::Draft));
                return true;
            }
            return false;
        }

        void StartBackupTaskExecute(const TBackupTask::TPtr& backupTask, NTabletFlatExecutor::TTransactionContext& txc) {
            auto it = Tasks.find(backupTask->GetTaskId());
            AFL_VERIFY(it != Tasks.end())("debug", backupTask->DebugString());
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Backups>().Key(backupTask->GetPathId()).Update(
                    NIceDb::TUpdate<Schema::Backups::Status>((ui64)EBackupStatus::Started));
        }

        void StartBackupTaskComplete(const TBackupTask::TPtr& backupTask, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto it = Tasks.find(backupTask->GetTaskId());
            AFL_VERIFY(it != Tasks.end())("debug", backupTask->DebugString());
            // Create backup actor;
        }

        void RemoveBackupTask(const TBackupTask::TPtr& backupTask, NTabletFlatExecutor::TTransactionContext& txc) {
            auto it = Tasks.find(backupTask->GetTaskId());
            if (it != Tasks.end()) {
                AFL_VERIFY(it->second->GetStatus() == EBackupStatus::Draft)("status", (ui64) it->second->GetStatus());
            }
            Tasks.erase(backupTask->GetTaskId());
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Backups>().Key(backupTask->GetPathId()).Delete();
        }

    private:
        THashMap<TString, TBackupTask::TPtr> Tasks;
    };
}
