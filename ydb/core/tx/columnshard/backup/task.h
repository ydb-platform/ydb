#pragma once

#include <ydb/core/protos/tx_columnshard.pb.h>


namespace NKikimr::NColumnShard {

    enum class EBackupStatus : ui64 {
        Draft = 0,
        Started = 1
    };

    class TBackupTask {
        YDB_ACCESSOR(ui64, PathId, 0);
        YDB_ACCESSOR(EBackupStatus, Status, EBackupStatus::Draft);
        YDB_ACCESSOR(NOlap::TSnapshot, Snapshot, NOlap::TSnapshot::Zero());
    public:
        using TPtr = std::shared_ptr<TBackupTask>;

        TBackupTask(EBackupStatus status)
            : Status(status)
        {}

        TString GetTaskId() const {
            return TStringBuilder() << PathId;
        }

        TString DebugString() const {
            return TStringBuilder() << "task_id=" << GetTaskId() << ";path_id=" << PathId;
        }

        bool Parse(const NKikimrSchemeOp::TBackupTask& backupTask) {
            PathId = backupTask.GetTableId();
            Snapshot = NOlap::TSnapshot(backupTask.GetSnapshotStep(), backupTask.GetSnapshotTxId());
            return Snapshot.Valid();
        }

        void Serialize(NKikimrSchemeOp::TBackupTask& backupTask) const {
            backupTask.SetTableId(PathId);
            backupTask.SetSnapshotStep(Snapshot.GetPlanStep());
            backupTask.SetSnapshotTxId(Snapshot.GetTxId());
        }

        bool operator==(const TBackupTask& other) const {
            return PathId == other.PathId && Snapshot == other.Snapshot;
        }
    };
}
