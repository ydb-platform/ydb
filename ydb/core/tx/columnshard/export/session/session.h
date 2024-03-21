#pragma once
#include "task.h"
#include "cursor.h"
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NTabletFlatExecutor {
class ITransaction;
}

namespace NKikimr::NOlap {
class IStoragesManager;
}

namespace NKikimr::NOlap::NExport {
    class TSession {
    public:
        enum class EStatus: ui64 {
            Draft = 0 /*"draft"*/,
            Confirmed = 1 /*"confirmed"*/,
            Started = 2 /*"started"*/,
            Finished = 3 /*"finished"*/
        };

    private:
        std::shared_ptr<TExportTask> Task;
        EStatus Status = EStatus::Draft;
        TCursor Cursor;
        std::optional<TActorId> ExportActorId;
    public:
        void SetCursor(const TCursor& cursor) {
            Cursor = cursor;
            if (Cursor.IsFinished()) {
                Finish();
            }
        }

        TSession(const std::shared_ptr<TExportTask>& task)
            : Task(task) {
            AFL_VERIFY(Task);
        }

        TSession(const std::shared_ptr<TExportTask>& task, const EStatus status, TCursor&& cursor)
            : Task(task)
            , Status(status)
            , Cursor(std::move(cursor)) {
            AFL_VERIFY(Status != EStatus::Started);
            AFL_VERIFY(Task);
        }

        bool IsConfirmed() const {
            return Status == EStatus::Confirmed;
        }

        void SaveFullToDB(NTable::TDatabase& tdb);

        void SaveCursorToDB(NTable::TDatabase& tdb);

        [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> SaveCursorTx(NColumnShard::TColumnShard* shard, TCursor&& newCursor, const std::shared_ptr<TSession>& selfPtr) const;

        const TCursor& GetCursor() const {
            return Cursor;
        }

        TString DebugString() const {
            return TStringBuilder() << "task=" << Task->DebugString() << ";status=" << Status;
        }

        bool IsDraft() const {
            return Status == EStatus::Draft;
        }

        void Confirm() {
            AFL_VERIFY(IsDraft());
            Status = EStatus::Confirmed;
        }

        bool IsStarted() const {
            return Status == EStatus::Started;
        }

        const TIdentifier& GetIdentifier() const {
            return Task->GetIdentifier();
        }

        [[nodiscard]] bool Start(const std::shared_ptr<IStoragesManager>& storages, const TTabletId tabletId, const TActorId& tabletActorId);
        void Stop();
        void Finish() {
            AFL_VERIFY(Status == EStatus::Started);
            Status = EStatus::Finished;
        }

    };
}
