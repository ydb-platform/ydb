#include "manager.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

namespace NKikimr::NOlap::NExport {

void TExportsManager::Start(const NColumnShard::TColumnShard* shard) {
    for (auto&& i : Sessions) {
        if (i.second->IsConfirmed()) {
            AFL_VERIFY(i.second->Start(shard->GetStoragesManager(), (TTabletId)shard->TabletID(), shard->SelfId()));
        }
    }
}

bool TExportsManager::Load(NTable::TDatabase& database) {
    NIceDb::TNiceDb db(database);
    using namespace NColumnShard;
    {
        auto rowset = db.Table<Schema::ExportPersistentSessions>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            NKikimrColumnShardExportProto::TExportTask taskProto;
            AFL_VERIFY(taskProto.ParseFromString(rowset.GetValue<Schema::ExportPersistentSessions::Task>()));
            auto task = TExportTask::BuildFromProto(taskProto);
            AFL_VERIFY(task)("event", "cannot_parse_export_session_task")("error", task.GetErrorMessage());

            NKikimrColumnShardExportProto::TCursor cursorProto;
            AFL_VERIFY(cursorProto.ParseFromString(rowset.GetValue<Schema::ExportPersistentSessions::Cursor>()));
            auto cursor = TCursor::BuildFromProto(cursorProto);
            AFL_VERIFY(cursor)("event", "cannot_parse_export_session_cursor")("error", cursor.GetErrorMessage());

            TSession::EStatus status;
            AFL_VERIFY(TryFromString(rowset.GetValue<Schema::ExportPersistentSessions::Status>(), status));

            auto session = std::make_shared<TSession>(std::make_shared<TExportTask>(task.DetachResult()), status, cursor.DetachResult());

            AFL_VERIFY(Sessions.emplace(session->GetIdentifier(), session).second);
            if (!rowset.Next()) {
                return false;
            }
        }

    }
    return true;
}

void TExportsManager::RemoveSession(const NExport::TIdentifier& id, NTabletFlatExecutor::TTransactionContext& txc) {
    auto session = GetSessionOptional(id);
    if (session) {
        AFL_VERIFY(session->IsDraft());
    }
    Sessions.erase(id);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<NColumnShard::Schema::ExportPersistentSessions>().Key(id.ToString()).Delete();
}

void TExportsManager::Stop() {
    for (auto&& i : Sessions) {
        i.second->Stop();
    }
}

}
