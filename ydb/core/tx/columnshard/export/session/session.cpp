#include "session.h"
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/export/actor/export_actor.h>
#include <ydb/core/tx/columnshard/export/transactions/tx_save_cursor.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>

namespace NKikimr::NOlap::NExport {

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSession::SaveCursorTx(NColumnShard::TColumnShard* shard, TCursor&& newCursor, const std::shared_ptr<TSession>& selfPtr) const {
    AFL_VERIFY(IsStarted());
    AFL_VERIFY(ExportActorId);
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxSaveCursor(shard, selfPtr, std::move(newCursor), *ExportActorId));
}

void TSession::Stop() {
    if (IsStarted()) {
        AFL_VERIFY(ExportActorId);
        NActors::TActivationContext::AsActorContext().Send(*ExportActorId, new TEvents::TEvPoisonPill);
        ExportActorId = {};
    }
}

bool TSession::Start(const std::shared_ptr<IStoragesManager>& storages, const TTabletId tabletId, const TActorId& tabletActorId) {
    AFL_VERIFY(IsConfirmed());
    auto blobsOperator = Task->GetStorageInitializer()->InitializeOperator(storages);
    if (!blobsOperator) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "problem_on_export_start")("reason", "cannot_initialize_operator")("problem", blobsOperator.GetErrorMessage());
        return false;
    }
    AFL_VERIFY(!ExportActorId);
    ExportActorId = NActors::TActivationContext::AsActorContext().Register(new TActor(tabletActorId, tabletId,
        Task->GetSerializer(), Task->GetSelector(), blobsOperator.DetachResult(), Task->GetIdentifier(), Cursor));
    Status = EStatus::Started;
    return true;
}

void TSession::SaveFullToDB(NTable::TDatabase& tdb) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(tdb);
    db.Table<Schema::ExportPersistentSessions>().Key(Task->GetIdentifier().ToString()).Update(
        NIceDb::TUpdate<Schema::ExportPersistentSessions::Status>(::ToString(Status)),
        NIceDb::TUpdate<Schema::ExportPersistentSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString()),
        NIceDb::TUpdate<Schema::ExportPersistentSessions::Task>(Task->SerializeToProto().SerializeAsString())
    );
}

void TSession::SaveCursorToDB(NTable::TDatabase& tdb) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(tdb);
    if (Status != EStatus::Started) {
        db.Table<Schema::ExportPersistentSessions>().Key(Task->GetIdentifier().ToString()).Update(
            NIceDb::TUpdate<Schema::ExportPersistentSessions::Status>(::ToString(Status)),
            NIceDb::TUpdate<Schema::ExportPersistentSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString())
        );
    } else {
        db.Table<Schema::ExportPersistentSessions>().Key(Task->GetIdentifier().ToString()).Update(
            NIceDb::TUpdate<Schema::ExportPersistentSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString())
        );
    }
}

}
