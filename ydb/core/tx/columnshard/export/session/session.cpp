#include "session.h"
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/export/transactions/tx_save_cursor.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>

namespace NKikimr::NOlap::NExport {

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSession::SaveCursorTx(NColumnShard::TColumnShard* shard, TCursor&& newCursor, const std::shared_ptr<TSession>& selfPtr) const {
    AFL_VERIFY(IsStarted());
    AFL_VERIFY(ExportActorId);
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxSaveCursor(shard, selfPtr, std::move(newCursor), *ExportActorId));
}

bool TSession::Start(const std::shared_ptr<IStoragesManager>& /*storages*/) {
    AFL_VERIFY(Status == EStatus::Confirmed);
//    auto blobsOperator = storages->GetOperatorOptional(Task->GetStorageId());
//    if (!blobsOperator) {
//        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "problem_on_export_start")("reason", "no_blob_operator")("storage_id", Task->GetStorageId());
//        return false;
//    }
//    ExportActorId = NActors::TActivationContext::AsActorContext().Register(new TActor(ShardActorId, ShardTabletId, controller, blobsOperator, Task->GetIdentifier()));
    AFL_VERIFY(false);
    Status = EStatus::Started;
    return true;
}

void TSession::SaveFullToDB(NTable::TDatabase& tdb) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(tdb);
    db.Table<Schema::ExportSessions>().Key(Task->GetIdentifier().ToString()).Update(
        NIceDb::TUpdate<Schema::ExportSessions::Status>(::ToString(Status)),
        NIceDb::TUpdate<Schema::ExportSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString()),
        NIceDb::TUpdate<Schema::ExportSessions::Task>(Task->SerializeToProto().SerializeAsString())
    );
}

void TSession::SaveCursorToDB(NTable::TDatabase& tdb) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(tdb);
    if (Status != EStatus::Started) {
        db.Table<Schema::ExportSessions>().Key(Task->GetIdentifier().ToString()).Update(
            NIceDb::TUpdate<Schema::ExportSessions::Status>(::ToString(Status)),
            NIceDb::TUpdate<Schema::ExportSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString())
        );
    } else {
        db.Table<Schema::ExportSessions>().Key(Task->GetIdentifier().ToString()).Update(
            NIceDb::TUpdate<Schema::ExportSessions::Cursor>(Cursor.SerializeToProto().SerializeAsString())
        );
    }
}

}
