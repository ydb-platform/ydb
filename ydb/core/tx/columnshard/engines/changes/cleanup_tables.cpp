#include "cleanup_tables.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

void TCleanupTablesColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = TablesToDrop.size()) {
        out << "drop " << dropped << " tables: " << JoinSeq(",", TablesToDrop) << ";";
    }
}

void TCleanupTablesColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    if (self && context.DB) {
        for (auto&& t : TablesToDrop) {
            self->TablesManager.TryFinalizeDropPathOnExecute(*context.DB, t);
        }
    }
}

void TCleanupTablesColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& /*context*/) {
    for (auto&& t : TablesToDrop) {
        self->TablesManager.TryFinalizeDropPathOnComplete(t);
    }
    self->Subscribers->OnEvent(std::make_shared<NColumnShard::NSubscriber::TEventTablesErased>(TablesToDrop));
}

void TCleanupTablesColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanupTables();
}

void TCleanupTablesColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanupTables();
}

NColumnShard::ECumulativeCounters TCleanupTablesColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}
