#include "tx_save_progress.h"
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>

namespace NKikimr::NOlap::NBackground {

bool TTxSaveSessionProgress::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Adapter->SaveProgressToLocalDatabase(txc, Session->SerializeToLocalDatabaseRecord());
    return true;
}

void TTxSaveSessionProgress::DoComplete(const TActorContext& /*ctx*/) {
}

}
