#include "tx_remove.h"
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>

namespace NKikimr::NOlap::NBackground {

bool TTxRemoveSession::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Adapter->RemoveSessionFromLocalDatabase(txc, ClassName, Identifier);
    return true;
}

void TTxRemoveSession::Complete(const TActorContext& /*ctx*/) {
    AFL_VERIFY(Sessions->RemoveSession(ClassName, Identifier))("class_name", ClassName)("id", Identifier);
}

}
