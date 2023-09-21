#include "gc.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap {

void IBlobsGCAction::OnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    self.GetStoragesManager()->GetOperator(GetStorageId())->FinishGC();
    return DoOnCompleteTxAfterCleaning(self, taskAction);
}

}
