#include "controller.h"
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TController::DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& change, const ::NKikimr::NColumnShard::TColumnShard& shard) {
    TGuard<TMutex> g(Mutex);
    if (SharingIds.empty()) {
        TCheckContext context;
        CheckInvariants(shard, context);
    }
    return TBase::DoOnWriteIndexComplete(change, shard);
}

void TController::DoOnAfterGCAction(const ::NKikimr::NColumnShard::TColumnShard& /*shard*/, const NOlap::IBlobsGCAction& action) {
    TGuard<TMutex> g(Mutex);
    for (auto d = action.GetBlobsToRemove().GetDirect().GetIterator(); d.IsValid(); ++d) {
        AFL_VERIFY(RemovedBlobIds[action.GetStorageId()][d.GetBlobId()].emplace(d.GetTabletId()).second);
    }
//    if (SharingIds.empty()) {
//        CheckInvariants();
//    }
}
