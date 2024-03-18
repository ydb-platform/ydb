#include "controllers.h"
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>

namespace NKikimr::NOlap {

void TWaitCompactionController::OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) {
    ++TiersModificationsCount;
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("count", TiersModificationsCount);
}

bool TWaitCompactionController::DoOnStartCompaction(std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges>& changes) {
    if (!CompactionEnabledFlag) {
        changes = nullptr;
    }
    if (!TTLEnabled && dynamic_pointer_cast<NKikimr::NOlap::TTTLColumnEngineChanges>(changes)) {
        changes = nullptr;
    }
    return true;
}

bool TWaitCompactionController::DoOnWriteIndexComplete(const NKikimr::NOlap::TColumnEngineChanges& changes, const NKikimr::NColumnShard::TColumnShard& /*shard*/) {
    if (changes.TypeString() == NKikimr::NOlap::TTTLColumnEngineChanges::StaticTypeName()) {
        AtomicIncrement(TTLFinishedCounter);
    }
    if (changes.TypeString().find(NKikimr::NOlap::TInsertColumnEngineChanges::StaticTypeName()) != TString::npos) {
        AtomicIncrement(InsertFinishedCounter);
    }
    return true;
}

bool TWaitCompactionController::DoOnWriteIndexStart(const ui64 /*tabletId*/, const TString& changeClassName) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", changeClassName);
    if (changeClassName.find(NKikimr::NOlap::TTTLColumnEngineChanges::StaticTypeName()) != TString::npos) {
        AtomicIncrement(TTLStartedCounter);
    }
    if (changeClassName.find(NKikimr::NOlap::TInsertColumnEngineChanges::StaticTypeName()) != TString::npos) {
        AtomicIncrement(InsertStartedCounter);
    }
    return true;
}

void TWaitCompactionController::SetTiersSnapshot(TTestBasicRuntime& runtime, const TActorId& tabletActorId, const NMetadata::NFetcher::ISnapshot::TPtr& snapshot) {
    CurrentConfig = snapshot;
    ui32 startCount = TiersModificationsCount;
    NTxUT::ProvideTieringSnapshot(runtime, tabletActorId, snapshot);
    while (TiersModificationsCount == startCount) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}

}