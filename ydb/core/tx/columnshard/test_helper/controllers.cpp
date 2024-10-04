#include "columnshard_ut_common.h"
#include "controllers.h"
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>

namespace NKikimr::NOlap {

void TWaitCompactionController::OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) {
    ++TiersModificationsCount;
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("count", TiersModificationsCount);
}

void TWaitCompactionController::SetTiersSnapshot(TTestBasicRuntime& runtime, const TActorId& tabletActorId,
    NColumnShard::NTiers::TTiersSnapshot tiers, NColumnShard::TTiersManager::TTieringRules tieringRules) {
    TiersSnapshot = std::move(tiers);
    TieringRulesSnapshot = std::move(tieringRules);
    ui32 startCount = TiersModificationsCount;
    NTxUT::RefreshTiering(runtime, tabletActorId);
    while (TiersModificationsCount == startCount) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}
}