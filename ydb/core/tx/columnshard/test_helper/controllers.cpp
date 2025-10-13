#include "columnshard_ut_common.h"
#include "controllers.h"

#include <ydb/core/tx/columnshard/engines/changes/ttl.h>

namespace NKikimr::NOlap {

void TWaitCompactionController::OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) {
    ++TiersModificationsCount;
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("count", TiersModificationsCount);
}

void TWaitCompactionController::OverrideTierConfigs(
    TTestBasicRuntime& runtime, const TActorId& tabletActorId, THashMap<TString, NColumnShard::NTiers::TTierConfig> tiers) {
    OverrideTiers = std::move(tiers);
    ui32 startCount = TiersModificationsCount;
    NTxUT::RefreshTiering(runtime, tabletActorId);
    while (TiersModificationsCount == startCount) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}
}   // namespace NKikimr::NOlap
