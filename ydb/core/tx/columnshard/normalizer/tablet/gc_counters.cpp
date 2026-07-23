#include "gc_counters.h"

#include <ydb/core/tx/columnshard/private_events/events.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TGCCountersNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    TBlobManagerDb::SaveGCBarrierPreparation(txc.DB, TGenStep());
    return std::vector<INormalizerTask::TPtr>();
}

}   // namespace NKikimr::NOlap
