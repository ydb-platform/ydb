#include "gc_counters.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TGCCountersNormalizer::DoInit(const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    TBlobManagerDb::SaveGCBarrierPreparation(txc.DB, TGenStep());
    return std::vector<INormalizerTask::TPtr>();
}

}
