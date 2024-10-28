#include "optimizer.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

TDuration GetCommonFreshnessCheckDuration() {
    return NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration();
}

}
