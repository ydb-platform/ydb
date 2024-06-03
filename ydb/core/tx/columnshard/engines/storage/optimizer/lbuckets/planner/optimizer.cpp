#include "optimizer.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

TDuration GetCommonFreshnessCheckDuration() {
    static const TDuration CommonFreshnessCheckDuration = TDuration::Seconds(300);
    return NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration(CommonFreshnessCheckDuration);
}

}
