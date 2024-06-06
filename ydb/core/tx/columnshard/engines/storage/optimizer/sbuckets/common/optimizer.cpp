#include "optimizer.h"
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

TDuration GetCommonFreshnessCheckDuration() {
    static const TDuration CommonFreshnessCheckDuration = TDuration::Seconds(300);
    return NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration(CommonFreshnessCheckDuration);
}

}
