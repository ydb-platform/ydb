#include "compare.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NArrow {

bool TComparator::IsDecimalNumericComparisonEnabled() {
    return HasAppData() && AppDataVerified().ColumnShardConfig.GetEnableDecimalNumericComparison();
}

}   // namespace NKikimr::NArrow
