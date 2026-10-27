#include "compare.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NArrow {

TComparator::EComparisonOrder TComparator::GetComparisonOrder(const NScheme::TTypeInfo& logicalType) {
    if (logicalType.GetTypeId() == NScheme::NTypeIds::Decimal && HasAppData() &&
        AppDataVerified().ColumnShardConfig.GetEnableDecimalNumericComparison()) {
        return EComparisonOrder::DecimalNumeric;
    }

    return EComparisonOrder::Default;
}

}   // namespace NKikimr::NArrow
