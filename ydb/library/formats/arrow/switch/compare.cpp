#include "compare.h"

namespace NKikimr::NArrow {

TComparator::EComparisonOrder TComparator::GetComparisonOrder(const NScheme::TTypeInfo& logicalType) {
    return logicalType.IsDecimalNumericComparisonEnabled() ? EComparisonOrder::DecimalNumeric : EComparisonOrder::Default;
}

}   // namespace NKikimr::NArrow
