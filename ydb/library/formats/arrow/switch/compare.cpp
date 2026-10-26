#include "compare.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NArrow {

bool TComparator::IsNumericDecimal(const NScheme::TTypeInfo& logicalType) {
    return logicalType.GetTypeId() == NScheme::NTypeIds::Decimal && HasAppData() &&
           AppDataVerified().ColumnShardConfig.GetEnableDecimalNumericComparison();
}

}   // namespace NKikimr::NArrow
