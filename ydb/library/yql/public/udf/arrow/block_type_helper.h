#pragma once
#include "block_item_comparator.h"
#include <ydb/library/yql/public/udf/udf_type_size_check.h>

namespace NYql {
namespace NUdf {

// ABI stable
class IBlockTypeHelper {
public:
    virtual ~IBlockTypeHelper() = default;
    virtual IBlockItemComparator::TPtr MakeComparator(TType* type) const = 0;
};

UDF_ASSERT_TYPE_SIZE(IBlockTypeHelper, 8);

}
}
