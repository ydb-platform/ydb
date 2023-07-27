#pragma once
#include "block_item_comparator.h"
#include "block_item_hasher.h"
#include <ydb/library/yql/public/udf/udf_type_size_check.h>
#include <ydb/library/yql/public/udf/udf_version.h>

namespace NYql {
namespace NUdf {

// ABI stable
class IBlockTypeHelper1 {
public:
    virtual ~IBlockTypeHelper1() = default;
    virtual IBlockItemComparator::TPtr MakeComparator(TType* type) const = 0;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 34)
class IBlockTypeHelper2 : public IBlockTypeHelper1 {
public:
    virtual IBlockItemHasher::TPtr MakeHasher(TType *type) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 34)
class IBlockTypeHelper : public IBlockTypeHelper2 {
public:
    IBlockTypeHelper();
};
#else
class IBlockTypeHelper : public IBlockTypeHelper1 {
public:
    IBlockTypeHelper();
};
#endif

UDF_ASSERT_TYPE_SIZE(IBlockTypeHelper, 8);

}
}
