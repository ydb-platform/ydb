#pragma once
#include "block_item_comparator.h"
#include "block_item_hasher.h"
#include <yql/essentials/public/udf/udf_type_size_check.h>
#include <yql/essentials/public/udf/udf_version.h>

#include <arrow/type.h>

namespace NYql::NUdf {

// ABI stable
class IBlockTypeHelper1 {
public:
    virtual ~IBlockTypeHelper1() = default;
    virtual IBlockItemComparator::TPtr MakeComparator(TType* type) const = 0;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 34)
class IBlockTypeHelper2: public IBlockTypeHelper1 {
public:
    virtual IBlockItemHasher::TPtr MakeHasher(TType* type) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 34)
class IBlockTypeHelper: public IBlockTypeHelper2 {
public:
    IBlockTypeHelper();
};
#else
class IBlockTypeHelper: public IBlockTypeHelper1 {
public:
    IBlockTypeHelper();
};
#endif

UDF_ASSERT_TYPE_SIZE(IBlockTypeHelper, 8);

template <EDataSlot slot>
std::shared_ptr<arrow::DataType> MakeTzLayoutArrowType() {
    static_assert(slot == EDataSlot::TzDate || slot == EDataSlot::TzDatetime || slot == EDataSlot::TzTimestamp || slot == EDataSlot::TzDate32 || slot == EDataSlot::TzDatetime64 || slot == EDataSlot::TzTimestamp64,
                  "Expected tz date type slot");

    if constexpr (slot == EDataSlot::TzDate) {
        return arrow::uint16();
    }
    if constexpr (slot == EDataSlot::TzDatetime) {
        return arrow::uint32();
    }
    if constexpr (slot == EDataSlot::TzTimestamp) {
        return arrow::uint64();
    }
    if constexpr (slot == EDataSlot::TzDate32) {
        return arrow::int32();
    }
    if constexpr (slot == EDataSlot::TzDatetime64) {
        return arrow::int64();
    }
    if constexpr (slot == EDataSlot::TzTimestamp64) {
        return arrow::int64();
    }
}

} // namespace NYql::NUdf
