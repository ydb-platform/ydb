#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <arrow/array/data.h>

#include <util/generic/size_literals.h>

#include <limits>

namespace NKikimr {
namespace NMiniKQL {

constexpr size_t MaxBlockSizeInBytes = 1_MB;
static_assert(MaxBlockSizeInBytes < (size_t)std::numeric_limits<i32>::max());

// maximum size of block item in bytes
size_t CalcMaxBlockItemSize(const TType* type);

inline size_t CalcBlockLen(size_t maxBlockItemSize) {
    return MaxBlockSizeInBytes / std::max<size_t>(maxBlockItemSize, 1);
}

class IBlockBuilder {
public:
    virtual ~IBlockBuilder() = default;
    virtual size_t MaxLength() const = 0;
    virtual void Add(NUdf::TUnboxedValuePod value) = 0;
    virtual void AddMany(const arrow::ArrayData& array, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) = 0;
    virtual NUdf::TUnboxedValuePod Build(TComputationContext& ctx, bool finish) = 0;
};

std::unique_ptr<IBlockBuilder> MakeBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxBlockLength);

}
}
