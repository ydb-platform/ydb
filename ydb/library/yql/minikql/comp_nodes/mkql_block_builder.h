#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <util/generic/size_literals.h>

#include <limits>

namespace NKikimr {
namespace NMiniKQL {

constexpr size_t MaxBlockSizeInBytes = 1_MB;
static_assert(MaxBlockSizeInBytes < (size_t)std::numeric_limits<i32>::max());

class IBlockBuilder {
public:
    virtual ~IBlockBuilder() = default;
    virtual size_t MaxLength() const = 0;
    virtual void Add(const NUdf::TUnboxedValue& value) = 0;
    virtual NUdf::TUnboxedValuePod Build(TComputationContext& ctx, bool finish) = 0;
};

std::unique_ptr<IBlockBuilder> MakeBlockBuilder(TType* type, arrow::MemoryPool& pool);

}
}
