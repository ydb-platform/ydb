#pragma once

#include "mkql_block_item.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

class IBlockReader : private TNonCopyable {
public:
    virtual ~IBlockReader() = default;
    // result will reference to Array/Scalar internals and will be valid until next call to GetItem/GetScalarItem
    virtual TBlockItem GetItem(const arrow::ArrayData& data, size_t index) = 0;
    virtual TBlockItem GetScalarItem(const arrow::Scalar& scalar) = 0;

    virtual NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const = 0;
};

std::unique_ptr<IBlockReader> MakeBlockReader(TType* type);

}
