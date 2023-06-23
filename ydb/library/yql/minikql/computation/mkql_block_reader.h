#pragma once

#include "mkql_block_item.h"
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>

#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::IBlockReader;
using NYql::NUdf::TOutputBuffer;

class IBlockItemConverter {
public:
    virtual ~IBlockItemConverter() = default;

    virtual NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const = 0;
    virtual TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const = 0;
};

using NYql::NUdf::MakeBlockReader;
using NYql::NUdf::TBlockItemSerializeProps;
using NYql::NUdf::UpdateBlockItemSerializeProps;

std::unique_ptr<IBlockItemConverter> MakeBlockItemConverter(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type,
    const NYql::NUdf::IPgBuilder& pgBuilder);

}
