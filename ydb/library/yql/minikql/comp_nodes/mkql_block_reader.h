#pragma once

#include "mkql_block_item.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>

#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::IBlockReader;

class IBlockItemConverter {
public:
    virtual ~IBlockItemConverter() = default;

    virtual NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const = 0;
};

using NYql::NUdf::MakeBlockReader;

std::unique_ptr<IBlockItemConverter> MakeBlockItemConverter(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type);

}
