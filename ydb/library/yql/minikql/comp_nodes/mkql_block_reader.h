#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <arrow/datum.h>

namespace NKikimr {
namespace NMiniKQL {

class IBlockReader {
public:
    virtual ~IBlockReader() = default;
    virtual void Reset(const arrow::Datum& datum) = 0;
    // for scalars will continuously return same value
    virtual TMaybe<NUdf::TUnboxedValuePod> GetNextValue() = 0;
};

std::unique_ptr<IBlockReader> MakeBlockReader(TType* type, const THolderFactory& holderFactory);

}
}
