#pragma once
#include <ydb/library/yql/minikql/mkql_node.h>
#include <arrow/type_fwd.h>

namespace NYql::NUdf {
    class TBlockItem;
}

namespace NYql {

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool);
arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool);

using TColumnConverter = std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::Array>&)>;
TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType);

} // NYql

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregatorFactory;
void RegisterPgBlockAggs(THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>>& registry);

}
}
