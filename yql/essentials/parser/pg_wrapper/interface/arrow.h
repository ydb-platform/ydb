#pragma once
#include <yql/essentials/providers/common/codec/yt_arrow_converter_interface/yt_arrow_converter.h>
#include <yql/essentials/providers/common/codec/yt_arrow_converter_interface/yt_arrow_converter_details.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/arrow/block_item.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>

#include <arrow/datum.h>

namespace NYql {

arrow20::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow20::MemoryPool& pool);
arrow20::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow20::MemoryPool& pool);

using TColumnConverter = std::function<std::shared_ptr<arrow20::Array>(const std::shared_ptr<arrow20::Array>&)>;
TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow20::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType);

std::unique_ptr<IYsonComplexTypeReader> BuildPgYsonColumnReader(const NUdf::TPgTypeDescription& desc);
std::unique_ptr<IYtColumnConverter> BuildPgTopLevelColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder, const NKikimr::NMiniKQL::TPgType* targetType);
} // namespace NYql

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregatorFactory;
void RegisterPgBlockAggs(THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>>& registry);

} // namespace NMiniKQL
} // namespace NKikimr
