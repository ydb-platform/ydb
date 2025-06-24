#include "loader.h"

#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor {

TString TColumnLoader::DebugString() const {
    TStringBuilder result;
    result << "accessor_constructor:" << AccessorConstructor->DebugString() << ";";
    result << "result_field:" << ResultField->ToString() << ";";
    result << "serializer:" << Serializer->DebugString() << ";";
    return result;
}

TColumnLoader::TColumnLoader(const NSerialization::TSerializerContainer& serializer, const TConstructorContainer& accessorConstructor,
    const std::shared_ptr<arrow::Field>& resultField, const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 columnId)
    : Serializer(serializer)
    , AccessorConstructor(accessorConstructor)
    , ResultField(resultField)
    , DefaultValue(defaultValue)
    , ColumnId(columnId) {
    AFL_VERIFY(!!AccessorConstructor);
    AFL_VERIFY(ResultField);
    AFL_VERIFY(Serializer);
}

const std::shared_ptr<arrow::Field>& TColumnLoader::GetField() const {
    return ResultField;
}

TChunkConstructionData TColumnLoader::BuildAccessorContext(const ui32 recordsCount, const std::optional<ui32>& notNullCount) const {
    return TChunkConstructionData(recordsCount, DefaultValue, ResultField->type(), Serializer.GetObjectPtr(), notNullCount);
}

TConclusion<std::shared_ptr<IChunkedArray>> TColumnLoader::ApplyConclusion(
    const TString& dataStr, const ui32 recordsCount, const std::optional<ui32>& notNullCount) const {
    return BuildAccessor(dataStr, BuildAccessorContext(recordsCount, notNullCount));
}

std::shared_ptr<IChunkedArray> TColumnLoader::ApplyVerified(
    const TString& dataStr, const ui32 recordsCount, const std::optional<ui32>& notNullCount) const {
    return BuildAccessor(dataStr, BuildAccessorContext(recordsCount, notNullCount)).DetachResult();
}

TConclusion<std::shared_ptr<IChunkedArray>> TColumnLoader::BuildAccessor(
    const TString& originalData, const TChunkConstructionData& chunkData) const {
    return AccessorConstructor->DeserializeFromString(originalData, chunkData);
}

std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray> TColumnLoader::BuildDefaultAccessor(const ui32 recordsCount) const {
    return AccessorConstructor
        ->ConstructDefault(TChunkConstructionData(recordsCount, DefaultValue, ResultField->type(), Serializer.GetObjectPtr()))
        .DetachResult();
}

bool TColumnLoader::IsEqualTo(const TColumnLoader& item) const {
    if (!Serializer.IsEqualTo(item.Serializer)) {
        return false;
    }
    if (!AccessorConstructor.IsEqualTo(item.AccessorConstructor)) {
        return false;
    }
    return true;
}

std::optional<NSplitter::TSimpleSerializationStat> TColumnLoader::TryBuildColumnStat() const {
    std::optional<NSplitter::TSimpleSerializationStat> result;
    SwitchType(ResultField->type()->id(), [&](const auto switcher) {
        if constexpr (switcher.IsCType) {
            using CType = typename decltype(switcher)::ValueType;
            result = NSplitter::TSimpleSerializationStat(std::max<ui32>(1, sizeof(CType) / 2), 1, sizeof(CType));
            result->SetOrigination(NSplitter::TSimpleSerializationStat::EOrigination::Loader);
        }
        return true;
    });
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor
