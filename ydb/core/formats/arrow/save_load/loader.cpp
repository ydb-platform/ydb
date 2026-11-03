#include "loader.h"
#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <ydb/core/formats/arrow/serializer/native.h>

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

TChunkConstructionData TColumnLoader::BuildAccessorContext(const ui32 recordsCount, const std::optional<ui32>& notNullCount,
    std::shared_ptr<IAdditionalAccessorData> additionalAccessorData) const {
    return TChunkConstructionData(recordsCount, DefaultValue, ResultField->type(), Serializer.GetObjectPtr(), notNullCount, std::move(additionalAccessorData));
}

TConclusion<std::shared_ptr<IChunkedArray>> TColumnLoader::ApplyConclusion(
    const TString& dataStr, const ui32 recordsCount, const std::optional<ui32>& notNullCount,
    std::shared_ptr<IAdditionalAccessorData> additionalAccessorData) const {
    return BuildAccessor(dataStr, BuildAccessorContext(recordsCount, notNullCount, std::move(additionalAccessorData)));
}

std::shared_ptr<IChunkedArray> TColumnLoader::ApplyVerified(
    const TString& dataStr, const ui32 recordsCount, const std::optional<ui32>& notNullCount,
    std::shared_ptr<IAdditionalAccessorData> additionalAccessorData) const {
    return BuildAccessor(dataStr, BuildAccessorContext(recordsCount, notNullCount, std::move(additionalAccessorData))).DetachResult();
}

TConclusion<std::shared_ptr<IChunkedArray>> TColumnLoader::BuildAccessor(
    const TString& originalData, const TChunkConstructionData& chunkData) const {
    auto collector = NScanStats::CurrentCollector();
    if (!collector) {
        return AccessorConstructor->DeserializeFromString(originalData, chunkData);
    }
    auto operation = collector->Start(NScanStats::TStep::Decode, "DECODE", "Deserialize " + TString(ResultField->name()), NScanStats::Current());
    NScanStats::TScope scope(operation);
    if (operation) {
        operation->Update([&](auto& record) {
            record.SetReadBytes(0);
            auto* details = record.MutableDetails()->MutableDecode();
            details->SetEncodedInputBytes(originalData.size());
            details->SetColumn(ResultField->name().data(), ResultField->name().size());
            if (AccessorConstructor->GetType() == IChunkedArray::EType::Array &&
                dynamic_cast<const NSerialization::TNativeSerializer*>(Serializer.GetObjectPtr().get())) {
                if (auto codec = NSerialization::TNativeSerializer::TryGetReadCodec(originalData)) {
                    details->SetCodec(*codec);
                }
            }
        });
    }
    auto result = AccessorConstructor->DeserializeFromString(originalData, chunkData);
    if (operation) {
        if (result.IsSuccess()) {
            operation->Update([&](auto& record) {
                auto* details = record.MutableDetails()->MutableDecode();
                details->SetDecodedRows(result.GetResult()->GetRecordsCount());
                if (const auto size = result.GetResult()->GetRawSize()) {
                    details->SetDecodedOutputBytes(*size);
                }
            });
        }
        operation->Finish(result.IsSuccess() ? NScanStats::TStep::completed : NScanStats::TStep::failed);
    }
    return result;
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
