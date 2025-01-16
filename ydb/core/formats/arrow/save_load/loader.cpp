#include "loader.h"

#include <ydb/library/formats/arrow/common/validation.h>

namespace NKikimr::NArrow::NAccessor {

TString TColumnLoader::DebugString() const {
    TStringBuilder result;
    result << "accessor_constructor:" << AccessorConstructor->DebugString() << ";";
    result << "result_field:" << ResultField->ToString() << ";";
    if (Transformer) {
        result << "transformer:" << Transformer->DebugString() << ";";
    }
    result << "serializer:" << Serializer->DebugString() << ";";
    return result;
}

TColumnLoader::TColumnLoader(NTransformation::ITransformer::TPtr transformer, const NSerialization::TSerializerContainer& serializer,
    const TConstructorContainer& accessorConstructor, const std::shared_ptr<arrow::Field>& resultField,
    const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 columnId)
    : Serializer(serializer)
    , Transformer(transformer)
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TColumnLoader::Apply(const TString& data) const {
    Y_ABORT_UNLESS(Serializer);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> columnArray =
        Transformer ? Serializer->Deserialize(data) : Serializer->Deserialize(data, AccessorConstructor->GetExpectedSchema(ResultField));
    if (!columnArray.ok()) {
        return columnArray;
    }
    if (Transformer) {
        return Transformer->Transform(*columnArray);
    } else {
        return columnArray;
    }
}

std::shared_ptr<arrow::RecordBatch> TColumnLoader::ApplyRawVerified(const TString& data) const {
    return TStatusValidator::GetValid(Apply(data));
}

TChunkConstructionData TColumnLoader::BuildAccessorContext(const ui32 recordsCount) const {
    return TChunkConstructionData(recordsCount, DefaultValue, ResultField->type());
}

TConclusion<std::shared_ptr<IChunkedArray>> TColumnLoader::ApplyConclusion(const TString& dataStr, const ui32 recordsCount) const {
    auto result = Apply(dataStr);
    if (result.ok()) {
        return BuildAccessor(*result, BuildAccessorContext(recordsCount));
    } else {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_parse_blob")("data_size", dataStr.size())(
            "expected_records_count", recordsCount)("problem", result.status().ToString());
        return TConclusionStatus::Fail(result.status().ToString());
    }
}

std::shared_ptr<IChunkedArray> TColumnLoader::ApplyVerified(const TString& dataStr, const ui32 recordsCount) const {
    auto data = TStatusValidator::GetValid(Apply(dataStr));
    return BuildAccessor(data, BuildAccessorContext(recordsCount));
}

std::shared_ptr<IChunkedArray> TColumnLoader::BuildAccessor(
    const std::shared_ptr<arrow::RecordBatch>& batch, const TChunkConstructionData& chunkData) const {
    return AccessorConstructor->Construct(batch, chunkData).DetachResult();
}

std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray> TColumnLoader::BuildDefaultAccessor(const ui32 recordsCount) const {
    return AccessorConstructor->ConstructDefault(TChunkConstructionData(recordsCount, DefaultValue, ResultField->type())).DetachResult();
}

bool TColumnLoader::IsEqualTo(const TColumnLoader& item) const {
    if (!!Transformer != !!item.Transformer) {
        return false;
    } else if (!!Transformer && !Transformer->IsEqualTo(*item.Transformer)) {
        return false;
    }
    if (!Serializer.IsEqualTo(item.Serializer)) {
        return false;
    }
    if (!AccessorConstructor.IsEqualTo(item.AccessorConstructor)) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NArrow::NAccessor
