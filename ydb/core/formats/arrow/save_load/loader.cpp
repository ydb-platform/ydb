#include "loader.h"

#include <ydb/core/formats/arrow/common/validation.h>

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

std::shared_ptr<IChunkedArray> TColumnLoader::ApplyVerified(const TString& dataStr, const ui32 recordsCount) const {
    auto data = TStatusValidator::GetValid(Apply(dataStr));
    return BuildAccessor(data, TChunkConstructionData(recordsCount, DefaultValue, ResultField->type()));
}

std::shared_ptr<IChunkedArray> TColumnLoader::BuildAccessor(
    const std::shared_ptr<arrow::RecordBatch>& batch, const TChunkConstructionData& chunkData) const {
    return AccessorConstructor->Construct(batch, chunkData).DetachResult();
}

std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray> TColumnLoader::BuildDefaultAccessor(const ui32 recordsCount) const {
    return AccessorConstructor->ConstructDefault(TChunkConstructionData(recordsCount, DefaultValue, ResultField->type())).DetachResult();
}

}   // namespace NKikimr::NArrow::NAccessor
