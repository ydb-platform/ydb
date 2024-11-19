#include "accessor.h"
#include "constructor.h"

namespace NKikimr::NArrow::NAccessor::NSparsed {

std::shared_ptr<arrow::Schema> TConstructor::DoGetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const {
    arrow::FieldVector fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
        std::make_shared<arrow::Field>("value", resultColumn->type()) };
    return std::make_shared<arrow::Schema>(fields);
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<TSparsedArray>(externalInfo.GetDefaultValue(), externalInfo.GetColumnType(), externalInfo.GetRecordsCount());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<arrow::RecordBatch>& originalData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(originalData->num_columns() == 2)("count", originalData->num_columns())("schema", originalData->schema()->ToString());
    NArrow::NAccessor::TSparsedArray::TBuilder builder(externalInfo.GetDefaultValue(), externalInfo.GetColumnType());
    builder.AddChunk(externalInfo.GetRecordsCount(), originalData);
    return builder.Finish();
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    *result.MutableSparsed() = {};
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

}   // namespace NKikimr::NArrow::NAccessor::NSparsed
