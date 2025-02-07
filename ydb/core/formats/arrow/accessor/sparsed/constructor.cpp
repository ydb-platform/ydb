#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NArrow::NAccessor::NSparsed {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<TSparsedArray>(externalInfo.GetDefaultValue(), externalInfo.GetColumnType(), externalInfo.GetRecordsCount());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    arrow::FieldVector fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
        std::make_shared<arrow::Field>("value", externalInfo.GetColumnType()) };
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto rbParsed = externalInfo.GetDefaultSerializer()->Deserialize(originalData, schema);
    if (!rbParsed.ok()) {
        return TConclusionStatus::Fail(rbParsed.status().ToString());
    }
    auto rb = *rbParsed;
    AFL_VERIFY(rb->num_columns() == 2)("count", rb->num_columns())("schema", rb->schema()->ToString());
    NArrow::NAccessor::TSparsedArray::TBuilder builder(externalInfo.GetDefaultValue(), externalInfo.GetColumnType());
    builder.AddChunk(externalInfo.GetRecordsCount(), rb);
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

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    std::shared_ptr<TSparsedArray> sparsed = std::static_pointer_cast<TSparsedArray>(columnData);
    return externalInfo.GetDefaultSerializer()->SerializePayload(sparsed.GetRecordBatchVerified());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(originalArray);
    return std::make_shared<TSparsedArray>(*originalArray, externalInfo.GetDefaultValue());
}

}   // namespace NKikimr::NArrow::NAccessor::NSparsed
