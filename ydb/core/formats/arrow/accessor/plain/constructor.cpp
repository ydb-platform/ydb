#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NAccessor::NPlain {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto result = externalInfo.GetDefaultSerializer()->Deserialize(originalData, schema);
    if (!result.ok()) {
        return TConclusionStatus::Fail(result.status().ToString());
    }
    auto rb = TStatusValidator::GetValid(result);
    AFL_VERIFY(rb->num_columns() == 1)("count", rb->num_columns())("schema", schema->ToString());
    if (externalInfo.HasNullRecordsCount()) {
        rb->column(0)->data()->SetNullCount(externalInfo.GetNullRecordsCountVerified());
    }
    return std::make_shared<NArrow::NAccessor::TTrivialArray>(rb->column(0));
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<NArrow::NAccessor::TTrivialArray>(
        NArrow::TThreadSimpleArraysCache::Get(externalInfo.GetColumnType(), externalInfo.GetDefaultValue(), externalInfo.GetRecordsCount()));
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    return NKikimrArrowAccessorProto::TConstructor();
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    std::shared_ptr<arrow::RecordBatch> rb;
    if (columnData->GetType() == IChunkedArray::EType::Array) {
        const auto* arr = static_cast<const TTrivialArray*>(columnData.get());
        rb = arrow::RecordBatch::Make(schema, columnData->GetRecordsCount(), { arr->GetArray() });
    } else {
        auto chunked = columnData->GetChunkedArray();
        auto table = arrow::Table::Make(schema, { chunked }, columnData->GetRecordsCount());
        rb = NArrow::ToBatch(table);
    }
    return externalInfo.GetDefaultSerializer()->SerializePayload(rb);
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto chunked = originalArray->GetChunkedArray();
    auto table = arrow::Table::Make(schema, { chunked }, originalArray->GetRecordsCount());
    return std::make_shared<TTrivialArray>(NArrow::ToBatch(table)->column(0));
}

}   // namespace NKikimr::NArrow::NAccessor::NPlain
