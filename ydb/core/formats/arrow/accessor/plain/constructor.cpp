#include "accessor.h"
#include "constructor.h"

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NAccessor::NPlain {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<arrow::RecordBatch>& originalData, const TChunkConstructionData& /*externalInfo*/) const {
    AFL_VERIFY(originalData->num_columns() == 1)("count", originalData->num_columns())("schema", originalData->schema()->ToString());
    return std::make_shared<NArrow::NAccessor::TTrivialArray>(originalData->column(0));
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

std::shared_ptr<arrow::Schema> TConstructor::DoGetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const {
    return std::make_shared<arrow::Schema>(arrow::FieldVector({ resultColumn }));
}

std::shared_ptr<arrow::RecordBatch> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    if (columnData->GetType() == IChunkedArray::EType::Array) {
        const auto* arr = static_cast<const TTrivialArray*>(columnData.get());
        return arrow::RecordBatch::Make(schema, columnData->GetRecordsCount(), { arr->GetArray() });
    } else {
        auto chunked = columnData->GetChunkedArray();
        auto table = arrow::Table::Make(schema, { chunked }, columnData->GetRecordsCount());
        return NArrow::ToBatch(table, chunked->num_chunks() > 1);
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NPlain
