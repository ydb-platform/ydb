#include "accessor.h"
#include "constructor.h"
#include "serializer.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NAccessor::NCompactKV {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const {
    if (externalInfo.GetColumnType()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("COMPACT_KV accessor supports only binary (JsonDocument) columns");
    }
    // Flatten the source into a single binary array.
    auto chunked = originalArray->GetChunkedArray();
    auto schema = std::make_shared<arrow::Schema>(
        arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto table = arrow::Table::Make(schema, { chunked }, originalArray->GetRecordsCount());
    auto batch = NArrow::ToBatch(table);
    return std::make_shared<TCompactKVArray>(batch->column(0));
}

TString TConstructor::DoSerializeToString(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData->GetType() == IChunkedArray::EType::CompactKVArray)("type", columnData->GetType());
    const auto* arr = static_cast<const TCompactKVArray*>(columnData.get());
    return TSerializer::SerializeArray(arr->GetArray(), Settings, externalInfo.GetDefaultSerializer());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    if (!TSerializer::IsCompactKV(originalData)) {
        // Data was serialized with the default (Arrow IPC) serializer — fall back to plain deserialization.
        NPlain::TConstructor plain;
        return plain.DeserializeFromString(originalData, externalInfo);
    }
    try {
        auto array = TSerializer::DeserializeArray(originalData, externalInfo.GetRecordsCount(), externalInfo.GetDefaultSerializer());
        return std::make_shared<TCompactKVArray>(array);
    } catch (const std::exception& e) {
        return TConclusionStatus::Fail(TStringBuilder() << "COMPACT_KV deserialization error: " << e.what());
    }
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<TCompactKVArray>(
        NArrow::TThreadSimpleArraysCache::Get(externalInfo.GetColumnType(), externalInfo.GetDefaultValue(), externalInfo.GetRecordsCount()));
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    *result.MutableCompactKV() = Settings.SerializeToProto();
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) {
    return Settings.DeserializeFromProto(proto.GetCompactKV());
}

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
