#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

std::vector<TChunkedArraySerialized> TSubColumnsArray::DoSplitBySizes(
    const TColumnLoader& loader, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
    std::vector<TChunkedArraySerialized> result;
    auto table = Records->BuildTableVerified();
    auto rb = NArrow::ToBatch(table);
    AFL_VERIFY(GetRecordsCount());

    ui32 idxCurrent = 0;
    for (ui32 i = 0; i < splitSizes.size(); ++i) {
        const ui32 recordsCount = 1.0 * splitSizes[i] / fullSerializedData.size() * GetRecordsCount();
        AFL_VERIFY(recordsCount >= 1);
        auto rbSlice = rb->Slice(idxCurrent, recordsCount);
        auto subColumnsChunk = std::make_shared<TSubColumnsArray>(rbSlice);
        result.emplace_back(subColumnsChunk,
            loader.GetAccessorConstructor()->SerializeToString(subColumnsChunk, loader.BuildAccessorContext(GetRecordsCount())));
    }

    return result;
}

TSubColumnsArray::TSubColumnsArray(const std::shared_ptr<IChunkedArray>& sourceArray, const std::shared_ptr<IDataAdapter>& adapter)
    : TBase(sourceArray->GetRecordsCount(), EType::SubColumnsArray, sourceArray->GetDataType()) {
    AFL_VERIFY(adapter);
    AFL_VERIFY(sourceArray);
    Schema = adapter->BuildSchemaForData(sourceArray).DetachResult();
    auto builders = NArrow::MakeBuilders(Schema, sourceArray->GetRecordsCount());
    IChunkedArray::TReader reader(sourceArray);
    for (ui32 i = 0; i < reader.GetRecordsCount();) {
        auto address = reader.GetReadChunk(i);
        adapter->AddDataToBuilders(address.GetArray(), Schema, builders);
        i += address.GetArray()->length();
        AFL_VERIFY(i <= reader.GetRecordsCount());
    }
    auto arrays = NArrow::Finish(std::move(builders));

    Records = std::make_shared<TGeneralContainer>(
        arrow::RecordBatch::Make(Schema, sourceArray->GetRecordsCount(), NArrow::Finish(std::move(builders))));
    Records->AddField(std::make_shared<arrow::Field>("__ORIGINAL", arrow::utf8()), sourceArray).Validate();
}

TSubColumnsArray::TSubColumnsArray(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<TString>& columns, const TChunkConstructionData& externalInfo)
    : TBase(externalInfo.GetRecordsCount(), EType::SubColumnsArray, externalInfo.GetColumnType())
    , Schema(schema) {
    AFL_VERIFY(schema);
    AFL_VERIFY((ui32)schema->num_fields() == columns.size())("schema", schema->ToString())("columns", columns.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<IChunkedArray>> arrays;
    for (ui32 i = 0; i < (ui32)schema->num_fields(); ++i) {
        fields.emplace_back(schema->field(i));
        auto loader = std::make_shared<TColumnLoader>(externalInfo.GetDefaultSerializer(), std::make_shared<NPlain::TConstructor>(),
            std::make_shared<arrow::Field>("__ORIGINAL", externalInfo.GetColumnType()), externalInfo.GetDefaultValue(), 0);
        std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(externalInfo.GetRecordsCount(), columns[i]) };
        arrays.emplace_back(std::make_shared<TDeserializeChunkedArray>(externalInfo.GetRecordsCount(), loader, std::move(chunks)));
    }

    Records = std::make_shared<TGeneralContainer>(std::move(fields), std::move(arrays));
}

TSubColumnsArray::TSubColumnsArray(const std::shared_ptr<arrow::RecordBatch>& batch)
    : TBase(batch->num_rows(), EType::SubColumnsArray, batch->schema()->field(batch->schema()->num_fields() - 1)->type())
    , Schema(batch->schema()) {
    AFL_VERIFY(batch);
    AFL_VERIFY(batch->schema()->field(batch->schema()->num_fields() - 1)->name() == "__ORIGINAL")("schema", batch->schema()->ToString());
    for (ui32 i = 0; i + 1 < (ui32)batch->schema()->num_fields(); ++i) {
        AFL_VERIFY(batch->schema()->field(i)->name() < batch->schema()->field(i + 1)->name())("schema", batch->schema()->ToString());
    }
    Records = std::make_shared<TGeneralContainer>(batch);
}

TSubColumnsArray::TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount)
    : TBase(recordsCount, EType::SubColumnsArray, type) {
    Schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("__ORIGINAL", type) }));
    Records = std::make_shared<TGeneralContainer>(arrow::RecordBatch::Make(
        Schema, recordsCount, std::vector<std::shared_ptr<arrow::Array>>({ NArrow::TThreadSimpleArraysCache::GetNull(type, recordsCount) })));
}

TString TSubColumnsArray::SerializeToString(const TChunkConstructionData& externalInfo) const {
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    *proto.MutableSchema()->MutableDescription() = NArrow::SerializeSchema(*Schema);
    AFL_VERIFY((ui32)Schema->num_fields() == Records->num_columns());
    for (auto&& i : Schema->fields()) {
        auto rb = NArrow::ToBatch(Records->BuildTableVerified(TGeneralContainer::TTableConstructionContext({ i->name() })));
        *proto.AddColumns()->MutableDescription() = externalInfo.GetDefaultSerializer()->SerializePayload(rb);
    }
    return proto.SerializeAsString();
}

}   // namespace NKikimr::NArrow::NAccessor
