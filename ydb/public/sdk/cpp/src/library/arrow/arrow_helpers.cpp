#include "arrow_helpers.h"
#include "serializer/stream.h"

#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

namespace NYdb {

inline namespace Dev {

namespace NArrow {

arrow::Result<std::shared_ptr<arrow::Schema>> DeserializeSchema(const std::string& str) {
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dictMemo;

    auto schema = ReadSchema(&reader, &dictMemo);
    return schema;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DeserializeBatch(const std::string& blob, const std::shared_ptr<arrow::Schema>& schema) {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();

    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(blob.data()), blob.size());
    arrow::io::BufferReader reader(buffer);

    auto tryBatch = arrow::ipc::ReadRecordBatch(schema, &dictMemo, options, &reader);
    if (!tryBatch.ok()) {
        return arrow::Status(arrow::StatusCode::SerializationError, "null batch");
    }

    std::shared_ptr<arrow::RecordBatch> batch = *tryBatch;

    if (!batch) {
        return arrow::Status(arrow::StatusCode::SerializationError, "null batch");
    }

    auto validation = batch->Validate();
    if (!validation.ok()) {
        return arrow::Status(arrow::StatusCode::SerializationError, "batch validation error: " + validation.ToString());
    }

    return batch;
}

std::string SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options) {
    arrow::ipc::IpcPayload payload;
    NKikimr::NArrow::TStatusValidator::Validate(arrow::ipc::GetRecordBatchPayload(*batch, options, &payload));

    int32_t metadata_length = 0;
    arrow::io::MockOutputStream mock;
    NKikimr::NArrow::TStatusValidator::Validate(arrow::ipc::WriteIpcPayload(payload, options, &mock, &metadata_length));

    std::string str;
    str.resize(mock.GetExtentBytesWritten());

    NSerialization::TFixedStringOutputStream out(&str);
    NKikimr::NArrow::TStatusValidator::Validate(arrow::ipc::WriteIpcPayload(payload, options, &out, &metadata_length));

    return str;
}

arrow::Result<std::string> CombineSerializedBatches(const std::string& first, const std::string& second, const std::string& serializedSchema) {
    auto trySchema = DeserializeSchema(serializedSchema);
    if (!trySchema.ok()) {
        return trySchema.status();
    }

    std::shared_ptr<arrow::Schema> schema = *trySchema;

    auto tryFirstBatch = DeserializeBatch(first, schema);
    auto trySecondBatch = DeserializeBatch(second, schema);

    if (!tryFirstBatch.ok()) {
        if (trySecondBatch.ok()) {
            return second;
        }

        return tryFirstBatch.status();
    }

    if (!trySecondBatch.ok()) {
        if (tryFirstBatch.ok()) {
            return first;
        }

        return trySecondBatch.status();
    }

    std::shared_ptr<arrow::RecordBatch> firstBatch = *tryFirstBatch;
    std::shared_ptr<arrow::RecordBatch> secondBatch = *trySecondBatch;

    std::shared_ptr<arrow::RecordBatch> result = NKikimr::NArrow::CombineBatches({firstBatch, secondBatch});
    auto serializedResult = SerializeBatch(result, arrow::ipc::IpcWriteOptions::Defaults());
    return serializedResult;
}

}
}
}
