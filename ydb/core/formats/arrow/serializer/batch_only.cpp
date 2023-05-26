#include "batch_only.h"
#include "stream.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/protos/services.pb.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <library/cpp/actors/core/log.h>
namespace NKikimr::NArrow::NSerialization {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TBatchPayloadDeserializer::DoDeserialize(const TString& data) const {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;

    std::shared_ptr<arrow::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow::io::BufferReader reader(buffer);
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "parsing")("size", data.size())("columns", Schema->num_fields());
    auto batchResult = arrow::ipc::ReadRecordBatch(Schema, &dictMemo, options, &reader);
    if (!batchResult.ok()) {
        return batchResult;
    }
    std::shared_ptr<arrow::RecordBatch> batch = *batchResult;
    if (!batch) {
        return arrow::Status(arrow::StatusCode::SerializationError, "empty batch");
    }
    auto validation = batch->Validate();
    if (!validation.ok()) {
        return arrow::Status(arrow::StatusCode::SerializationError, "batch is not valid: " + validation.ToString());
    }
    return batch;
}

TString TBatchPayloadSerializer::DoSerialize(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    arrow::ipc::IpcPayload payload;
    // Build payload. Compression if set up performed here.
    TStatusValidator::Validate(arrow::ipc::GetRecordBatchPayload(*batch, Options, &payload));

    int32_t metadata_length = 0;
    arrow::io::MockOutputStream mock;
    // Process prepared payload through mock stream. Fast and efficient.
    TStatusValidator::Validate(arrow::ipc::WriteIpcPayload(payload, Options, &mock, &metadata_length));

    TString str;
    str.resize(mock.GetExtentBytesWritten());

    TFixedStringOutputStream out(&str);
    // Write prepared payload into the resultant string. No extra allocation will be made.
    TStatusValidator::Validate(arrow::ipc::WriteIpcPayload(payload, Options, &out, &metadata_length));
    Y_VERIFY(out.GetPosition() == str.size());
    Y_VERIFY_DEBUG(TBatchPayloadDeserializer(batch->schema()).Deserialize(str).ok());
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "serialize")("size", str.size())("columns", batch->schema()->num_fields());
    return str;
}

}
