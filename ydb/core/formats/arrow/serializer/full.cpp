#include "full.h"
#include "stream.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
namespace NKikimr::NArrow::NSerialization {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TFullDataDeserializer::DoDeserialize(const TString& data) const {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;

    std::shared_ptr<arrow::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow::io::BufferReader readerStream(buffer);
    auto reader = TStatusValidator::GetValid(arrow::ipc::RecordBatchStreamReader::Open(&readerStream));

    std::shared_ptr<arrow::RecordBatch> batch;
    auto readResult = reader->ReadNext(&batch);
    if (!readResult.ok()) {
        return readResult;
    }
    if (!batch) {
        return arrow::Status(arrow::StatusCode::SerializationError, "null batch");
    }
    auto validation = batch->Validate();
    if (!validation.ok()) {
        return arrow::Status(arrow::StatusCode::SerializationError, "validation error: " + validation.ToString());
    }
    return batch;
}

TString TFullDataSerializer::DoSerialize(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    TString result;
    result.reserve(64u << 10);
    {
        TStringOutputStream stream(&result);
        auto writer = TStatusValidator::GetValid(arrow::ipc::MakeStreamWriter(&stream, batch->schema(), Options));
        TStatusValidator::Validate(writer->WriteRecordBatch(*batch));
        TStatusValidator::Validate(writer->Close());
        Y_VERIFY(stream.GetPosition() == result.size());
    }
    return result;
}

}
