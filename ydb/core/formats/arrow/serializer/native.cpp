#include "native.h"
#include "stream.h"
#include "parsing.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/formats/arrow/common/validation.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

namespace NKikimr::NArrow::NSerialization {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TNativeSerializer::DoDeserialize(const TString& data) const {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;

    std::shared_ptr<arrow::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow::io::BufferReader readerStream(buffer);
    auto reader = TStatusValidator::GetValid(arrow::ipc::RecordBatchStreamReader::Open(&readerStream, options));

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

TString TNativeSerializer::DoSerializeFull(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    TString result;
    {
        arrow::io::MockOutputStream mock;
        auto writer = TStatusValidator::GetValid(arrow::ipc::MakeStreamWriter(&mock, batch->schema(), Options));
        TStatusValidator::Validate(writer->WriteRecordBatch(*batch));
        result.reserve(mock.GetExtentBytesWritten());
    }
    {
        TStringOutputStream stream(&result);
        auto writer = TStatusValidator::GetValid(arrow::ipc::MakeStreamWriter(&stream, batch->schema(), Options));
        TStatusValidator::Validate(writer->WriteRecordBatch(*batch));
        TStatusValidator::Validate(writer->Close());
        Y_ABORT_UNLESS(stream.GetPosition() == result.size());
    }
    return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TNativeSerializer::DoDeserialize(const TString& data, const std::shared_ptr<arrow::Schema>& schema) const {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;

    std::shared_ptr<arrow::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow::io::BufferReader reader(buffer);
    AFL_TRACE(NKikimrServices::ARROW_HELPER)("event", "parsing")("size", data.size())("columns", schema->num_fields());
    auto batchResult = arrow::ipc::ReadRecordBatch(schema, &dictMemo, options, &reader);
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

TString TNativeSerializer::DoSerializePayload(const std::shared_ptr<arrow::RecordBatch>& batch) const {
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
    Y_ABORT_UNLESS(out.GetPosition() == str.size());
    Y_DEBUG_ABORT_UNLESS(Deserialize(str, batch->schema()).ok());
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "serialize")("size", str.size())("columns", batch->schema()->num_fields());
    return str;
}

NKikimr::TConclusion<std::shared_ptr<arrow::util::Codec>> TNativeSerializer::BuildCodec(const arrow::Compression::type& cType, const std::optional<ui32> level) const {
    auto codec = NArrow::TStatusValidator::GetValid(arrow::util::Codec::Create(cType));
    if (!codec) {
        return std::shared_ptr<arrow::util::Codec>();
    }
    const int levelDef = level.value_or(codec->default_compression_level());
    const int levelMin = codec->minimum_compression_level();
    const int levelMax = codec->maximum_compression_level();
    if (levelDef < levelMin || levelMax < levelDef) {
        return TConclusionStatus::Fail(
            TStringBuilder() << "incorrect level for codec. have to be: [" << levelMin << ":" << levelMax << "]"
        );
    }
    std::shared_ptr<arrow::util::Codec> codecPtr = std::move(NArrow::TStatusValidator::GetValid(arrow::util::Codec::Create(cType, levelDef)));
    return codecPtr;
}

NKikimr::TConclusionStatus TNativeSerializer::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    std::optional<arrow::Compression::type> codec;
    std::optional<int> level;
    {
        auto fValue = features.Extract("COMPRESSION.TYPE");
        if (!fValue) {
            return TConclusionStatus::Fail("not defined COMPRESSION.TYPE as arrow::Compression");
        }
        codec = NArrow::CompressionFromString(*fValue);
        if (!codec) {
            return TConclusionStatus::Fail("cannot parse COMPRESSION.TYPE as arrow::Compression");
        }
    }
    {
        auto fValue = features.Extract("COMPRESSION.LEVEL");
        if (fValue) {
            ui32 levelLocal;
            if (!TryFromString<ui32>(*fValue, levelLocal)) {
                return TConclusionStatus::Fail("cannot parse COMPRESSION.LEVEL as ui32");
            }
            level = levelLocal;
        }
    }
    auto codecPtrStatus = BuildCodec(codec.value_or(Options.codec->compression_type()), level);
    if (!codecPtrStatus) {
        return codecPtrStatus.GetError();
    }
    Options.codec = *codecPtrStatus;
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TNativeSerializer::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) {
    if (!proto.HasArrowCompression()) {
        return TConclusionStatus::Fail("no arrow serializer data in proto");
    }
    auto compression = proto.GetArrowCompression();
    if (!compression.HasCodec()) {
        Options = GetDefaultOptions();
        return TConclusionStatus::Success();
    }
    std::optional<arrow::Compression::type> codec = NArrow::CompressionFromProto(compression.GetCodec());
    if (!codec) {
        return TConclusionStatus::Fail("cannot parse codec type from proto");
    }
    std::optional<int> level;
    if (compression.HasLevel()) {
        level = compression.GetLevel();
    }

    Options.use_threads = false;
    auto result = BuildCodec(*codec, level);
    if (!result) {
        return result.GetError();
    }
    Options.codec = *result;
    return TConclusionStatus::Success();
}

void TNativeSerializer::DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const {
    proto.MutableArrowCompression()->SetCodec(NArrow::CompressionToProto(Options.codec->compression_type()));
    proto.MutableArrowCompression()->SetLevel(Options.codec->compression_level());
}

}
