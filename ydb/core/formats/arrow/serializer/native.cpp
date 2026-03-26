#include "native.h"
#include "parsing.h"
#include "stream.h"

#include <ydb/core/formats/arrow/dictionary/conversion.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/services/services.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/writer.h>

namespace NKikimr::NArrow::NSerialization {

arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> TNativeSerializer::DoDeserialize(const TString& data) const {
    arrow20::ipc::DictionaryMemo dictMemo;
    auto options = arrow20::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;
    options.memory_pool = Options.memory_pool;

    std::shared_ptr<arrow20::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow20::io::BufferReader readerStream(buffer);
    auto reader = TStatusValidator::GetValid(arrow20::ipc::RecordBatchStreamReader::Open(&readerStream, options));

    std::shared_ptr<arrow20::RecordBatch> batch;
    auto readResult = reader->ReadNext(&batch);
    if (!readResult.ok()) {
        return readResult;
    }
    if (!batch) {
        return arrow20::Status(arrow20::StatusCode::SerializationError, "null batch");
    }
    auto validation = batch->Validate();
    if (!validation.ok()) {
        return arrow20::Status(arrow20::StatusCode::SerializationError, "validation error: " + validation.ToString());
    }
    return batch;
}

TString TNativeSerializer::DoSerializeFull(const std::shared_ptr<arrow20::RecordBatch>& batch) const {
    TString result;
    {
        arrow20::io::MockOutputStream mock;
        auto writer = TStatusValidator::GetValid(arrow20::ipc::MakeStreamWriter(&mock, batch->schema(), Options));
        TStatusValidator::Validate(writer->WriteRecordBatch(*batch));
        result.reserve(mock.GetExtentBytesWritten());
    }
    {
        TStringOutputStream stream(&result);
        auto writer = TStatusValidator::GetValid(arrow20::ipc::MakeStreamWriter(&stream, batch->schema(), Options));
        TStatusValidator::Validate(writer->WriteRecordBatch(*batch));
        TStatusValidator::Validate(writer->Close());
        Y_ABORT_UNLESS(stream.GetPosition() == result.size());
    }
    return result;
}

arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> TNativeSerializer::DoDeserialize(
    const TString& data, const std::shared_ptr<arrow20::Schema>& schema) const {
    arrow20::ipc::DictionaryMemo dictMemo;
    auto options = arrow20::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;
    options.memory_pool = Options.memory_pool;

    std::shared_ptr<arrow20::Buffer> buffer(std::make_shared<TBufferOverString>(data));
    arrow20::io::BufferReader reader(buffer);
    AFL_TRACE(NKikimrServices::ARROW_HELPER)("event", "parsing")("size", data.size())("columns", schema->num_fields());
    auto batchResult = arrow20::ipc::ReadRecordBatch(schema, &dictMemo, options, &reader);
    if (!batchResult.ok()) {
        return batchResult;
    }
    std::shared_ptr<arrow20::RecordBatch> batch = *batchResult;
    if (!batch) {
        return arrow20::Status(arrow20::StatusCode::SerializationError, "empty batch");
    }
    auto validation = batch->Validate();
    if (!validation.ok()) {
        return arrow20::Status(arrow20::StatusCode::SerializationError, "batch is not valid: " + validation.ToString());
    }
    return batch;
}

TString TNativeSerializer::DoSerializePayload(const std::shared_ptr<arrow20::RecordBatch>& batch) const {
    arrow20::ipc::IpcPayload payload;
    // Build payload. Compression if set up performed here.
    TStatusValidator::Validate(arrow20::ipc::GetRecordBatchPayload(*batch, Options, &payload));
#ifndef NDEBUG
    TStatusValidator::Validate(batch->ValidateFull());
#endif

    int32_t metadata_length = 0;
    arrow20::io::MockOutputStream mock;
    // Process prepared payload through mock stream. Fast and efficient.
    TStatusValidator::Validate(arrow20::ipc::WriteIpcPayload(payload, Options, &mock, &metadata_length));

    TString str;
    str.resize(mock.GetExtentBytesWritten());

    TFixedStringOutputStream out(&str);
    // Write prepared payload into the resultant string. No extra allocation will be made.
    TStatusValidator::Validate(arrow20::ipc::WriteIpcPayload(payload, Options, &out, &metadata_length));
    Y_ABORT_UNLESS(out.GetPosition() == str.size());
#ifndef NDEBUG
    TStatusValidator::GetValid(Deserialize(str, batch->schema()));
#endif
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "serialize")("size", str.size())("columns", batch->schema()->num_fields());
    return str;
}

NKikimr::TConclusion<std::shared_ptr<arrow20::util::Codec>> TNativeSerializer::BuildCodec(
    const arrow20::Compression::type& cType, const std::optional<ui32> level) const {
    auto codec = NArrow::TStatusValidator::GetValid(arrow20::util::Codec::Create(cType));
    if (!codec) {
        return std::shared_ptr<arrow20::util::Codec>();
    }
    const int levelDef = level.value_or(codec->default_compression_level());
    const int levelMin = codec->minimum_compression_level();
    const int levelMax = codec->maximum_compression_level();
    if (levelDef < levelMin || levelMax < levelDef) {
        return TConclusionStatus::Fail(TStringBuilder() << "incorrect level for codec `" << arrow20::util::Codec::GetCodecAsString(cType)
                                                        << "`. have to be: [" << levelMin << ":" << levelMax << "]");
    }
    std::shared_ptr<arrow20::util::Codec> codecPtr = std::move(NArrow::TStatusValidator::GetValid(arrow20::util::Codec::Create(cType, levelDef)));
    return codecPtr;
}

NKikimr::TConclusionStatus TNativeSerializer::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    std::optional<arrow20::Compression::type> codec;
    std::optional<int> level;
    {
        auto fValue = features.Extract("COMPRESSION.TYPE");
        if (!fValue) {
            return TConclusionStatus::Fail("not defined COMPRESSION.TYPE as arrow20::Compression");
        }
        codec = NArrow::CompressionFromString(*fValue);
        if (!codec) {
            return TConclusionStatus::Fail("cannot parse COMPRESSION.TYPE as arrow20::Compression");
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
    std::optional<arrow20::Compression::type> codec = NArrow::CompressionFromProto(compression.GetCodec());
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
    if (Options.codec) {
        proto.MutableArrowCompression()->SetCodec(NArrow::CompressionToProto(Options.codec->compression_type()));
        if (arrow20::util::Codec::SupportsCompressionLevel(Options.codec->compression_type())) {
            proto.MutableArrowCompression()->SetLevel(Options.codec->compression_level());
        }
    } else {
        proto.MutableArrowCompression()->SetCodec(NArrow::CompressionToProto(arrow20::Compression::UNCOMPRESSED));
    }
}

}   // namespace NKikimr::NArrow::NSerialization
