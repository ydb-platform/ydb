#include <ydb/library/yql/providers/ydb_remote/actors/read_stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <arrow/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/metadata_internal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/compression.h>

namespace NYql::NYdbRemote {
namespace {

TSource Source(Ydb::Type::PrimitiveTypeId type = Ydb::Type::UINT64, bool optional = false) {
    TSource source;
    source.SetVersion(1);
    source.SetEndpoint("localhost:2135");
    source.SetDatabase("/Remote");
    source.SetTable("/Remote/table");
    auto* column = source.AddColumns();
    column->SetName("value");
    auto* item = column->MutableType();
    if (optional) {
        item = item->mutable_optional_type()->mutable_item();
    }
    item->set_type_id(type);
    return source;
}

NYdb::TResultSet Result(std::shared_ptr<arrow::Array> values, const std::string& name = "value",
                       arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults()) {
    auto schema = arrow::schema({arrow::field(name, values->type())});
    auto batch = arrow::RecordBatch::Make(schema, values->length(), {values});
    auto schemaBytes = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
    auto bytes = arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
    Ydb::ResultSet proto;
    proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
    proto.mutable_arrow_format_meta()->set_schema(schemaBytes->ToString());
    proto.set_data(bytes->ToString());
    return NYdb::TResultSet(std::move(proto));
}

std::string Framed(const arrow::Buffer& metadata, const std::string& body = {}) {
    const ui32 size = (metadata.size() + 7) & ~ui32(7);
    std::string result(8, '\xff');
    for (ui32 i = 0; i < 4; ++i) {
        result[4 + i] = static_cast<char>(size >> (8 * i));
    }
    result.append(reinterpret_cast<const char*>(metadata.data()), metadata.size());
    result.resize(8 + size, '\0');
    result += body;
    return result;
}

NYdb::TResultSet RawResult(const std::shared_ptr<arrow::Schema>& schema,
                          const std::shared_ptr<arrow::Buffer>& metadata, const std::string& body) {
    Ydb::ResultSet proto;
    proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
    proto.mutable_arrow_format_meta()->set_schema(arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString());
    proto.set_data(Framed(*metadata, body));
    return NYdb::TResultSet(std::move(proto));
}

std::shared_ptr<arrow::Array> Integers() {
    arrow::UInt64Builder builder;
    UNIT_ASSERT(builder.AppendValues({1, 2, 3}).ok());
    return builder.Finish().ValueOrDie();
}

} // namespace

Y_UNIT_TEST_SUITE(YdbRemoteReadStream) {
    Y_UNIT_TEST(EscapesIdentifiersAndRejectsControls) {
        auto source = Source();
        source.SetTable("/Remote/a`b\\c");
        source.MutableColumns(0)->SetName("a`b\\c");
        UNIT_ASSERT_VALUES_EQUAL(BuildReadQuery(source), "SELECT `a\\`b\\\\c` FROM `/Remote/a\\`b\\\\c`;");
        source.SetTable(TString("/Remote/\0bad", 12));
        UNIT_ASSERT_EXCEPTION(ValidateSource(source), yexception);
    }

    Y_UNIT_TEST(RejectsUnsupportedPlanBeforeExecution) {
        auto source = Source();
        source.SetVersion(2);
        UNIT_ASSERT_EXCEPTION(ValidateSource(source), yexception);
        source.SetVersion(1);
        source.MutableColumns(0)->MutableType()->set_type_id(Ydb::Type::JSON);
        UNIT_ASSERT_EXCEPTION(ValidateSource(source), yexception);
        source = Source();
        source.SetMaxBatchBytes(0);
        UNIT_ASSERT_EXCEPTION(ValidateSource(source), yexception);
    }

    Y_UNIT_TEST(DecodesAndOwnsBuffers) {
        auto source = Source();
        auto decoded = DecodeArrowResult(Result(Integers()), source, source.GetMaxBatchBytes());
        UNIT_ASSERT_VALUES_EQUAL(decoded->num_rows(), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::UInt64Array&>(*decoded->column(0)).Value(2), 3);
    }

    Y_UNIT_TEST(DeliveredBuffersDoNotRetainIpcPadding) {
        std::shared_ptr<arrow::Buffer> metadata;
        UNIT_ASSERT(arrow::ipc::internal::WriteRecordBatchMessage(1, 4096, nullptr,
            {{1, 0, 0}}, {{0, 0}, {0, 8}}, arrow::ipc::IpcWriteOptions::Defaults(), &metadata).ok());
        auto result = RawResult(arrow::schema({arrow::field("value", arrow::uint64())}), metadata, std::string(4096, '\0'));
        auto decoded = DecodeArrowResult(result, Source(), 8192);
        const auto& buffer = decoded->column_data(0)->buffers[1];
        UNIT_ASSERT(!buffer->parent());
        UNIT_ASSERT_VALUES_EQUAL(buffer->size(), buffer->capacity());
        UNIT_ASSERT(buffer->size() < 4096);
    }

    Y_UNIT_TEST(RejectsWrongNamesTypesAndOversizedPayload) {
        auto source = Source();
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(Result(Integers(), "other"), source, source.GetMaxBatchBytes()), yexception);
        source.MutableColumns(0)->MutableType()->set_type_id(Ydb::Type::INT64);
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(Result(Integers()), source, source.GetMaxBatchBytes()), yexception);
        source = Source();
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(Result(Integers()), source, 1), yexception);
    }

    Y_UNIT_TEST(QueryServiceBoolUsesUInt8AndRejectsInvalidValues) {
        arrow::UInt8Builder builder;
        UNIT_ASSERT(builder.Append(1).ok());
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.Append(0).ok());
        auto source = Source(Ydb::Type::BOOL, true);
        auto decoded = DecodeArrowResult(Result(builder.Finish().ValueOrDie()), source, source.GetMaxBatchBytes());
        const auto& values = static_cast<const arrow::UInt8Array&>(*decoded->column(0));
        UNIT_ASSERT_VALUES_EQUAL(values.Value(0), 1);
        UNIT_ASSERT(values.IsNull(1));
        UNIT_ASSERT_VALUES_EQUAL(values.Value(2), 0);
        UNIT_ASSERT(builder.Append(2).ok());
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(Result(builder.Finish().ValueOrDie()), source,
            source.GetMaxBatchBytes()), yexception, "invalid Bool value");
    }

    Y_UNIT_TEST(BoolUsesMiniKqlLayoutAndPreservesNulls) {
        arrow::BooleanBuilder builder;
        UNIT_ASSERT(builder.Append(true).ok());
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.Append(false).ok());
        auto result = Result(builder.Finish().ValueOrDie());
        auto source = Source(Ydb::Type::BOOL, true);
        auto decoded = DecodeArrowResult(result, source, source.GetMaxBatchBytes());
        const auto& values = static_cast<const arrow::UInt8Array&>(*decoded->column(0));
        UNIT_ASSERT_VALUES_EQUAL(values.Value(0), 1);
        UNIT_ASSERT(values.IsNull(1));
        UNIT_ASSERT_VALUES_EQUAL(values.Value(2), 0);
        source = Source(Ydb::Type::BOOL, false);
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(result, source, source.GetMaxBatchBytes()), yexception);
    }

    Y_UNIT_TEST(RejectsLegacyAndUnalignedIpcFraming) {
        auto schema = arrow::schema({arrow::field("value", arrow::uint64())});
        auto schemaBytes = arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString();
        auto batch = arrow::RecordBatch::Make(schema, 3, {Integers()});
        auto batchBytes = arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie()->ToString();
        Ydb::ResultSet proto;
        proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
        proto.mutable_arrow_format_meta()->set_schema(schemaBytes.substr(4));
        proto.set_data(batchBytes);
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(NYdb::TResultSet(proto), Source(), 4096), yexception, "continuation prefix");
        proto.mutable_arrow_format_meta()->set_schema(schemaBytes);
        batchBytes[4] = static_cast<char>(static_cast<unsigned char>(batchBytes[4]) | 1);
        proto.set_data(batchBytes);
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(NYdb::TResultSet(proto), Source(), 4096), yexception, "alignment");
    }

    Y_UNIT_TEST(RejectsAliasedBoolExpansionBeforeConversion) {
        constexpr int64_t rows = 1024;
        auto source = Source(Ydb::Type::BOOL);
        source.ClearColumns();
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<arrow::ipc::internal::FieldMetadata> nodes;
        std::vector<arrow::ipc::internal::BufferMetadata> buffers;
        for (int i = 0; i < 16; ++i) {
            auto* column = source.AddColumns();
            column->SetName("c" + ToString(i));
            column->MutableType()->set_type_id(Ydb::Type::BOOL);
            fields.push_back(arrow::field(std::string(column->GetName()), arrow::boolean()));
            nodes.push_back({rows, 0, 0});
            buffers.push_back({0, 0});
            buffers.push_back({0, rows / 8}); // Every column aliases the same tiny bitmap.
        }
        std::shared_ptr<arrow::Buffer> metadata;
        UNIT_ASSERT(arrow::ipc::internal::WriteRecordBatchMessage(rows, rows / 8, nullptr,
            nodes, buffers, arrow::ipc::IpcWriteOptions::Defaults(), &metadata).ok());
        auto result = RawResult(arrow::schema(fields), metadata, std::string(rows / 8, '\xff'));
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(result, source, 4096), yexception, "Bool expansion");
    }

    Y_UNIT_TEST(RejectsExperimentalCompressionAndMetadataBeforeArrowOpen) {
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        options.metadata_version = arrow::ipc::MetadataVersion::V4;
        auto custom = arrow::key_value_metadata({"ARROW:experimental_compression"}, {"ZSTD"});
        std::shared_ptr<arrow::Buffer> metadata;
        UNIT_ASSERT(arrow::ipc::internal::WriteRecordBatchMessage(3, 24, custom,
            {{3, 0, 0}}, {{0, 0}, {0, 24}}, options, &metadata).ok());
        auto result = RawResult(arrow::schema({arrow::field("value", arrow::uint64())}), metadata, std::string(24, '\0'));
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(result, Source(), 4096), yexception, "custom metadata");
    }

    Y_UNIT_TEST(RejectsAliasedMessageMetadata) {
        flatbuffers::FlatBufferBuilder builder;
        auto key = builder.CreateString("key");
        auto value = builder.CreateString(std::string(1024, 'x'));
        auto entry = arrow::flatbuf::CreateKeyValue(builder, key, value);
        std::vector<flatbuffers::Offset<arrow::flatbuf::KeyValue>> entries(256, entry);
        auto metadataEntries = builder.CreateVector(entries);
        auto message = arrow::flatbuf::CreateMessage(builder, arrow::flatbuf::MetadataVersion::V5,
            arrow::flatbuf::MessageHeader::RecordBatch, flatbuffers::Offset<void>(), 0, metadataEntries);
        builder.Finish(message);
        auto metadata = std::make_shared<arrow::Buffer>(builder.GetBufferPointer(), builder.GetSize());
        auto result = RawResult(arrow::schema({arrow::field("value", arrow::uint64())}), metadata, {});
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(result, Source(), 4096), yexception, "custom metadata");
    }

    Y_UNIT_TEST(RejectsSchemaMetadataAndMissingHeaders) {
        auto schema = arrow::schema({arrow::field("value", arrow::uint64())}, arrow::key_value_metadata({"key"}, {"value"}));
        auto batch = arrow::RecordBatch::Make(schema, 3, {Integers()});
        Ydb::ResultSet proto;
        proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
        proto.mutable_arrow_format_meta()->set_schema(arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString());
        proto.set_data(arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie()->ToString());
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(NYdb::TResultSet(proto), Source(), 4096), yexception, "schema custom metadata");
        flatbuffers::FlatBufferBuilder builder;
        auto missingHeader = arrow::flatbuf::CreateMessage(builder, arrow::flatbuf::MetadataVersion::V5,
            arrow::flatbuf::MessageHeader::Schema, flatbuffers::Offset<void>(), 0);
        builder.Finish(missingHeader);
        arrow::Buffer metadata(builder.GetBufferPointer(), builder.GetSize());
        proto.mutable_arrow_format_meta()->set_schema(Framed(metadata));
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(NYdb::TResultSet(proto), Source(), 4096), yexception, "expected Arrow schema");
    }

    Y_UNIT_TEST(RejectsCompressionBeforeDecompression) {
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        options.codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
        auto source = Source();
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(Result(Integers(), "value", options), source, source.GetMaxBatchBytes()), yexception);
    }
}

} // namespace NYql::NYdbRemote
