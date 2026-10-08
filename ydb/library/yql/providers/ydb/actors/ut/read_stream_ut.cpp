#include <ydb/library/yql/providers/ydb/actors/read_stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <arrow/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/metadata_internal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/compression.h>

namespace NYql::NYdb {
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

::NYdb::TResultSet Result(std::shared_ptr<arrow::Array> values, const std::string& name = "value",
                       arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults()) {
    auto schema = arrow::schema({arrow::field(name, values->type())});
    auto batch = arrow::RecordBatch::Make(schema, values->length(), {values});
    auto schemaBytes = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
    auto bytes = arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
    Ydb::ResultSet proto;
    proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
    proto.mutable_arrow_format_meta()->set_schema(schemaBytes->ToString());
    proto.set_data(bytes->ToString());
    return ::NYdb::TResultSet(std::move(proto));
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

::NYdb::TResultSet RawResult(const std::shared_ptr<arrow::Schema>& schema,
                          const std::shared_ptr<arrow::Buffer>& metadata, const std::string& body) {
    Ydb::ResultSet proto;
    proto.set_format(Ydb::ResultSet::FORMAT_ARROW);
    proto.mutable_arrow_format_meta()->set_schema(arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString());
    proto.set_data(Framed(*metadata, body));
    return ::NYdb::TResultSet(std::move(proto));
}

std::shared_ptr<arrow::Array> Integers() {
    arrow::UInt64Builder builder;
    UNIT_ASSERT(builder.AppendValues({1, 2, 3}).ok());
    return builder.Finish().ValueOrDie();
}

} // namespace

Y_UNIT_TEST_SUITE(YdbReadStream) {
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
        auto output = TakeOutputBatch(*decoded, 0, 1024, MaxOutputRowBytes);
        decoded.reset();
        const auto& buffer = output->column_data(0)->buffers[1];
        UNIT_ASSERT(!buffer->parent());
        UNIT_ASSERT_VALUES_EQUAL(buffer->size(), buffer->capacity());
        UNIT_ASSERT(buffer->size() < 4096);
    }

    Y_UNIT_TEST(SplitsByOwnedBytesAndPreservesOffsetsAndNulls) {
        arrow::StringBuilder strings;
        arrow::UInt64Builder integers;
        arrow::UInt8Builder flags;
        for (ui64 row = 0; row < 97; ++row) {
            UNIT_ASSERT(integers.Append(row).ok());
            if (row % 7 == 0) {
                UNIT_ASSERT(strings.AppendNull().ok());
                UNIT_ASSERT(flags.AppendNull().ok());
            } else {
                UNIT_ASSERT(strings.Append(std::string(37 * (row % 19), 'a' + row % 26)).ok());
                UNIT_ASSERT(flags.Append(row % 2).ok());
            }
        }
        auto batch = arrow::RecordBatch::Make(arrow::schema({
            arrow::field("value", arrow::utf8()), arrow::field("key", arrow::uint64()),
            arrow::field("flag", arrow::uint8())}), 97,
            {strings.Finish().ValueOrDie(), integers.Finish().ValueOrDie(), flags.Finish().ValueOrDie()});
        batch = batch->Slice(3, 90); // Exercise input offsets and non-byte-aligned bitmaps.
        ui64 blocks = 0;
        int64_t offset = 0;
        while (offset < batch->num_rows()) {
            auto output = TakeOutputBatch(*batch, offset, 2048, MaxOutputRowBytes);
            UNIT_ASSERT(output->ValidateFull().ok());
            UNIT_ASSERT(output->num_rows() > 0);
            UNIT_ASSERT(NUdf::GetSizeOfArrowBatchInBytes(*output) <= 2048);
            for (const auto& column : output->columns()) {
                UNIT_ASSERT_VALUES_EQUAL(column->offset(), 0);
                for (const auto& buffer : column->data()->buffers) {
                    if (buffer) {
                        UNIT_ASSERT(!buffer->parent());
                        UNIT_ASSERT_VALUES_EQUAL(buffer->size(), buffer->capacity());
                    }
                }
            }
            const auto& values = static_cast<const arrow::StringArray&>(*output->column(0));
            const auto& keys = static_cast<const arrow::UInt64Array&>(*output->column(1));
            const auto& bools = static_cast<const arrow::UInt8Array&>(*output->column(2));
            for (int64_t row = 0; row < output->num_rows(); ++row) {
                const ui64 expected = offset + row + 3;
                UNIT_ASSERT_VALUES_EQUAL(keys.Value(row), expected);
                UNIT_ASSERT_VALUES_EQUAL(values.IsNull(row), expected % 7 == 0);
                UNIT_ASSERT_VALUES_EQUAL(bools.IsNull(row), expected % 7 == 0);
                if (!values.IsNull(row)) {
                    UNIT_ASSERT_VALUES_EQUAL(values.GetString(row), std::string(37 * (expected % 19), 'a' + expected % 26));
                    UNIT_ASSERT_VALUES_EQUAL(bools.Value(row), expected % 2);
                }
            }
            offset += output->num_rows();
            ++blocks;
        }
        UNIT_ASSERT_VALUES_EQUAL(offset, 90);
        UNIT_ASSERT(blocks > 1);
    }

    Y_UNIT_TEST(LargeRowUsesOwnBlockAndDoesNotRetainFollowingRows) {
        arrow::StringBuilder builder;
        const std::string value(2 * 1024 * 1024, 'x');
        UNIT_ASSERT(builder.Append(value).ok());
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.Append("tail").ok());
        auto decoded = DecodeArrowResult(Result(builder.Finish().ValueOrDie()), Source(Ydb::Type::UTF8, true), MaxDecodedPartBytes);
        std::weak_ptr<arrow::Buffer> inputBuffer = decoded->column_data(0)->buffers[2];
        auto first = TakeOutputBatch(*decoded, 0, 1024 * 1024, MaxOutputRowBytes);
        UNIT_ASSERT_VALUES_EQUAL(first->num_rows(), 1);
        UNIT_ASSERT(NUdf::GetSizeOfArrowBatchInBytes(*first) > 1024 * 1024);
        auto tail = TakeOutputBatch(*decoded, 1, 1024 * 1024, MaxOutputRowBytes);
        UNIT_ASSERT_VALUES_EQUAL(tail->num_rows(), 2);
        decoded.reset();
        UNIT_ASSERT(inputBuffer.expired());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::StringArray&>(*first->column(0)).GetString(0), value);
        const auto& values = static_cast<const arrow::StringArray&>(*tail->column(0));
        UNIT_ASSERT(values.IsNull(0));
        UNIT_ASSERT_VALUES_EQUAL(values.GetString(1), "tail");
    }

    Y_UNIT_TEST(SingleRowLimitIncludesPaddingOffsetsAndAccounting) {
        arrow::StringBuilder builder;
        UNIT_ASSERT(builder.Append(std::string(4096, 'x')).ok());
        auto decoded = DecodeArrowResult(Result(builder.Finish().ValueOrDie()), Source(Ydb::Type::UTF8), MaxDecodedPartBytes);
        auto output = TakeOutputBatch(*decoded, 0, 1024, MaxOutputRowBytes);
        const auto bytes = NUdf::GetSizeOfArrowBatchInBytes(*output);
        UNIT_ASSERT(bytes > 4096);
        UNIT_ASSERT(TakeOutputBatch(*decoded, 0, 1024, bytes));
        UNIT_ASSERT_EXCEPTION_CONTAINS(TakeOutputBatch(*decoded, 0, 1024, bytes - 1), yexception, "single output row");
    }

    Y_UNIT_TEST(RejectsAliasedFixedWidthBuffersBeforeOutputCopy) {
        auto source = Source();
        auto* second = source.AddColumns();
        *second = source.GetColumns(0);
        second->SetName("other");
        std::shared_ptr<arrow::Buffer> metadata;
        UNIT_ASSERT(arrow::ipc::internal::WriteRecordBatchMessage(512, 4096, nullptr,
            {{512, 0, 0}, {512, 0, 0}}, {{0, 0}, {0, 4096}, {0, 0}, {0, 4096}},
            arrow::ipc::IpcWriteOptions::Defaults(), &metadata).ok());
        auto result = RawResult(arrow::schema({arrow::field("value", arrow::uint64()),
            arrow::field("other", arrow::uint64())}), metadata, std::string(4096, '\0'));
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(result, source, 5000), yexception, "decoded buffers");
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(::NYdb::TResultSet(proto), Source(), 4096), yexception, "continuation prefix");
        proto.mutable_arrow_format_meta()->set_schema(schemaBytes);
        batchBytes[4] = static_cast<char>(static_cast<unsigned char>(batchBytes[4]) | 1);
        proto.set_data(batchBytes);
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(::NYdb::TResultSet(proto), Source(), 4096), yexception, "alignment");
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(::NYdb::TResultSet(proto), Source(), 4096), yexception, "schema custom metadata");
        flatbuffers::FlatBufferBuilder builder;
        auto missingHeader = arrow::flatbuf::CreateMessage(builder, arrow::flatbuf::MetadataVersion::V5,
            arrow::flatbuf::MessageHeader::Schema, flatbuffers::Offset<void>(), 0);
        builder.Finish(missingHeader);
        arrow::Buffer metadata(builder.GetBufferPointer(), builder.GetSize());
        proto.mutable_arrow_format_meta()->set_schema(Framed(metadata));
        UNIT_ASSERT_EXCEPTION_CONTAINS(DecodeArrowResult(::NYdb::TResultSet(proto), Source(), 4096), yexception, "expected Arrow schema");
    }

    Y_UNIT_TEST(RejectsCompressionBeforeDecompression) {
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        options.codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
        auto source = Source();
        UNIT_ASSERT_EXCEPTION(DecodeArrowResult(Result(Integers(), "value", options), source, source.GetMaxBatchBytes()), yexception);
    }
}

} // namespace NYql::NYdb
