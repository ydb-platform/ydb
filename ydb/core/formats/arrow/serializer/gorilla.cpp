#include "gorilla.h"
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

// Character dividing arrow schema and compressed data in serialized string.
const SCHEMA_TO_DATA_DIVIDER = '\n';

namespace NKikimr::NArrow::NSerialization {
    TString TGorillaSerializer::DoSerializeFull(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        // TODO: ???
        return DoSerializePayload(batch);
    }

    TString TGorillaSerializer::DoSerializePayload(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        std::stringstream out_stream;

        auto timestamp_data = batch->column_data()[0];
        arrow::TimestampArray casted_timestamp_data(timestamp_data);
        auto values_data = batch->column_data()[1];
        auto value_column_type = batch->schema()->field(1)->type();
        auto arrays_size = timestamp_data->length;

        // Header is a first time aligned to 2 hours window.
        uint64_t first_time = casted_timestamp_data.Value(0);
        uint64_t header = first_time - (first_time % (60 * 60 * 2));

        auto schema_serialized_buffer = arrow::ipc::SerializeSchema(*batch->schema()).ValueOrDie();
        auto schema_serialized_str = schema_serialized_buffer->ToString();

        Compressor c(out_stream, header);
        for (int i = 0; i < arrays_size; i++) {
            uint64_t reinterpreted_value;
            if (value_column_type->Equals(arrow::uint64())) {
                uint64_t value = values_data->GetValues<uint64_t>(1)[i];
                reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
            } else if (value_column_type->Equals(arrow::uint32())) {
                uint32_t value = values_data->GetValues<uint32_t>(1)[i];
                reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
            } else if (value_column_type->Equals(arrow::DoubleType())) {
                double value = values_data->GetValues<double>(1)[i];
                reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
            } else {
                Y_ABORT("Unimplemented.");
            }
            c.compress(casted_timestamp_data.Value(i), reinterpreted_value);
        }
        c.finish();

        std::string compressed = out_stream.str();
        return { std::to_string(schema_serialized_str.length()) + SCHEMA_TO_DATA_DIVIDER + schema_serialized_str + compressed };
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data) const {
        size_t divider_index = data.find_first_of(SCHEMA_TO_DATA_DIVIDER);
        if (divider_index == std::string::npos) {
            Y_ABORT("RecordBatch schema to compressed data divider not found.");
        }
        size_t schema_length;
        std::stringstream header_ss((data.substr(0, divider_index)));
        header_ss >> schema_length;

        size_t schema_from_index = divider_index + 1;
        auto reader_stream = arrow::io::BufferReader::FromString(data.substr(schema_from_index));
        arrow::ipc::DictionaryMemo dictMemo;
        auto schema_deserialized = arrow::ipc::ReadSchema(reader_stream.get(), &dictMemo).ValueOrDie();
        auto value_column_type = schema_deserialized->field(1)->type();

        std::stringstream in_stream(data.substr(schema_from_index + schema_length));

        auto time_column_builder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
        std::shared_ptr<arrow::ArrayBuilder> value_column_builder;
        if (value_column_type->Equals(arrow::uint64())) {
            value_column_builder = std::make_shared<arrow::UInt64Builder>();
        } else if (value_column_type->Equals(arrow::uint32())) {
            value_column_builder = std::make_shared<arrow::UInt32Builder>();
        } else if (value_column_type->Equals(arrow::DoubleType())) {
            value_column_builder = std::make_shared<arrow::DoubleBuilder>();
        } else {
            Y_ABORT("Unimplemented.");
        }

        Decompressor d(in_stream);
        std::optional<std::pair<uint64_t, uint64_t>> current_pair = std::nullopt;
        int rows_counter = 0;
        do {
            current_pair = d.next();
            if (current_pair) {
                Y_VERIFY_OK(time_column_builder.Append((*current_pair).first));

                uint64_t value = (*current_pair).second;
                if (value_column_type->Equals(arrow::uint64())) {
                    Y_VERIFY_OK(std::dynamic_pointer_cast<arrow::UInt64Builder>(value_column_builder)->Append(value));
                } else if (value_column_type->Equals(arrow::uint32())) {
                    uint32_t reinterpreted_value = *reinterpret_cast<uint32_t*>(&value);
                    Y_VERIFY_OK(std::dynamic_pointer_cast<arrow::UInt32Builder>(value_column_builder)->Append(reinterpreted_value));
                } else if (value_column_type->Equals(arrow::DoubleType())) {
                    double reinterpreted_value = *reinterpret_cast<double*>(&value);
                    Y_VERIFY_OK(std::dynamic_pointer_cast<arrow::DoubleBuilder>(value_column_builder)->Append(reinterpreted_value));
                } else {
                    Y_ABORT("Unimplemented.");
                }

                rows_counter++;
            }
        } while (current_pair);

        std::shared_ptr<arrow::Array> time_column_array;
        Y_VERIFY_OK(time_column_builder.Finish(&time_column_array));
        std::shared_ptr<arrow::Array> value_column_array;
        Y_VERIFY_OK(value_column_builder->Finish(&value_column_array));

        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema_deserialized, rows_counter, {time_column_array, value_column_array});

        auto validation = batch->Validate();
        if (!validation.ok()) {
            return arrow::Status(arrow::StatusCode::SerializationError, "validation error: " + validation.ToString());
        }
        return batch;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data, const std::shared_ptr<arrow::Schema>& schema) const {
        // TODO: ???
        (void) schema;
        return DoDeserialize(data);
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
        // TODO: ???
        return TConclusionStatus::Success();
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) {
        // TOOD: ???
        return TConclusionStatus::Success();
    }

    void TGorillaSerializer::DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const { }

}
