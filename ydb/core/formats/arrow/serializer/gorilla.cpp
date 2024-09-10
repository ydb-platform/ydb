#include "gorilla/compressor.h"
#include "gorilla/decompressor.h"
#include "gorilla.h"
#include "stream.h"
#include "parsing.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/formats/arrow/common/validation.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <sstream>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

namespace NKikimr::NArrow::NSerialization {
    // Character dividing arrow schema and compressed data in serialized string.
    const char SCHEMA_TO_DATA_DIVIDER = '\n';

    uint64_t TGorillaSerializer::getU64FromArrayData(
            std::shared_ptr<arrow::DataType> &column_type,
            std::shared_ptr<arrow::ArrayData> &array_data,
            size_t i
    ) const {
        uint64_t reinterpreted_value;
        if (column_type->Equals(arrow::uint64())) {
            uint64_t value = array_data->GetValues<uint64_t>(1)[i];
            reinterpreted_value = *reinterpret_cast<uint64_t *>(&value);
        } else if (column_type->Equals(arrow::uint32())) {
            uint32_t value = array_data->GetValues<uint32_t>(1)[i];
            reinterpreted_value = *reinterpret_cast<uint64_t *>(&value);
        } else if (column_type->Equals(arrow::DoubleType())) {
            double value = array_data->GetValues<double>(1)[i];
            reinterpreted_value = *reinterpret_cast<uint64_t *>(&value);
        } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            auto casted_timestamp_data = arrow::TimestampArray(array_data);
            reinterpreted_value = casted_timestamp_data.Value(i);
        } else {
            Y_ABORT("Unknown value column type met for uint64_t serialization.");
        }
        return reinterpreted_value;
    }

    std::shared_ptr<arrow::ArrayBuilder> TGorillaSerializer::getColumnBuilderByType(
            std::shared_ptr<arrow::DataType> &column_type
    ) const {
        std::shared_ptr<arrow::ArrayBuilder> value_column_builder;
        if (column_type->Equals(arrow::uint64())) {
            value_column_builder = std::make_shared<arrow::UInt64Builder>();
        } else if (column_type->Equals(arrow::uint32())) {
            value_column_builder = std::make_shared<arrow::UInt32Builder>();
        } else if (column_type->Equals(arrow::DoubleType())) {
            value_column_builder = std::make_shared<arrow::DoubleBuilder>();
        } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            value_column_builder = std::make_shared<arrow::TimestampBuilder>(
                    arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
        } else {
            Y_ABORT("Unknown value column type met to get column builder.");
        }
        return value_column_builder;
    }

    arrow::Status TGorillaSerializer::builderAppendValue(
            std::shared_ptr<arrow::DataType> &column_type,
            std::shared_ptr<arrow::ArrayBuilder> &column_builder,
            uint64_t value
    ) const {
        if (column_type->Equals(arrow::uint64())) {
            Y_ABORT_UNLESS(std::dynamic_pointer_cast<arrow::UInt64Builder>(column_builder)->Append(value).ok());
        } else if (column_type->Equals(arrow::uint32())) {
            uint32_t reinterpreted_value = *reinterpret_cast<uint32_t *>(&value);
            Y_ABORT_UNLESS(
                    std::dynamic_pointer_cast<arrow::UInt32Builder>(column_builder)->Append(reinterpreted_value).ok());
        } else if (column_type->Equals(arrow::DoubleType())) {
            double reinterpreted_value = *reinterpret_cast<double *>(&value);
            Y_ABORT_UNLESS(
                    std::dynamic_pointer_cast<arrow::DoubleBuilder>(column_builder)->Append(reinterpreted_value).ok());
        } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            Y_ABORT_UNLESS(std::dynamic_pointer_cast<arrow::TimestampBuilder>(column_builder)->Append(value).ok());
        } else {
            Y_ABORT("Unknown value column type met to append value to builder.");
        }
        return arrow::Status::OK();
    }

    std::vector<uint64_t> TGorillaSerializer::getU64VecFromBatch(
            const std::shared_ptr<arrow::RecordBatch> &batch,
            size_t column_index
    ) const {
        std::vector<uint64_t> entities_vec;
        auto data = batch->column_data()[column_index];
        auto column_type = batch->schema()->field(column_index)->type();
        auto array_size = data->length;

        entities_vec.reserve(array_size);
        for (int i = 0; i < array_size; i++) {
            uint64_t reinterpretedValue = getU64FromArrayData(column_type, data, i);
            entities_vec.push_back(reinterpretedValue);
        }

        return entities_vec;
    }

    template<typename T, typename F>
    arrow::Result<TString> TGorillaSerializer::serializeBatchEntities(
            const std::shared_ptr<arrow::Schema> &batch_schema,
            std::vector<T> &entities,
            F create_c_func
    ) const {
        auto schema_serialized_buffer = arrow::ipc::SerializeSchema(*batch_schema).ValueOrDie();
        auto schema_serialized_str = schema_serialized_buffer->ToString();

        std::stringstream out_stream;
        auto arrays_size = entities.size();

        std::unique_ptr<NGorilla::CompressorBase<T>> c = create_c_func(out_stream);
        for (size_t i = 0; i < arrays_size; i++) {
            c->compress(entities[i]);
        }
        c->finish();
        TString compressed = out_stream.str();

        return {std::to_string(schema_serialized_str.length()) + "\n" + schema_serialized_str + compressed};
    }

    TString TGorillaSerializer::DoSerializePayload(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto initial_schema = batch->schema();
        auto column_type = initial_schema->field(0)->type();

        auto entities_vec = getU64VecFromBatch(batch, 0);
        arrow::Result<TString> serialization_res;
        if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            serialization_res = serializeBatchEntities(initial_schema, entities_vec, [](std::stringstream &out_stream) {
                auto bw = std::make_shared<NGorilla::BitWriter>(out_stream);
                return std::make_unique<NGorilla::TimestampsCompressor>(bw);
            });
        } else {
            serialization_res = serializeBatchEntities(initial_schema, entities_vec, [](std::stringstream &out_stream) {
                auto bw = std::make_shared<NGorilla::BitWriter>(out_stream);
                return std::make_unique<NGorilla::ValuesCompressor>(bw);
            });
        }
        Y_ABORT_UNLESS(serialization_res.ok());
        return *serialization_res;
    }

    TString TGorillaSerializer::DoSerializeFull(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return DoSerializePayload(batch);
    }

    template<typename T>
    std::vector<T> deserializeEntities(std::unique_ptr<NGorilla::DecompressorBase<T>> &d) {
        std::vector<T> entities;
        std::optional<T> current_pair;
        do {
            current_pair = d->next();
            if (current_pair) {
                entities.push_back(*current_pair);
            }
        } while (current_pair);
        return entities;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data) const {
        // Deserialize batch schema.
        size_t div_pos = data.find_first_of(SCHEMA_TO_DATA_DIVIDER);
        if (div_pos == std::string::npos) {
            Y_ABORT("RecordBatch schema to compressed data divider not found.");
        }
        size_t schema_length;
        std::stringstream header_ss((data.substr(0, div_pos)));
        header_ss >> schema_length;
        size_t schema_from_pos = div_pos + 1;
        auto reader_buf = arrow::Buffer::FromString(data.substr(schema_from_pos));
        auto reader_stream = std::make_unique<arrow::io::BufferReader>(reader_buf);
        arrow::ipc::DictionaryMemo dictMemo;
        auto schema = arrow::ipc::ReadSchema(reader_stream.get(), &dictMemo).ValueOrDie();

        // Deserialize data.
        auto column_type = schema->field(0)->type();
        std::stringstream in_stream(data.substr(schema_from_pos + schema_length));
        auto br = std::make_shared<NGorilla::BitReader>(in_stream);
        std::unique_ptr<NGorilla::DecompressorBase<uint64_t>> d;
        if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            d = std::make_unique<NGorilla::TimestampsDecompressor>(br);
        } else {
            d = std::make_unique<NGorilla::ValuesDecompressor>(br);
        }
        auto entities = deserializeEntities(d);

        auto column_builder = getColumnBuilderByType(column_type);
        for (auto e: entities) {
            Y_ABORT_UNLESS(builderAppendValue(column_type, column_builder, e).ok());
        }

        std::shared_ptr<arrow::Array> column_array;
        ARROW_ASSIGN_OR_RAISE(column_array, column_builder->Finish());

        std::shared_ptr<arrow::RecordBatch> batch_deserialized = arrow::RecordBatch::Make(schema, entities.size(),
                                                                                          {column_array});

        auto validation = batch_deserialized->Validate();
        if (!validation.ok()) {
            return arrow::Status(arrow::StatusCode::SerializationError, "validation error: " + validation.ToString());
        }

        return { batch_deserialized };
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data, const std::shared_ptr<arrow::Schema>& schema) const {
        auto batch_result = DoDeserialize(data);
        if (!batch_result.ok()) {
            return batch_result;
        }
        std::shared_ptr<arrow::RecordBatch> batch = *batch_result;
        if (!batch->schema()->Equals(*schema)) {
            return arrow::Status(arrow::StatusCode::SerializationError, "deserialized schema is corrupted");
        }
        return batch_result;
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
        Y_UNUSED(features);
        return TConclusionStatus::Success();
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) {
        Y_UNUSED(proto);
        return TConclusionStatus::Success();
    }

    void TGorillaSerializer::DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const {
        Y_UNUSED(proto);
    }
}
