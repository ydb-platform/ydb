#include "arrow_inference.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <arrow/json/options.h>
#include <arrow/json/reader.h>
#include <parquet/arrow/reader.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/yexception.h>

namespace NYdb::NArrowInference {

namespace {

bool ShouldBeOptional(const arrow::DataType& type, std::shared_ptr<TFormatConfig> config) {
    if (!config->ShouldMakeOptional) {
        return false;
    }

    switch (type.id()) {
        case arrow::Type::NA:
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
        return false;
    default:
        return true;
    }
}

bool IsFloatingPoint(arrow::Type::type id) {
    return id == arrow::Type::FLOAT || id == arrow::Type::DOUBLE || id == arrow::Type::HALF_FLOAT;
}

bool IsInteger(arrow::Type::type id) {
    return id >= arrow::Type::UINT8 && id <= arrow::Type::INT64;
}

std::shared_ptr<arrow::DataType> GetCommonDataType(const std::shared_ptr<arrow::DataType>& type1,
                                                   const std::shared_ptr<arrow::DataType>& type2) {
    if (type1->Equals(*type2)) {
        return type1;
    }
    if (IsFloatingPoint(type1->id()) || IsFloatingPoint(type2->id())) {
        return arrow::float64();
    }
    if (IsInteger(type1->id()) && IsInteger(type2->id())) {
        return arrow::int64();
    }

    // TODO: Format specific. Utf8 is ok for all CSV data. For parquet its probably binary
    return arrow::utf8();
}

std::shared_ptr<arrow::Schema> InferCommonSchema(const std::vector<std::shared_ptr<arrow::Schema>>& schemas) {
    if (schemas.empty()) {
        return nullptr;
    }
    if (schemas.size() == 1) {
        return schemas[0];
    }

    auto commonFields = schemas[0]->fields();

    for (size_t i = 1; i < schemas.size(); ++i) {
        auto schema = schemas[i];
        for (int j = 0; j < schema->num_fields(); ++j) {
            auto currentField = commonFields[j];
            auto newField = schema->field(j);
            if (currentField->Equals(newField)) {
                continue;
            }

            auto commonType = GetCommonDataType(currentField->type(), newField->type());
            bool isNullable = currentField->nullable() || schema->field(j)->nullable();
            commonFields[j] = arrow::field(commonFields[j]->name(), commonType, isNullable);
        }
    }

    return std::make_shared<arrow::Schema>(commonFields);
}

std::shared_ptr<arrow::Schema> GetSchemaFromCsv(
    const std::shared_ptr<arrow::io::InputStream>& input,
    std::shared_ptr<TCsvConfig> config) {
    if (!config) {
        return nullptr;
    }

    config->ReadOpts.use_threads = false;
    config->ReadOpts.block_size = 1 << 20;

    auto result = arrow::csv::StreamingReader::Make(
        arrow::io::default_io_context(), input, config->ReadOpts, config->ParseOpts, config->ConvOpts);
    if (!result.ok()) {
        return nullptr;
    }

    auto streamingReader = result.ValueOrDie();
    int64_t rowsRead = 0;
    std::shared_ptr<arrow::Schema> schema = nullptr;

    while (rowsRead < config->RowsToAnalyze || config->RowsToAnalyze == 0) {
        auto batch_result = streamingReader->Next();
        if (!batch_result.ok() || !(*batch_result)) {
            break; // No more data
        }

        auto batch = *batch_result;
        rowsRead += batch->num_rows();

        if (!schema) {
            schema = batch->schema(); // TODO: merge
        }
    }

    return schema;
}

std::shared_ptr<arrow::Schema> GetSchemaFromJson(const std::shared_ptr<arrow::io::InputStream>& input, std::shared_ptr<TJsonConfig> config) {
    if (!config) {
        return nullptr;
    }
    std::shared_ptr<arrow::json::TableReader> reader;
    arrow::json::ReadOptions readOptions = arrow::json::ReadOptions::Defaults();
    readOptions.use_threads = false;
    if (auto randomFile = std::dynamic_pointer_cast<arrow::io::RandomAccessFile>(input)) {
        int64_t fileSize;
        auto size_status = randomFile->GetSize().Value(&fileSize);
        if (size_status.ok()) {
            readOptions.block_size = static_cast<int32_t>(fileSize);
        }
    }
    auto result = arrow::json::TableReader::Make(arrow::default_memory_pool(), input, readOptions, config->ParseOpts).Value(&reader);
    if (!result.ok()) {
        return nullptr;
    }
    auto tableResult = reader->Read();
    if (!tableResult.ok()) {
        return nullptr;
    }
    return tableResult.ValueOrDie()->schema();
}

std::shared_ptr<arrow::Schema> GetSchemaFromParquet(const std::shared_ptr<arrow::io::InputStream>& input) {
    auto file = std::dynamic_pointer_cast<arrow::io::RandomAccessFile>(input);
    if (!file) {
        return nullptr;
    }

    parquet::arrow::FileReaderBuilder builder;
    builder.properties(parquet::ArrowReaderProperties(false));
    auto openStatus = builder.Open(file);
    if (!openStatus.ok()) {
        return nullptr;
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto readerStatus = builder.Build(&reader);
    if (!readerStatus.ok()) {
        return nullptr;
    }

    std::shared_ptr<arrow::Schema> schema;
    auto schemaRes = reader->GetSchema(&schema);
    if (!schemaRes.ok()) {
        return nullptr;
    }

    return schema;
}

} // namespace

std::variant<ArrowFields, TString> InferTypes(const std::vector<std::shared_ptr<arrow::io::InputStream>>& inputs, std::shared_ptr<TFormatConfig> config) {
    if (inputs.empty()) {
        return TString{"no input files"};
    }

    std::vector<std::shared_ptr<arrow::Schema>> schemas;

    for (auto& input : inputs) {
        std::shared_ptr<arrow::Schema> schema;

        switch (config->Format) {
        case EFileFormat::CsvWithNames:
        case EFileFormat::TsvWithNames:
            schema = GetSchemaFromCsv(input, std::dynamic_pointer_cast<TCsvConfig>(config));
            break;

        case EFileFormat::Parquet:
            schema = GetSchemaFromParquet(input);
            break;
        
        case EFileFormat::JsonEachRow:
        case EFileFormat::JsonList:
            schema = GetSchemaFromJson(input, std::dynamic_pointer_cast<TJsonConfig>(config));
            break;

        default:
            return TStringBuilder{} << "unexpected format: " << ConvertFileFormat(config->Format);
        }

        if (!schema) {
            return TString{"Failed to read schema from input stream."};
        }

        schemas.push_back(schema);
    }

    auto commonSchema = InferCommonSchema(schemas);
    if (!commonSchema) {
        return TStringBuilder{} << "couldn't infer common schema";
    }

    return commonSchema->fields();
}

bool ArrowToYdbType(Ydb::Type& result, const arrow::DataType& type, std::shared_ptr<TFormatConfig> config) {
    auto& resType = ShouldBeOptional(type, config) ? *result.mutable_optional_type()->mutable_item() : result;
    switch (type.id()) {
    case arrow::Type::NA:
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow::Type::BOOL:
        resType.set_type_id(Ydb::Type::BOOL);
        return true;
    case arrow::Type::UINT8:
        resType.set_type_id(Ydb::Type::UINT8);
        return true;
    case arrow::Type::INT8:
        resType.set_type_id(Ydb::Type::INT8);
        return true;
    case arrow::Type::UINT16:
        resType.set_type_id(Ydb::Type::UINT16);
        return true;
    case arrow::Type::INT16:
        resType.set_type_id(Ydb::Type::INT16);
        return true;
    case arrow::Type::UINT32:
        resType.set_type_id(Ydb::Type::UINT32);
        return true;
    case arrow::Type::INT32:
        resType.set_type_id(Ydb::Type::INT32);
        return true;
    case arrow::Type::UINT64:
        resType.set_type_id(Ydb::Type::UINT64);
        return true;
    case arrow::Type::INT64:
        resType.set_type_id(Ydb::Type::INT64);
        return true;
    case arrow::Type::HALF_FLOAT: // TODO: is there anything?
        return false;
    case arrow::Type::FLOAT:
        resType.set_type_id(Ydb::Type::FLOAT);
        return true;
    case arrow::Type::DOUBLE:
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow::Type::STRING: // TODO: is it true?
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow::Type::BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow::Type::FIXED_SIZE_BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow::Type::DATE32:
        resType.set_type_id(Ydb::Type::DATE);
        return true;
    case arrow::Type::DATE64: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DATETIME64);
        return true;
    case arrow::Type::TIMESTAMP:
        if (config->Format == EFileFormat::JsonEachRow || config->Format == EFileFormat::JsonList) {
            result.set_type_id(Ydb::Type::UTF8);
        } else {
            resType.set_type_id(Ydb::Type::TIMESTAMP);
        }
        return true;
    case arrow::Type::TIME32: // TODO: is there anything?
        return false;
    case arrow::Type::TIME64: // TODO: is there anything?
        return false;
    case arrow::Type::INTERVAL_MONTHS: // TODO: is it true?
        return false;
    case arrow::Type::INTERVAL_DAY_TIME: // TODO: is it true?
        resType.set_type_id(Ydb::Type::INTERVAL64);
        return true;
    case arrow::Type::DECIMAL128: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow::Type::DECIMAL256: // TODO: is there anything?
        return false;
    case arrow::Type::LARGE_LIST: // TODO: is it true?
    case arrow::Type::FIXED_SIZE_LIST: // TODO: is it true?
    case arrow::Type::LIST: { // TODO: is ok?
        return false;
    }
    case arrow::Type::STRUCT:
    case arrow::Type::SPARSE_UNION:
    case arrow::Type::DENSE_UNION: {
        return false;
    }
    case arrow::Type::DICTIONARY: // TODO: is representable?
        return false;
    case arrow::Type::MAP: { // TODO: is ok?
        return false;
    }
    case arrow::Type::EXTENSION: // TODO: is representable?
        return false;
    case arrow::Type::DURATION: // TODO: is it true?
        resType.set_type_id(Ydb::Type::INTERVAL64);
        return true;
    case arrow::Type::LARGE_STRING: // TODO: is it true?
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow::Type::LARGE_BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow::Type::MAX_ID:
        return false;
    }
    return false;
}

} // namespace NYdb::NArrowInference