#include "arrow_inference.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/csv/options.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/csv/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/json/options.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/json/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/yexception.h>

namespace NYdb::NArrowInference {

namespace {

bool ShouldBeOptional(const arrow20::DataType& type, std::shared_ptr<TFormatConfig> config) {
    if (!config->ShouldMakeOptional) {
        return false;
    }

    switch (type.id()) {
        case arrow20::Type::NA:
        case arrow20::Type::STRING:
        case arrow20::Type::BINARY:
        case arrow20::Type::LARGE_BINARY:
        case arrow20::Type::FIXED_SIZE_BINARY:
        return false;
    default:
        return true;
    }
}

bool IsFloatingPoint(arrow20::Type::type id) {
    return id == arrow20::Type::FLOAT || id == arrow20::Type::DOUBLE || id == arrow20::Type::HALF_FLOAT;
}

bool IsInteger(arrow20::Type::type id) {
    return id >= arrow20::Type::UINT8 && id <= arrow20::Type::INT64;
}

std::shared_ptr<arrow20::DataType> GetCommonDataType(const std::shared_ptr<arrow20::DataType>& type1,
                                                   const std::shared_ptr<arrow20::DataType>& type2) {
    if (type1->Equals(*type2)) {
        return type1;
    }
    if (IsFloatingPoint(type1->id()) || IsFloatingPoint(type2->id())) {
        return arrow20::float64();
    }
    if (IsInteger(type1->id()) && IsInteger(type2->id())) {
        return arrow20::int64();
    }

    // TODO: Format specific. Utf8 is ok for all CSV data. For parquet its probably binary
    return arrow20::utf8();
}

std::shared_ptr<arrow20::Schema> InferCommonSchema(const std::vector<std::shared_ptr<arrow20::Schema>>& schemas) {
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
            commonFields[j] = arrow20::field(commonFields[j]->name(), commonType, isNullable);
        }
    }

    return std::make_shared<arrow20::Schema>(commonFields);
}

std::shared_ptr<arrow20::Schema> GetSchemaFromCsv(
    const std::shared_ptr<arrow20::io::InputStream>& input,
    std::shared_ptr<TCsvConfig> config) {
    if (!config) {
        return nullptr;
    }

    config->ReadOpts.use_threads = false;
    config->ReadOpts.block_size = 1 << 20;

    auto result = arrow20::csv::StreamingReader::Make(
        arrow20::io::default_io_context(), input, config->ReadOpts, config->ParseOpts, config->ConvOpts);
    if (!result.ok()) {
        return nullptr;
    }

    auto streamingReader = result.ValueOrDie();
    int64_t rowsRead = 0;
    std::shared_ptr<arrow20::Schema> schema = nullptr;

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

std::shared_ptr<arrow20::Schema> GetSchemaFromJson(const std::shared_ptr<arrow20::io::InputStream>& input, std::shared_ptr<TJsonConfig> config) {
    if (!config) {
        return nullptr;
    }
    std::shared_ptr<arrow20::json::TableReader> reader;
    arrow20::json::ReadOptions readOptions = arrow20::json::ReadOptions::Defaults();
    readOptions.use_threads = false;
    if (auto randomFile = std::dynamic_pointer_cast<arrow20::io::RandomAccessFile>(input)) {
        int64_t fileSize;
        auto size_status = randomFile->GetSize().Value(&fileSize);
        if (size_status.ok()) {
            readOptions.block_size = static_cast<int32_t>(fileSize);
        }
    }
    auto result = arrow20::json::TableReader::Make(arrow20::default_memory_pool(), input, readOptions, config->ParseOpts).Value(&reader);
    if (!result.ok()) {
        return nullptr;
    }
    auto tableResult = reader->Read();
    if (!tableResult.ok()) {
        return nullptr;
    }
    return tableResult.ValueOrDie()->schema();
}

std::shared_ptr<arrow20::Schema> GetSchemaFromParquet(const std::shared_ptr<arrow20::io::InputStream>& input) {
    auto file = std::dynamic_pointer_cast<arrow20::io::RandomAccessFile>(input);
    if (!file) {
        return nullptr;
    }

    parquet20::arrow20::FileReaderBuilder builder;
    builder.properties(parquet20::ArrowReaderProperties(false));
    auto openStatus = builder.Open(file);
    if (!openStatus.ok()) {
        return nullptr;
    }

    std::unique_ptr<parquet20::arrow20::FileReader> reader;
    auto readerStatus = builder.Build(&reader);
    if (!readerStatus.ok()) {
        return nullptr;
    }

    std::shared_ptr<arrow20::Schema> schema;
    auto schemaRes = reader->GetSchema(&schema);
    if (!schemaRes.ok()) {
        return nullptr;
    }

    return schema;
}

} // namespace

std::variant<ArrowFields, TString> InferTypes(const std::vector<std::shared_ptr<arrow20::io::InputStream>>& inputs, std::shared_ptr<TFormatConfig> config) {
    if (inputs.empty()) {
        return TString{"no input files"};
    }

    std::vector<std::shared_ptr<arrow20::Schema>> schemas;

    for (auto& input : inputs) {
        std::shared_ptr<arrow20::Schema> schema;

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

bool ArrowToYdbType(Ydb::Type& result, const arrow20::DataType& type, std::shared_ptr<TFormatConfig> config) {
    auto& resType = ShouldBeOptional(type, config) ? *result.mutable_optional_type()->mutable_item() : result;
    switch (type.id()) {
    case arrow20::Type::NA:
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow20::Type::BOOL:
        resType.set_type_id(Ydb::Type::BOOL);
        return true;
    case arrow20::Type::UINT8:
        resType.set_type_id(Ydb::Type::UINT8);
        return true;
    case arrow20::Type::INT8:
        resType.set_type_id(Ydb::Type::INT8);
        return true;
    case arrow20::Type::UINT16:
        resType.set_type_id(Ydb::Type::UINT16);
        return true;
    case arrow20::Type::INT16:
        resType.set_type_id(Ydb::Type::INT16);
        return true;
    case arrow20::Type::UINT32:
        resType.set_type_id(Ydb::Type::UINT32);
        return true;
    case arrow20::Type::INT32:
        resType.set_type_id(Ydb::Type::INT32);
        return true;
    case arrow20::Type::UINT64:
        resType.set_type_id(Ydb::Type::UINT64);
        return true;
    case arrow20::Type::INT64:
        resType.set_type_id(Ydb::Type::INT64);
        return true;
    case arrow20::Type::HALF_FLOAT: // TODO: is there anything?
        return false;
    case arrow20::Type::FLOAT:
        resType.set_type_id(Ydb::Type::FLOAT);
        return true;
    case arrow20::Type::DOUBLE:
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow20::Type::STRING: // TODO: is it true?
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow20::Type::BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow20::Type::FIXED_SIZE_BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow20::Type::DATE32:
        resType.set_type_id(Ydb::Type::DATE);
        return true;
    case arrow20::Type::DATE64: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DATETIME64);
        return true;
    case arrow20::Type::TIMESTAMP:
        if (config->Format == EFileFormat::JsonEachRow || config->Format == EFileFormat::JsonList) {
            result.set_type_id(Ydb::Type::UTF8);
        } else {
            resType.set_type_id(Ydb::Type::TIMESTAMP);
        }
        return true;
    case arrow20::Type::TIME32: // TODO: is there anything?
        return false;
    case arrow20::Type::TIME64: // TODO: is there anything?
        return false;
    case arrow20::Type::INTERVAL_MONTHS: // TODO: is it true?
        return false;
    case arrow20::Type::INTERVAL_DAY_TIME: // TODO: is it true?
        resType.set_type_id(Ydb::Type::INTERVAL64);
        return true;
    case arrow20::Type::DECIMAL128: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow20::Type::DECIMAL256: // TODO: is there anything?
        return false;
    case arrow20::Type::LARGE_LIST: // TODO: is it true?
    case arrow20::Type::FIXED_SIZE_LIST: // TODO: is it true?
    case arrow20::Type::LIST: { // TODO: is ok?
        return false;
    }
    case arrow20::Type::STRUCT:
    case arrow20::Type::SPARSE_UNION:
    case arrow20::Type::DENSE_UNION: {
        return false;
    }
    case arrow20::Type::DICTIONARY: // TODO: is representable?
        return false;
    case arrow20::Type::MAP: { // TODO: is ok?
        return false;
    }
    case arrow20::Type::EXTENSION: // TODO: is representable?
        return false;
    case arrow20::Type::DURATION: // TODO: is it true?
        resType.set_type_id(Ydb::Type::INTERVAL64);
        return true;
    case arrow20::Type::LARGE_STRING: // TODO: is it true?
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow20::Type::LARGE_BINARY: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow20::Type::INTERVAL_MONTH_DAY_NANO: // TODO: is it true?
        resType.set_type_id(Ydb::Type::INTERVAL64);
        return true;
    case arrow20::Type::RUN_END_ENCODED: // TODO: is representable?
        return false;
    case arrow20::Type::STRING_VIEW: // TODO: is it true?
        resType.set_type_id(Ydb::Type::UTF8);
        return true;
    case arrow20::Type::BINARY_VIEW: // TODO: is it true?
        resType.set_type_id(Ydb::Type::STRING);
        return true;
    case arrow20::Type::LIST_VIEW: // TODO: is representable?
        return false;
    case arrow20::Type::LARGE_LIST_VIEW: // TODO: is representable?
        return false;
    case arrow20::Type::DECIMAL32: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow20::Type::DECIMAL64: // TODO: is it true?
        resType.set_type_id(Ydb::Type::DOUBLE);
        return true;
    case arrow20::Type::MAX_ID:
        return false;
    }
    return false;
}

} // namespace NYdb::NArrowInference