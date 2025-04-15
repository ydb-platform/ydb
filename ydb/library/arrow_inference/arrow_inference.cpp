#include "arrow_inference.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

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

bool ShouldBeOptional(const arrow::DataType& type, std::shared_ptr<FormatConfig> config) {
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

std::variant<ArrowFields, TString> InferCsvTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<CsvConfig> config) {
    int64_t fileSize;
    if (auto sizeStatus = file->GetSize().Value(&fileSize); !sizeStatus.ok()) {
        return TStringBuilder{} << "coudn't get file size: " << sizeStatus.ToString();
    }

    std::shared_ptr<arrow::csv::TableReader> reader;
    auto readerStatus = arrow::csv::TableReader::Make(
        arrow::io::default_io_context(),
        std::move(file),
        arrow::csv::ReadOptions{.use_threads = false, .block_size = static_cast<int32_t>(fileSize)},
        config->ParseOpts,
        config->ConvOpts
    )
    .Value(&reader);

    if (!readerStatus.ok()) {
        return TString{TStringBuilder{} << "couldn't open csv/tsv file, check format and compression parameters: " << readerStatus.ToString()};
    }

    std::shared_ptr<arrow::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TStringBuilder{} << "couldn't parse csv/tsv file, check format and compression parameters: " << tableRes.ToString();
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferParquetTypes(std::shared_ptr<arrow::io::RandomAccessFile> file) {
    parquet::arrow::FileReaderBuilder builder;
    builder.properties(parquet::ArrowReaderProperties(false));
    auto openStatus = builder.Open(std::move(file));
    if (!openStatus.ok()) {
        return TStringBuilder{} << "couldn't open parquet file, check format parameters: " << openStatus.ToString();
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto readerStatus = builder.Build(&reader);
    if (!readerStatus.ok()) {
        return TStringBuilder{} << "couldn't read parquet file, check format parameters: " << readerStatus.ToString();
    }

    std::shared_ptr<arrow::Schema> schema;
    auto schemaRes = reader->GetSchema(&schema);
    if (!schemaRes.ok()) {
        return TStringBuilder{} << "couldn't parse parquet file, check format parameters: " << schemaRes.ToString();
    }

    return schema->fields();
}

std::variant<ArrowFields, TString> InferJsonTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<JsonConfig> config) {
    int64_t fileSize;
    if (auto sizeStatus = file->GetSize().Value(&fileSize); !sizeStatus.ok()) {
        return TStringBuilder{} << "coudn't get file size: " << sizeStatus.ToString();
    }

    std::shared_ptr<arrow::json::TableReader> reader;
    auto readerStatus = arrow::json::TableReader::Make(
        arrow::default_memory_pool(),
        std::move(file),
        arrow::json::ReadOptions{.use_threads = false, .block_size = static_cast<int32_t>(fileSize)},
        config->ParseOpts
    ).Value(&reader);

    if (!readerStatus.ok()) {
        return TString{TStringBuilder{} << "couldn't open json file, check format and compression parameters: " << readerStatus.ToString()};
    }

    std::shared_ptr<arrow::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TString{TStringBuilder{} << "couldn't parse json file, check format and compression parameters: " << tableRes.ToString()};
    }

    return table->fields();
}

} // namespace

std::variant<ArrowFields, TString> InferTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<FormatConfig> config) {
    switch (config->Format) {
    case EFileFormat::CsvWithNames:
        return InferCsvTypes(std::move(file), std::dynamic_pointer_cast<CsvConfig>(config));
    case EFileFormat::TsvWithNames:
        return InferCsvTypes(std::move(file), std::dynamic_pointer_cast<TsvConfig>(config));
    case EFileFormat::Parquet:
        return InferParquetTypes(std::move(file));
    case EFileFormat::JsonEachRow:
    case EFileFormat::JsonList:
        return InferJsonTypes(std::move(file), std::dynamic_pointer_cast<JsonConfig>(config));
    case EFileFormat::Undefined:
    default:
        return TStringBuilder{} << "unexpected format: " << ConvertFileFormat(config->Format);
    }
}

bool ArrowToYdbType(Ydb::Type& maybeOptionalType, const arrow::DataType& type, std::shared_ptr<FormatConfig> config) {
    auto& resType = ShouldBeOptional(type, config) ? *maybeOptionalType.mutable_optional_type()->mutable_item() : maybeOptionalType;
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
            maybeOptionalType.set_type_id(Ydb::Type::UTF8);
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

} // namespace NYdb 