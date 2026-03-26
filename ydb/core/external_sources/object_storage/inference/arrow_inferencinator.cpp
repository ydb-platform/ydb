#include "arrow_inferencinator.h"
#include "infer_config.h"

#include <arrow/table.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <arrow/json/options.h>
#include <arrow/json/reader.h>
#include <parquet/arrow/reader.h>

#include <ydb/core/external_sources/object_storage/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#define LOG_E(name, stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::OBJECT_STORAGE_INFERENCINATOR, name << ": " << this->SelfId() << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::OBJECT_STORAGE_INFERENCINATOR, name << ": " << this->SelfId() << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::OBJECT_STORAGE_INFERENCINATOR, name << ": " << this->SelfId() << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::OBJECT_STORAGE_INFERENCINATOR, name << ": " << this->SelfId() << ". " << stream)

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

namespace {

bool ShouldBeOptional(const arrow20::DataType& type, std::shared_ptr<FormatConfig> config) {
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

bool ArrowToYdbType(Ydb::Type& maybeOptionalType, const arrow20::DataType& type, std::shared_ptr<FormatConfig> config) {
    auto& resType = ShouldBeOptional(type, config) ? *maybeOptionalType.mutable_optional_type()->mutable_item() : maybeOptionalType;
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
            maybeOptionalType.set_type_id(Ydb::Type::UTF8);
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
    case arrow20::Type::MAX_ID:
        return false;
    }
    return false;
}

TEvInferredFileSchema* MakeErrorSchema(TString path, NFq::TIssuesIds::EIssueCode code, TString message) {
    NYql::TIssues issues;
    issues.AddIssue(std::move(message));
    issues.back().SetCode(code, NYql::TSeverityIds::S_ERROR);
    return new TEvInferredFileSchema{std::move(path), std::move(issues)};
}

}

namespace {

using ArrowField = std::shared_ptr<arrow20::Field>;
using ArrowFields = std::vector<ArrowField>;

std::variant<ArrowFields, TString> InferCsvTypes(std::shared_ptr<arrow20::io::RandomAccessFile> file, std::shared_ptr<CsvConfig> config) {
    int64_t fileSize;
    constexpr auto errorHeader = "couldn't open csv/tsv file, check format and compression parameters: "sv;
    if (auto sizeStatus = file->GetSize().Value(&fileSize); !sizeStatus.ok()) {
        return TStringBuilder{} << errorHeader << "coudn't get file size: " << sizeStatus.ToString();
    }
    if (fileSize <= 0 || fileSize > INT32_MAX) {
        return TStringBuilder{} << errorHeader << "empty file";
    }

    std::shared_ptr<arrow20::csv::TableReader> reader;
    auto readerStatus = arrow20::csv::TableReader::Make(
        arrow20::io::default_io_context(),
        std::move(file),
        arrow20::csv::ReadOptions{.use_threads = false, .block_size = static_cast<int32_t>(fileSize)},
        config->ParseOpts,
        config->ConvOpts
    )
    .Value(&reader);

    if (!readerStatus.ok()) {
        return TString{TStringBuilder{} << errorHeader << readerStatus.ToString()};
    }

    std::shared_ptr<arrow20::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TStringBuilder{} << errorHeader << tableRes.ToString();
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferParquetTypes(std::shared_ptr<arrow20::io::RandomAccessFile> file) {
    constexpr auto errorHeader = "couldn't open parquet file, check format parameters: "sv;
    parquet::arrow20::FileReaderBuilder builder;
    builder.properties(parquet::ArrowReaderProperties(false));
    auto openStatus = builder.Open(std::move(file));
    if (!openStatus.ok()) {
        return TStringBuilder{} << errorHeader << openStatus.ToString();
    }

    std::unique_ptr<parquet::arrow20::FileReader> reader;
    auto readerStatus = builder.Build(&reader);
    if (!readerStatus.ok()) {
        return TStringBuilder{} << errorHeader << readerStatus.ToString();
    }

    std::shared_ptr<arrow20::Schema> schema;
    auto schemaRes = reader->GetSchema(&schema);
    if (!schemaRes.ok()) {
        return TStringBuilder{} << errorHeader << schemaRes.ToString();
    }

    return schema->fields();
}

std::variant<ArrowFields, TString> InferJsonTypes(std::shared_ptr<arrow20::io::RandomAccessFile> file, std::shared_ptr<JsonConfig> config) {
    constexpr auto errorHeader = "couldn't open json file, check format and compression parameters: "sv;
    int64_t fileSize;
    if (auto sizeStatus = file->GetSize().Value(&fileSize); !sizeStatus.ok()) {
        return TStringBuilder{} << errorHeader << "coudn't get file size: " << sizeStatus.ToString();
    }
    if (fileSize <= 0 || fileSize > INT32_MAX) {
        return TStringBuilder{} << errorHeader << "empty file";
    }
    std::shared_ptr<arrow20::json::TableReader> reader;
    auto readerStatus = arrow20::json::TableReader::Make(
        arrow20::default_memory_pool(),
        std::move(file),
        arrow20::json::ReadOptions{.use_threads = false, .block_size = static_cast<int32_t>(fileSize)},
        config->ParseOpts
    ).Value(&reader);

    if (!readerStatus.ok()) {
        return TString{TStringBuilder{} << errorHeader << readerStatus.ToString()};
    }

    std::shared_ptr<arrow20::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TString{TStringBuilder{} << errorHeader << tableRes.ToString()};
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferType(std::shared_ptr<arrow20::io::RandomAccessFile> file, std::shared_ptr<FormatConfig> config) {
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

}

class TArrowInferencinator : public NActors::TActorBootstrapped<TArrowInferencinator> {
public:
    TArrowInferencinator(NActors::TActorId arrowFetcher)
        : ArrowFetcherId_{arrowFetcher}
    {}

    void Bootstrap() {
        Become(&TArrowInferencinator::WorkingState);
    }

    STRICT_STFUNC(WorkingState,
        HFunc(TEvInferFileSchema, HandleInferRequest);
        HFunc(TEvFileError, HandleFileError);
        HFunc(TEvArrowFile, HandleFileInference);
    )

    void HandleInferRequest(TEvInferFileSchema::TPtr& ev, const NActors::TActorContext& ctx) {
        RequesterId_ = ev->Sender;
        ctx.Send(ArrowFetcherId_, ev->Release());
    }

    void HandleFileInference(TEvArrowFile::TPtr& ev, const NActors::TActorContext& ctx) {
        if (!RequesterId_) {
            RequesterId_ = ev->Sender;
        }

        auto& file = *ev->Get();
        auto mbArrowFields = InferType(file.File, file.Config);
        if (std::holds_alternative<TString>(mbArrowFields)) {
            ctx.Send(RequesterId_, MakeErrorSchema(file.Path, NFq::TIssuesIds::INTERNAL_ERROR, std::get<TString>(mbArrowFields)));
            RequesterId_ = {};
            return;
        }
        auto& arrowFields = std::get<ArrowFields>(mbArrowFields);
        std::vector<Ydb::Column> ydbFields;
        for (const auto& field : arrowFields) {
            Ydb::Column column;
            if (!ArrowToYdbType(*column.mutable_type(), *field->type(), file.Config)) {
                continue;
            }
            if (field->name().empty()) {
                continue;
            }
            column.mutable_name()->assign(field->name());
            ydbFields.push_back(column);
        }

        ctx.Send(RequesterId_, new TEvInferredFileSchema(file.Path, std::move(ydbFields)));
        RequesterId_ = {};
    }

    void HandleFileError(TEvFileError::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_D("TArrowInferencinator", "HandleFileError: " << ev->Get()->Issues.ToOneLineString());
        ctx.Send(RequesterId_, new TEvInferredFileSchema(ev->Get()->Path, std::move(ev->Get()->Issues)));
    }

private:
    NActors::TActorId ArrowFetcherId_;
    NActors::TActorId RequesterId_;
};

NActors::IActor* CreateArrowInferencinator(NActors::TActorId arrowFetcher) {
    return new TArrowInferencinator{arrowFetcher};
}
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
