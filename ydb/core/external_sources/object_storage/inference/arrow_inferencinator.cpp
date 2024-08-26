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

bool ShouldBeOptional(const arrow::DataType& type) {
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

bool ArrowToYdbType(Ydb::Type& maybeOptionalType, const arrow::DataType& type) {
    auto& resType = ShouldBeOptional(type) ? *maybeOptionalType.mutable_optional_type()->mutable_item() : maybeOptionalType;
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
        resType.set_type_id(Ydb::Type::TIMESTAMP);
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
    case arrow::Type::STRUCT: { // TODO: is ok?
        auto& structType = *resType.mutable_struct_type();
        for (const auto& field : type.fields()) {
            auto& member = *structType.add_members();
            auto& memberType = *member.mutable_type();
            if (!ArrowToYdbType(memberType, *field->type())) {
                return false;
            }
            member.mutable_name()->assign(field->name().data(), field->name().size());
        }
        return true;
    }
    case arrow::Type::SPARSE_UNION:
    case arrow::Type::DENSE_UNION: { // TODO: is ok?
        auto& variant = *resType.mutable_variant_type()->mutable_struct_items();
        for (const auto& field : type.fields()) {
            auto& member = *variant.add_members();
            if (!ArrowToYdbType(*member.mutable_type(), *field->type())) {
                return false;
            }
            if (field->name().empty()) {
                return false;
            }
            member.mutable_name()->assign(field->name().data(), field->name().size());
        }
        return true;
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

TEvInferredFileSchema* MakeErrorSchema(TString path, NFq::TIssuesIds::EIssueCode code, TString message) {
    NYql::TIssues issues;
    issues.AddIssue(std::move(message));
    issues.back().SetCode(code, NYql::TSeverityIds::S_ERROR);
    return new TEvInferredFileSchema{std::move(path), std::move(issues)};
}

}

namespace {

using ArrowField = std::shared_ptr<arrow::Field>;
using ArrowFields = std::vector<ArrowField>;

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
        return TString{TStringBuilder{} << "couldn't open csv/tsv file, check format and compression params: " << readerStatus.ToString()};
    }

    std::shared_ptr<arrow::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TStringBuilder{} << "couldn't parse csv/tsv file, check format and compression params: " << tableRes.ToString();
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferParquetTypes(std::shared_ptr<arrow::io::RandomAccessFile> file) {
    parquet::arrow::FileReaderBuilder builder;
    builder.properties(parquet::ArrowReaderProperties(false));
    auto openStatus = builder.Open(std::move(file));
    if (!openStatus.ok()) {
        return TStringBuilder{} << "couldn't open parquet file, check format params: " << openStatus.ToString();
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto readerStatus = builder.Build(&reader);
    if (!readerStatus.ok()) {
        return TStringBuilder{} << "couldn't read parquet file, check format params: " << readerStatus.ToString();
    }

    std::shared_ptr<arrow::Schema> schema;
    auto schemaRes = reader->GetSchema(&schema);
    if (!schemaRes.ok()) {
        return TStringBuilder{} << "couldn't parse parquet file, check format params: " << schemaRes.ToString();
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
        return TString{TStringBuilder{} << "couldn't open json file, check format and compression params: " << readerStatus.ToString()};
    }

    std::shared_ptr<arrow::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TString{TStringBuilder{} << "couldn't parse json file, check format and compression params: " << tableRes.ToString()};
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferType(EFileFormat format, std::shared_ptr<arrow::io::RandomAccessFile> file, FormatConfigPtr config) {
    switch (format) {
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
        return std::variant<ArrowFields, TString>{std::in_place_type_t<TString>{}, TStringBuilder{} << "unexpected format: " << ConvertFileFormat(format)};
    }
}

}

class TArrowInferencinator : public NActors::TActorBootstrapped<TArrowInferencinator> {
public:
    TArrowInferencinator(
        NActors::TActorId arrowFetcher,
        EFileFormat format,
        const THashMap<TString, TString>& params)
        : Format_{format}
        , Config_{MakeFormatConfig(Format_, params)}
        , ArrowFetcherId_{arrowFetcher}
    {
        Y_ABORT_UNLESS(IsArrowInferredFormat(Format_));
    }

    void Bootstrap() {
        Become(&TArrowInferencinator::WorkingState);
    }

    STRICT_STFUNC(WorkingState,
        HFunc(TEvInferFileSchema, HandleInferRequest);
        HFunc(TEvInferPartitions, HandlePartitionsRequest);
        HFunc(TEvFileError, HandleFileError);
        HFunc(TEvArrowFile, HandleFileInference);
    )

    void HandleInferRequest(TEvInferFileSchema::TPtr& ev, const NActors::TActorContext& ctx) {
        RequesterId_ = ev->Sender;
        ctx.Send(ArrowFetcherId_, ev->Release());
    }

    void HandlePartitionsRequest(TEvInferPartitions::TPtr& ev, const NActors::TActorContext& ctx) {
        RequesterId_ = ev->Sender;
        auto config = MakeFormatConfig(EFileFormat::CsvWithNames);
        if (std::holds_alternative<TString>(config)) {
            ctx.Send(RequesterId_, MakeErrorSchema("", NFq::TIssuesIds::INTERNAL_ERROR, "couldn't infer partition columns"));
            return;
        }

        InferFileSchema(
            ev->Get()->File,
            "",
            EFileFormat::CsvWithNames,
            std::get<FormatConfigPtr>(config),
            ctx
        );
    }

    void HandleFileInference(TEvArrowFile::TPtr& ev, const NActors::TActorContext& ctx) {
        auto& file = *ev->Get();
        if (std::holds_alternative<TString>(Config_)) {
            ctx.Send(RequesterId_, MakeErrorSchema(file.Path, NFq::TIssuesIds::INTERNAL_ERROR, std::get<TString>(Config_)));
            return;
        }
        
        InferFileSchema(
            file.File,
            file.Path,
            Format_,
            std::get<FormatConfigPtr>(Config_),
            ctx
        );
    }

    void HandleFileError(TEvFileError::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_D("TArrowInferencinator", "HandleFileError: " << ev->Get()->Issues.ToOneLineString());
        ctx.Send(RequesterId_, new TEvInferredFileSchema(ev->Get()->Path, std::move(ev->Get()->Issues)));
    }

private:
    void InferFileSchema(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const TString& path,
        const EFileFormat& format,
        const FormatConfigPtr& config,
        const NActors::TActorContext& ctx) {
        
        auto mbArrowFields = InferType(format, file, config);
        if (std::holds_alternative<TString>(mbArrowFields)) {
            ctx.Send(RequesterId_, MakeErrorSchema(path, NFq::TIssuesIds::INTERNAL_ERROR, std::get<TString>(mbArrowFields)));
            return;
        }
        auto& arrowFields = std::get<ArrowFields>(mbArrowFields);
        std::vector<Ydb::Column> ydbFields;
        for (const auto& field : arrowFields) {
            ydbFields.emplace_back();
            auto& ydbField = ydbFields.back();
            if (!ArrowToYdbType(*ydbField.mutable_type(), *field->type())) {
                ctx.Send(RequesterId_, MakeErrorSchema(path, NFq::TIssuesIds::UNSUPPORTED, TStringBuilder{} << "couldn't convert arrow type to ydb: " << field->ToString()));
                return;
            }
            ydbField.mutable_name()->assign(field->name());
        }
        ctx.Send(RequesterId_, new TEvInferredFileSchema(path, std::move(ydbFields)));
    }

    EFileFormat Format_;
    std::variant<FormatConfigPtr, TString> Config_;
    NActors::TActorId ArrowFetcherId_;
    NActors::TActorId RequesterId_;
};

NActors::IActor* CreateArrowInferencinator(
    NActors::TActorId arrowFetcher,
    EFileFormat format,
    const THashMap<TString, TString>& params) {

    return new TArrowInferencinator{arrowFetcher, format, params};
}
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
