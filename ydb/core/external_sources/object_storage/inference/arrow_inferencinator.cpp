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

#include <util/string/join.h>

#include <optional>

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

using ArrowField = std::shared_ptr<arrow::Field>;
using ArrowFields = std::vector<ArrowField>;

bool ShouldBeOptional(const arrow::DataType& type, const std::shared_ptr<FormatConfig>& config) {
    if (!config->ShouldMakeOptional) {
        return false;
    }

    // For JSON formats, TIMESTAMP is reinterpreted as UTF8 (see MapPrimitiveType),
    // so it should not be wrapped in Optional either.
    if (type.id() == arrow::Type::TIMESTAMP &&
        (config->Format == EFileFormat::JsonEachRow || config->Format == EFileFormat::JsonList)) {
        return false;
    }

    switch (type.id()) {
    case arrow::Type::NA:
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
        return false;
    default:
        return true;
    }
}

// Direct, context-free arrow→ydb primitive type mapping. Special cases (formats,
// non-mappable / unsupported types) are handled in MapPrimitiveType below.
constexpr std::pair<arrow::Type::type, Ydb::Type::PrimitiveTypeId> kPrimitiveTypeTable[] = {
    {arrow::Type::NA,                Ydb::Type::UTF8},
    {arrow::Type::BOOL,              Ydb::Type::BOOL},
    {arrow::Type::UINT8,             Ydb::Type::UINT8},
    {arrow::Type::INT8,              Ydb::Type::INT8},
    {arrow::Type::UINT16,            Ydb::Type::UINT16},
    {arrow::Type::INT16,             Ydb::Type::INT16},
    {arrow::Type::UINT32,            Ydb::Type::UINT32},
    {arrow::Type::INT32,             Ydb::Type::INT32},
    {arrow::Type::UINT64,            Ydb::Type::UINT64},
    {arrow::Type::INT64,             Ydb::Type::INT64},
    {arrow::Type::FLOAT,             Ydb::Type::FLOAT},
    {arrow::Type::DOUBLE,            Ydb::Type::DOUBLE},
    {arrow::Type::STRING,            Ydb::Type::UTF8},
    {arrow::Type::LARGE_STRING,      Ydb::Type::UTF8},
    {arrow::Type::BINARY,            Ydb::Type::STRING},
    {arrow::Type::LARGE_BINARY,      Ydb::Type::STRING},
    {arrow::Type::DATE32,            Ydb::Type::DATE},
    {arrow::Type::DATE64,            Ydb::Type::DATETIME64},
    {arrow::Type::DECIMAL128,        Ydb::Type::DOUBLE},
};

std::optional<Ydb::Type::PrimitiveTypeId> MapPrimitiveType(const arrow::DataType& type, const FormatConfig& config) {
    // TIMESTAMP is special: for JSON formats arrow infers everything as a UTF8 string,
    // so we keep it as UTF8; for CSV/Parquet we map to TIMESTAMP.
    if (type.id() == arrow::Type::TIMESTAMP) {
        if (config.Format == EFileFormat::JsonEachRow || config.Format == EFileFormat::JsonList) {
            return Ydb::Type::UTF8;
        }
        return Ydb::Type::TIMESTAMP;
    }

    for (const auto& [arrowId, ydbId] : kPrimitiveTypeTable) {
        if (arrowId == type.id()) {
            return ydbId;
        }
    }
    return std::nullopt;
}

bool ArrowToYdbType(Ydb::Type& maybeOptionalType, const arrow::DataType& type, const std::shared_ptr<FormatConfig>& config) {
    auto mapped = MapPrimitiveType(type, *config);
    if (!mapped) {
        return false;
    }
    auto& resType = ShouldBeOptional(type, config)
        ? *maybeOptionalType.mutable_optional_type()->mutable_item()
        : maybeOptionalType;
    resType.set_type_id(*mapped);
    return true;
}

TEvInferredFileSchema* MakeErrorSchema(TString path, NFq::TIssuesIds::EIssueCode code, TString message) {
    NYql::TIssues issues;
    issues.AddIssue(std::move(message));
    issues.back().SetCode(code, NYql::TSeverityIds::S_ERROR);
    return new TEvInferredFileSchema{std::move(path), std::move(issues)};
}

// Common skeleton for arrow CSV / JSON readers: validate file size, build a reader,
// read the table, return its fields. The reader is constructed by `makeReader`,
// which receives the file size hint and returns an arrow::Result with the reader.
template <class MakeReader>
std::variant<ArrowFields, TString> ReadFields(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::string_view errorHeader,
    MakeReader makeReader)
{
    int64_t fileSize = 0;
    if (auto sizeStatus = file->GetSize().Value(&fileSize); !sizeStatus.ok()) {
        return TStringBuilder{} << errorHeader << "couldn't get file size: " << sizeStatus.ToString();
    }
    if (fileSize <= 0 || fileSize > INT32_MAX) {
        return TStringBuilder{} << errorHeader << "empty file";
    }

    auto readerResult = makeReader(std::move(file), static_cast<int32_t>(fileSize));
    if (!readerResult.ok()) {
        return TStringBuilder{} << errorHeader << readerResult.status().ToString();
    }
    auto reader = *std::move(readerResult);

    std::shared_ptr<arrow::Table> table;
    if (auto tableRes = reader->Read().Value(&table); !tableRes.ok()) {
        return TStringBuilder{} << errorHeader << tableRes.ToString();
    }
    return table->fields();
}

std::variant<ArrowFields, TString> InferCsvTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, const std::shared_ptr<FormatConfig>& config) {
    return ReadFields(
        std::move(file),
        "couldn't open csv/tsv file, check format and compression parameters: "sv,
        [&](std::shared_ptr<arrow::io::RandomAccessFile> f, int32_t blockSize) {
            return arrow::csv::TableReader::Make(
                arrow::io::default_io_context(),
                std::move(f),
                arrow::csv::ReadOptions{.use_threads = false, .block_size = blockSize},
                config->CsvParseOpts,
                config->CsvConvOpts);
        });
}

std::variant<ArrowFields, TString> InferJsonTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, const std::shared_ptr<FormatConfig>& config) {
    return ReadFields(
        std::move(file),
        "couldn't open json file, check format and compression parameters: "sv,
        [&](std::shared_ptr<arrow::io::RandomAccessFile> f, int32_t blockSize) {
            return arrow::json::TableReader::Make(
                arrow::default_memory_pool(),
                std::move(f),
                arrow::json::ReadOptions{.use_threads = false, .block_size = blockSize},
                config->JsonParseOpts);
        });
}

std::variant<ArrowFields, TString> InferParquetTypes(std::shared_ptr<arrow::io::RandomAccessFile> file) {
    constexpr auto errorHeader = "couldn't open parquet file, check format parameters: "sv;
    parquet::arrow::FileReaderBuilder builder;
    builder.properties(parquet::ArrowReaderProperties(false));
    if (auto openStatus = builder.Open(std::move(file)); !openStatus.ok()) {
        return TStringBuilder{} << errorHeader << openStatus.ToString();
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    if (auto readerStatus = builder.Build(&reader); !readerStatus.ok()) {
        return TStringBuilder{} << errorHeader << readerStatus.ToString();
    }

    std::shared_ptr<arrow::Schema> schema;
    if (auto schemaRes = reader->GetSchema(&schema); !schemaRes.ok()) {
        return TStringBuilder{} << errorHeader << schemaRes.ToString();
    }
    return schema->fields();
}

std::variant<ArrowFields, TString> InferType(std::shared_ptr<arrow::io::RandomAccessFile> file, const std::shared_ptr<FormatConfig>& config) {
    switch (config->Format) {
    case EFileFormat::CsvWithNames:
    case EFileFormat::TsvWithNames:
    case EFileFormat::Csv:
        return InferCsvTypes(std::move(file), config);
    case EFileFormat::Parquet:
        return InferParquetTypes(std::move(file));
    case EFileFormat::JsonEachRow:
    case EFileFormat::JsonList:
        return InferJsonTypes(std::move(file), config);
    case EFileFormat::Undefined:
    default:
        return TStringBuilder{} << "unexpected format: " << ConvertFileFormat(config->Format);
    }
}

} // namespace

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
        if (auto* err = std::get_if<TString>(&mbArrowFields)) {
            ReplyAndReset(ctx, MakeErrorSchema(file.Path, NFq::TIssuesIds::INTERNAL_ERROR, std::move(*err)));
            return;
        }

        auto& arrowFields = std::get<ArrowFields>(mbArrowFields);
        std::vector<Ydb::Column> ydbFields;
        ydbFields.reserve(arrowFields.size());
        size_t skippedEmptyName = 0;
        std::vector<TString> skippedUnsupported;
        for (const auto& field : arrowFields) {
            if (field->name().empty()) {
                ++skippedEmptyName;
                continue;
            }
            Ydb::Column column;
            if (!ArrowToYdbType(*column.mutable_type(), *field->type(), file.Config)) {
                skippedUnsupported.push_back(TStringBuilder() << field->name() << " (" << field->type()->ToString() << ")");
                continue;
            }
            column.mutable_name()->assign(field->name());
            ydbFields.push_back(std::move(column));
        }

        if (ydbFields.empty()) {
            TStringBuilder err;
            err << "couldn't infer schema: no usable columns found";
            if (arrowFields.empty()) {
                err << " (file has no columns)";
            } else {
                err << " (got " << arrowFields.size() << " arrow field(s)";
                if (skippedEmptyName) {
                    err << ", " << skippedEmptyName << " with empty name";
                }
                if (!skippedUnsupported.empty()) {
                    err << ", unsupported types: [" << JoinSeq(", ", skippedUnsupported) << "]";
                }
                err << ")";
            }
            ReplyAndReset(ctx, MakeErrorSchema(file.Path, NFq::TIssuesIds::INTERNAL_ERROR, err));
            return;
        }

        ReplyAndReset(ctx, new TEvInferredFileSchema(file.Path, std::move(ydbFields)));
    }

    void HandleFileError(TEvFileError::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_D("TArrowInferencinator", "HandleFileError: " << ev->Get()->Issues.ToOneLineString());
        ReplyAndReset(ctx, new TEvInferredFileSchema(ev->Get()->Path, std::move(ev->Get()->Issues)));
    }

private:
    void ReplyAndReset(const NActors::TActorContext& ctx, TEvInferredFileSchema* event) {
        ctx.Send(RequesterId_, event);
        RequesterId_ = {};
    }

    NActors::TActorId ArrowFetcherId_;
    NActors::TActorId RequesterId_;
};

NActors::IActor* CreateArrowInferencinator(NActors::TActorId arrowFetcher) {
    return new TArrowInferencinator{arrowFetcher};
}
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
