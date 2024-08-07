#include "arrow_inferencinator.h"

#include <arrow/table.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>

#include <ydb/core/external_sources/object_storage/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_value.pb.h>


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
}

struct FormatConfig {
    virtual ~FormatConfig() noexcept = default;
};

struct CsvConfig : public FormatConfig {
    arrow::csv::ParseOptions ParseOpts = arrow::csv::ParseOptions::Defaults();
    arrow::csv::ConvertOptions ConvOpts = arrow::csv::ConvertOptions::Defaults();
};

using TsvConfig = CsvConfig;

namespace {

using ArrowField = std::shared_ptr<arrow::Field>;
using ArrowFields = std::vector<ArrowField>;

std::variant<ArrowFields, TString> InferCsvTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, const CsvConfig& config) {
    std::shared_ptr<arrow::csv::TableReader> reader;
    auto fileSize = static_cast<int32_t>(file->GetSize().ValueOr(1 << 20));
    fileSize = std::min(fileSize, 1 << 20);
    auto readerStatus = arrow::csv::TableReader::Make(
        arrow::io::default_io_context(), std::move(file), arrow::csv::ReadOptions{.use_threads = false, .block_size = fileSize}, config.ParseOpts, config.ConvOpts
    )
    .Value(&reader);

    if (!readerStatus.ok()) {
        return TString{TStringBuilder{} << "couldn't make table from data: " << readerStatus.ToString()};
    }

    std::shared_ptr<arrow::Table> table;
    auto tableRes = reader->Read().Value(&table);

    if (!tableRes.ok()) {
        return TStringBuilder{} << "couldn't read table from data: " << readerStatus.ToString();
    }

    return table->fields();
}

std::variant<ArrowFields, TString> InferType(EFileFormat format, std::shared_ptr<arrow::io::RandomAccessFile> file, const FormatConfig& config) {
    switch (format) {
    case EFileFormat::CsvWithNames:
        return InferCsvTypes(std::move(file), static_cast<const CsvConfig&>(config));
    case EFileFormat::TsvWithNames:
        return InferCsvTypes(std::move(file), static_cast<const TsvConfig&>(config));
    case EFileFormat::Undefined:
    default:
        return std::variant<ArrowFields, TString>{std::in_place_type_t<TString>{}, TStringBuilder{} << "unexpected format: " << ConvertFileFormat(format)};
    }
}

std::unique_ptr<CsvConfig> MakeCsvConfig(const THashMap<TString, TString>&) { // TODO: extract params
    return std::make_unique<CsvConfig>();
}

std::unique_ptr<TsvConfig> MakeTsvConfig(const THashMap<TString, TString>& params) {
    auto config = MakeCsvConfig(params);
    config->ParseOpts.delimiter = '\t';
    return config;
}

std::unique_ptr<FormatConfig> MakeFormatConfig(EFileFormat format, const THashMap<TString, TString>& params) {
    switch (format) {
    case EFileFormat::CsvWithNames:
        return MakeCsvConfig(params);
    case EFileFormat::TsvWithNames:
        return MakeTsvConfig(params);
    case EFileFormat::Undefined:
    default:
        return nullptr;
    }
}

}

class TArrowInferencinator : public NActors::TActorBootstrapped<TArrowInferencinator> {
public:
    TArrowInferencinator(NActors::TActorId arrowFetcher, EFileFormat format, const THashMap<TString, TString>& params)
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
        HFunc(TEvFileError, HandleFileError);
        HFunc(TEvArrowFile, HandleFileInference);
    )

    void HandleInferRequest(TEvInferFileSchema::TPtr& ev, const NActors::TActorContext& ctx) {
        RequesterId_ = ev->Sender;
        ctx.Send(ArrowFetcherId_, ev->Release());
    }

    void HandleFileInference(TEvArrowFile::TPtr& ev, const NActors::TActorContext& ctx) {
        auto& file = *ev->Get();
        auto mbArrowFields = InferType(Format_, file.File, *Config_);
        if (std::holds_alternative<TString>(mbArrowFields)) {
            ctx.Send(RequesterId_, MakeError(file.Path, NFq::TIssuesIds::INTERNAL_ERROR, std::get<TString>(mbArrowFields)));
            return;
        }

        auto& arrowFields = std::get<ArrowFields>(mbArrowFields);
        std::vector<Ydb::Column> ydbFields;
        for (const auto& field : arrowFields) {
            ydbFields.emplace_back();
            auto& ydbField = ydbFields.back();
            if (!ArrowToYdbType(*ydbField.mutable_type(), *field->type())) {
                ctx.Send(RequesterId_, MakeError(file.Path, NFq::TIssuesIds::UNSUPPORTED, TStringBuilder{} << "couldn't convert arrow type to ydb: " << field->ToString()));
                return;
            }
            ydbField.mutable_name()->assign(field->name());
        }
        ctx.Send(RequesterId_, new TEvInferredFileSchema(file.Path, std::move(ydbFields)));
    }

    void HandleFileError(TEvFileError::TPtr& ev, const NActors::TActorContext& ctx) {
        Cout << "TArrowInferencinator::HandleFileError" << Endl;
        ctx.Send(RequesterId_, ev->Release());
    }

private:
    EFileFormat Format_;
    std::unique_ptr<FormatConfig> Config_;
    NActors::TActorId ArrowFetcherId_;
    NActors::TActorId RequesterId_;
};

NActors::IActor* CreateArrowInferencinator(NActors::TActorId arrowFetcher, EFileFormat format, const THashMap<TString, TString>& params) {
    return new TArrowInferencinator{arrowFetcher, format, params};
}
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
