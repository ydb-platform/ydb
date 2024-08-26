#include "infer_config.h"

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

namespace {

std::variant<FormatConfigPtr, TString> MakeCsvConfig(const THashMap<TString, TString>& params) {
    auto config = std::make_shared<CsvConfig>();
    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (delimiter->Size() != 1) {
            return "csv_delimiter must be single character";
        }
        config->ParseOpts.delimiter = (*delimiter)[0];
    }
    return config;
}

std::variant<FormatConfigPtr, TString> MakeTsvConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<TsvConfig>();
    config->ParseOpts.delimiter = '\t';
    return config;
}

std::variant<FormatConfigPtr, TString> MakeParquetConfig(const THashMap<TString, TString>&) {
    return std::make_shared<ParquetConfig>();
}

std::variant<FormatConfigPtr, TString> MakeJsonEachRowConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<JsonConfig>();
    config->ParseOpts.newlines_in_values = true;
    return config;
}

std::variant<FormatConfigPtr, TString> MakeJsonListConfig(const THashMap<TString, TString>&) {
    return std::make_shared<JsonConfig>();
}

}

std::variant<FormatConfigPtr, TString> MakeFormatConfig(EFileFormat format, const THashMap<TString, TString>& params) {
    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (format != EFileFormat::CsvWithNames) {
            return "csv_delimiter should only be specified for 'csv_with_names' format";
        }
    }

    switch (format) {
    case EFileFormat::CsvWithNames:
        return MakeCsvConfig(params);
    case EFileFormat::TsvWithNames:
        return MakeTsvConfig(params);
    case EFileFormat::Parquet:
        return MakeParquetConfig(params);
    case EFileFormat::JsonEachRow:
        return MakeJsonEachRowConfig(params);
    case EFileFormat::JsonList:
        return MakeJsonListConfig(params);
    case EFileFormat::Undefined:
    default:
        return "unknown format specified";
    }
}

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference