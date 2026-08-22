#include "infer_config.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

namespace {

void ConfigureCsv(FormatConfig& config, const THashMap<TString, TString>& params) {
    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (delimiter->size() != 1) {
            throw yexception() << "invalid parameter: csv_delimiter must be single character";
        }
        config.CsvParseOpts.delimiter = (*delimiter)[0];
    }
    config.CsvConvOpts.timestamp_parsers.push_back(arrow::TimestampParser::MakeStrptime("\%Y-\%m-\%d \%H:\%M:\%S"));
}

void ConfigureTsv(FormatConfig& config) {
    config.CsvParseOpts.delimiter = '\t';
    config.CsvConvOpts.timestamp_parsers.push_back(arrow::TimestampParser::MakeStrptime("\%Y-\%m-\%d \%H:\%M:\%S"));
}

void ConfigureJsonEachRow(FormatConfig& config) {
    config.JsonParseOpts.newlines_in_values = true;
}

}

std::shared_ptr<FormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params) {
    static THashSet<TString> supportedParams {
        "format",
        "compression",
        "filepattern",
        "partitionedby",
        "projection",
        "csvdelimiter",
    };

    for (const auto& [param, value] : params) {
        if (!supportedParams.contains(param)) {
            throw yexception() << "parameter is not supported with type inference: " << param;
        }
    }

    EFileFormat format;
    if (auto formatPtr = params.FindPtr("format"); formatPtr) {
        format = ConvertFileFormat(*formatPtr);
    } else {
        throw yexception() << "format unspecified, use format parameter with type inferring";
    }

    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (format != EFileFormat::CsvWithNames && format != EFileFormat::Csv) {
            throw yexception() << "invalid parameter: csv_delimiter should only be specified for 'csv_with_names' or 'csv' format";
        }
    }

    auto config = std::make_shared<FormatConfig>();
    config->Format = format;
    config->ShouldMakeOptional = true;

    switch (format) {
    case EFileFormat::CsvWithNames:
    case EFileFormat::Csv:
        ConfigureCsv(*config, params);
        break;
    case EFileFormat::TsvWithNames:
        ConfigureTsv(*config);
        break;
    case EFileFormat::Parquet:
        // No configuration needed.
        break;
    case EFileFormat::JsonEachRow:
        ConfigureJsonEachRow(*config);
        break;
    case EFileFormat::JsonList:
        // Defaults are sufficient.
        break;
    case EFileFormat::Undefined:
    default:
        throw yexception() << "invalid parameter: unknown format specified";
    }

    return config;
}

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
