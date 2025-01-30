#include "infer_config.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

namespace {

std::shared_ptr<FormatConfig> MakeCsvConfig(const THashMap<TString, TString>& params) {
    auto config = std::make_shared<CsvConfig>();
    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (delimiter->size() != 1) {
            throw yexception() << "invalid parameter: csv_delimiter must be single character";
        }
        config->ParseOpts.delimiter = (*delimiter)[0];
    }
    config->ConvOpts.timestamp_parsers.push_back(arrow::TimestampParser::MakeStrptime("\%Y-\%m-\%d \%H:\%M:\%S"));
    return config;
}

std::shared_ptr<FormatConfig> MakeTsvConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<TsvConfig>();
    config->ParseOpts.delimiter = '\t';
    config->ConvOpts.timestamp_parsers.push_back(arrow::TimestampParser::MakeStrptime("\%Y-\%m-\%d \%H:\%M:\%S"));
    return config;
}

std::shared_ptr<FormatConfig> MakeParquetConfig(const THashMap<TString, TString>&) {
    return std::make_shared<ParquetConfig>();
}

std::shared_ptr<FormatConfig> MakeJsonEachRowConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<JsonConfig>();
    config->ParseOpts.newlines_in_values = true;
    return config;
}

std::shared_ptr<FormatConfig> MakeJsonListConfig(const THashMap<TString, TString>&) {
    return std::make_shared<JsonConfig>();
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
        if (format != EFileFormat::CsvWithNames) {
            throw yexception() << "invalid parameter: csv_delimiter should only be specified for 'csv_with_names' format";
        }
    }

    std::shared_ptr<FormatConfig> config;
    switch (format) {
    case EFileFormat::CsvWithNames:
        config = MakeCsvConfig(params);
        break;
    case EFileFormat::TsvWithNames:
        config = MakeTsvConfig(params);
        break;
    case EFileFormat::Parquet:
        config = MakeParquetConfig(params);
        break;
    case EFileFormat::JsonEachRow:
        config = MakeJsonEachRowConfig(params);
        break;
    case EFileFormat::JsonList:
        config = MakeJsonListConfig(params);
        break;
    case EFileFormat::Undefined:
    default:
        throw yexception() << "invalid parameter: unknown format specified";
    }

    config->Format = format;
    config->ShouldMakeOptional = true;
    return config;
}

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference