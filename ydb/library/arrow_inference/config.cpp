#include "config.h"

#include <util/string/split.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>

namespace NYdb::NArrowInference {

namespace {

std::shared_ptr<TFormatConfig> MakeCsvConfig(const THashMap<TString, TString>& params) {
    auto config = std::make_shared<TCsvConfig>();
    if (auto delimiter = params.FindPtr("csvdelimiter"); delimiter) {
        if (delimiter->size() != 1) {
            throw yexception() << "invalid parameter: csv_delimiter must be single character";
        }
        config->ParseOpts.delimiter = (*delimiter)[0];
    }
    return config;
}

std::shared_ptr<TFormatConfig> MakeTsvConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<TTsvConfig>();
    config->ParseOpts.delimiter = '\t';
    return config;
}

std::shared_ptr<TFormatConfig> MakeParquetConfig(const THashMap<TString, TString>&) {
    return std::make_shared<TParquetConfig>();
}

std::shared_ptr<TFormatConfig> MakeJsonEachRowConfig(const THashMap<TString, TString>&) {
    auto config = std::make_shared<TJsonConfig>();
    config->ParseOpts.newlines_in_values = true;
    return config;
}

std::shared_ptr<TFormatConfig> MakeJsonListConfig(const THashMap<TString, TString>&) {
    return std::make_shared<TJsonConfig>();
}

} // namespace

EFileFormat ConvertFileFormat(TStringBuf format) {
    if (format == "csv_with_names") {
        return EFileFormat::CsvWithNames;
    }
    if (format == "tsv_with_names") {
        return EFileFormat::TsvWithNames;
    }
    if (format == "json_each_row") {
        return EFileFormat::JsonEachRow;
    }
    if (format == "json_list") {
        return EFileFormat::JsonList;
    }
    if (format == "parquet") {
        return EFileFormat::Parquet;
    }
    return EFileFormat::Undefined;
}

TStringBuf ConvertFileFormat(EFileFormat format) {
    switch (format) {

    case EFileFormat::CsvWithNames:
        return "csv_with_names";
    case EFileFormat::TsvWithNames:
        return "tsv_with_names";
    case EFileFormat::JsonEachRow:
        return "json_each_row";
    case EFileFormat::JsonList:
        return "json_list";
    case EFileFormat::Parquet:
      return "parquet";
    case EFileFormat::Undefined:
    default:
        return "UNSUPPORTED";
    }
}

std::shared_ptr<TFormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params) {
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

    std::shared_ptr<TFormatConfig> config;
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
    default:
        throw yexception() << "invalid parameter: unknown format specified";
    }

    config->Format = format;
    config->ShouldMakeOptional = true;
    return config;
}

} // namespace NArrowInference 