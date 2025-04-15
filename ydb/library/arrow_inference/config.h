#pragma once

#include <arrow/csv/options.h>
#include <arrow/json/options.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NYdb::NArrowInference {

enum class EFileFormat {
    Undefined,
    CsvWithNames,
    TsvWithNames,
    JsonEachRow,
    JsonList,
    Parquet,
};

struct FormatConfig {
    virtual ~FormatConfig() noexcept = default;

    EFileFormat Format;
    bool ShouldMakeOptional;
};

struct CsvConfig : public FormatConfig {
    arrow::csv::ParseOptions ParseOpts = arrow::csv::ParseOptions::Defaults();
    arrow::csv::ConvertOptions ConvOpts = arrow::csv::ConvertOptions::Defaults();
};

using TsvConfig = CsvConfig;
using ParquetConfig = FormatConfig;

struct JsonConfig : public FormatConfig {
    arrow::json::ParseOptions ParseOpts = arrow::json::ParseOptions::Defaults();
};

// Convert string representation to EFileFormat
EFileFormat ConvertFileFormat(TStringBuf format);

// Convert EFileFormat to string representation
TStringBuf ConvertFileFormat(EFileFormat format);

// Create format config from parameters
std::shared_ptr<FormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NArrowInference 