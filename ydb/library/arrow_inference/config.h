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

struct TFormatConfig {
    virtual ~TFormatConfig() noexcept = default;

    EFileFormat Format;
    bool ShouldMakeOptional;
};

struct TCsvConfig : public TFormatConfig {
    arrow::csv::ParseOptions ParseOpts = arrow::csv::ParseOptions::Defaults();
    arrow::csv::ConvertOptions ConvOpts = arrow::csv::ConvertOptions::Defaults();
    arrow::csv::ReadOptions ReadOpts = arrow::csv::ReadOptions::Defaults(); // use_threads and block_size will be rewritten
    int RowsToAnalyze = 0; // 0 means unlimited
};

using TTsvConfig = TCsvConfig;
using TParquetConfig = TFormatConfig;

struct TJsonConfig : public TFormatConfig {
    arrow::json::ParseOptions ParseOpts = arrow::json::ParseOptions::Defaults();
};

// Convert string representation to EFileFormat
EFileFormat ConvertFileFormat(TStringBuf format);

// Convert EFileFormat to string representation
TStringBuf ConvertFileFormat(EFileFormat format);

// Create format config from parameters
std::shared_ptr<TFormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NArrowInference 