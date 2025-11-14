#pragma once

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/csv/options.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/json/options.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>

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
    arrow20::csv::ParseOptions ParseOpts = arrow20::csv::ParseOptions::Defaults();
    arrow20::csv::ConvertOptions ConvOpts = arrow20::csv::ConvertOptions::Defaults();
    arrow20::csv::ReadOptions ReadOpts = arrow20::csv::ReadOptions::Defaults(); // use_threads and block_size will be rewritten
    int RowsToAnalyze = 0; // 0 means unlimited
};

using TTsvConfig = TCsvConfig;
using TParquetConfig = TFormatConfig;

struct TJsonConfig : public TFormatConfig {
    arrow20::json::ParseOptions ParseOpts = arrow20::json::ParseOptions::Defaults();
};

// Convert string representation to EFileFormat
EFileFormat ConvertFileFormat(TStringBuf format);

// Convert EFileFormat to string representation
TStringBuf ConvertFileFormat(EFileFormat format);

// Create format config from parameters
std::shared_ptr<TFormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NArrowInference 