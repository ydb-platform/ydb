#pragma once

#include "arrow_inferencinator.h"

#include <arrow/csv/options.h>
#include <arrow/json/options.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

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

std::shared_ptr<FormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference