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
    arrow20::csv::ParseOptions ParseOpts = arrow20::csv::ParseOptions::Defaults();
    arrow20::csv::ConvertOptions ConvOpts = arrow20::csv::ConvertOptions::Defaults();
};

using TsvConfig = CsvConfig;
using ParquetConfig = FormatConfig;

struct JsonConfig : public FormatConfig {
    arrow20::json::ParseOptions ParseOpts = arrow20::json::ParseOptions::Defaults();
};

std::shared_ptr<FormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference