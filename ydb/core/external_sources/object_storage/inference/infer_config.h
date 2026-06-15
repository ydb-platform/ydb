#pragma once

#include "arrow_inferencinator.h"

#include <arrow/csv/options.h>
#include <arrow/json/options.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

struct FormatConfig {
    EFileFormat Format = EFileFormat::Undefined;
    bool ShouldMakeOptional = true;

    // CSV / TSV options (used when Format is CsvWithNames, TsvWithNames, or Csv).
    arrow::csv::ParseOptions CsvParseOpts = arrow::csv::ParseOptions::Defaults();
    arrow::csv::ConvertOptions CsvConvOpts = arrow::csv::ConvertOptions::Defaults();

    // JSON options (used when Format is JsonEachRow or JsonList).
    arrow::json::ParseOptions JsonParseOpts = arrow::json::ParseOptions::Defaults();
};

std::shared_ptr<FormatConfig> MakeFormatConfig(const THashMap<TString, TString>& params = {});

} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
