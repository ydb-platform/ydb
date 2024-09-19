#pragma once

#include <ydb/library/yql/core/yql_statistics.h>
#include <map>

namespace NYql {

struct TS3ProviderStatistics : public IProviderStatistics {
    ui64 RawByteSize = 0;
    bool OverrideApplied = false;
    double FullRawRowAvgSize = 0.0;
    double FullDecodedRowAvgSize = 0.0;
    double PrunedRawRowAvgSize = 0.0;
    double PrunedDecodedRowAvgSize = 0.0;
    TString Format;
    TString Compression;
    std::unordered_map<ui64, double> Costs;
};

} // namespace NYql
