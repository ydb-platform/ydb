#pragma once

#include <ydb/library/yql/core/yql_statistics.h>

namespace NYql {

struct TYtColumnStatistic {
    int64_t DataWeight;
};

class TYtProviderStatistic : public IProviderStatistics {
public:
    std::unordered_map<TString, TYtColumnStatistic> ColumnStatistics;
    TVector<TString> SortColumns;
};

}  // namespace NYql
