#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/query_stats/stats.h>

namespace NYdb {
namespace NConsoleClient {

NTable::ECollectQueryStatsMode ParseQueryStatsMode(const TString& statsMode,
    NTable::ECollectQueryStatsMode defaultMode);

} // namespace NConsoleClient
} // namespace NYdb
