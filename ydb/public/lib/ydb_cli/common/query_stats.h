#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/query_stats/stats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NYdb {
namespace NConsoleClient {

NTable::ECollectQueryStatsMode ParseQueryStatsModeOrThrow(
    const TString& statsMode,
    NTable::ECollectQueryStatsMode defaultMode);

NQuery::EStatsMode ParseQueryStatsModeOrThrow(
    const TString& statsMode,
    NQuery::EStatsMode defaultMode);

} // namespace NConsoleClient
} // namespace NYdb
