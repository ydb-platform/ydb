#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/stats.h>

class TDuration;

namespace Ydb {
    namespace TableStats {
        class QueryStats;
    }

    namespace Table {
        class QueryStatsCollection;
    }
}

namespace NYdb::inline Dev {

class TProtoAccessor;

namespace NScripting {

class TScriptingClient;
class TYqlResultPartIterator;

} // namespace NScripting

namespace NTable {

enum class ECollectQueryStatsMode {
    None = 0,  // Stats collection is disabled
    Basic = 1, // Aggregated stats of reads, updates and deletes per table
    Full = 2,   // Add per-stage execution profile, query plan and query meta on top of Basic mode
    Profile = 3   // Detailed execution stats including stats for individual tasks and channels
};

using TQueryStats = NQuery::TExecStats;

std::optional<ECollectQueryStatsMode> ParseQueryStatsMode(std::string_view statsMode);

std::string_view QueryStatsModeToString(ECollectQueryStatsMode statsMode);

} // namespace NTable
} // namespace NYdb
