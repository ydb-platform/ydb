#include "query_stats.h"
#include <iostream>

#include "common.h"

namespace NYdb {
namespace NConsoleClient {

NTable::ECollectQueryStatsMode ParseQueryStatsModeOrThrow(const TString& statsMode,
    NTable::ECollectQueryStatsMode defaultMode)
{
    if (!statsMode.empty()) {
        auto stats = NTable::ParseQueryStatsMode({statsMode.data(), statsMode.size()});
        if (stats) {
            return *stats;
        }

        throw TMisuseException() << "Unknown stats collection mode " + statsMode + '.';
    }

    return defaultMode;
}

} // namespace NConsoleClient
} // namespace NYdb
