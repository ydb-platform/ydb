#include "query_stats.h"

#include "common.h"

namespace NYdb {
namespace NConsoleClient {

NTable::ECollectQueryStatsMode ParseQueryStatsMode(const TString& statsMode,
    NTable::ECollectQueryStatsMode defaultMode)
{
    if (statsMode) {
        if (statsMode == "none") {
            return NTable::ECollectQueryStatsMode::None;
        } else if (statsMode == "basic") {
            return NTable::ECollectQueryStatsMode::Basic;
        } else if (statsMode == "full") {
            return NTable::ECollectQueryStatsMode::Full;
        } else {
            throw TMisuseException() << "Unknown stats collection mode.";
        }
    }

    return defaultMode;
}

} // namespace NConsoleClient
} // namespace NYdb
