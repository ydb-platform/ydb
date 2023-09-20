#include "stats.h"

namespace NYdb {
namespace NTable {

std::optional<ECollectQueryStatsMode> ParseQueryStatsMode(std::string_view statsMode)
{
    if (statsMode == "none") {
        return ECollectQueryStatsMode::None;
    } else if (statsMode == "basic") {
        return ECollectQueryStatsMode::Basic;
    } else if (statsMode == "full") {
        return ECollectQueryStatsMode::Full;
    } else if (statsMode == "profile") {
        return ECollectQueryStatsMode::Profile;
    }

    return {};
}

std::string_view QueryStatsModeToString(ECollectQueryStatsMode statsMode)
{
    switch (statsMode) {
    case ECollectQueryStatsMode::None:
        return "none";
    case ECollectQueryStatsMode::Basic:
        return "basic";
    case ECollectQueryStatsMode::Full:
        return "full";
    case ECollectQueryStatsMode::Profile:
        return "profile";
    }
}

} // namespace NTable
} // namespace NYdb
