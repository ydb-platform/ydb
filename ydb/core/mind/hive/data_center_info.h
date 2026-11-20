#include "hive.h"
#include "hive_events.h"

#include <map>

namespace NKikimr {
namespace NHive {

struct TDataCenterInfo {
    using TFullFollowerGroupId = std::pair<TTabletId, TFollowerGroupId>;
    using TFollowerIter = TList<TFollowerTabletInfo>::iterator; // list iterators are not invalidated

    std::unordered_set<TNodeId> RegisteredNodes;
    bool UpdateScheduled = false;
    std::unordered_map<TFullFollowerGroupId, std::vector<TFollowerIter>> Followers;

    bool IsRegistered() const {
        return !RegisteredNodes.empty();
    }
};

// Tablet counts per data center, used to balance tablets between data centers, see THive::CheckDataCenterBalance
struct TDataCenterBalanceStats {
    struct TDataCenterStat {
        i64 Tablets = 0;
        i64 Nodes = 0; // always positive for an entry
        std::vector<TNodeId> NodeIds;
    };
    std::map<TDataCenterId, TDataCenterStat> DataCenters;

    // (max - min) / max of the tablets per node of the data centers,
    // with `moved` tablets moved from data center `from` to data center `to`
    double GetImbalance(const TDataCenterId& from = {}, const TDataCenterId& to = {}, i64 moved = 0) const;
    // the data centers with the most and the fewest tablets per node
    std::pair<TDataCenterId, TDataCenterId> GetHeaviestAndLightest() const;
    // number of moves from the heaviest data center to the lightest one which keep reducing the imbalance,
    // at most maxMovements (0 means unlimited)
    i64 GetMovementsToBalance(i64 maxMovements) const;
};

} // NHive
} // NKikimr
