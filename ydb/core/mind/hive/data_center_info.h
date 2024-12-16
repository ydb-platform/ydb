#include "hive.h"
#include "hive_events.h"

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

} // NHive
} // NKikimr
