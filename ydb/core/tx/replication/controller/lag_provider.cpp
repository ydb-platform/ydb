#include "lag_provider.h"

namespace NKikimr::NReplication::NController {

void TLagProvider::AddPendingLag(ui64 childId) {
    Pending.insert(childId);
}

bool TLagProvider::UpdateLag(TItemWithLag& child, ui64 childId, TDuration lag) {
    bool updated = false;

    if (const auto& prevLag = child.Lag) {
        auto it = ChildrenByLag.find(*prevLag);
        Y_ABORT_UNLESS(it != ChildrenByLag.end());

        it->second.erase(childId);
        if (it->second.empty()) {
            updated = true;
            ChildrenByLag.erase(it);
        }
    }

    child.Lag = lag;
    ChildrenByLag[lag].insert(childId);

    const bool pending = !Pending.empty();
    Pending.erase(childId);

    if (Pending) {
        return false;
    } else if (pending) {
        return true;
    }

    return updated;
}

const TMaybe<TDuration> TLagProvider::GetLag() const {
    if (ChildrenByLag.empty() || !Pending.empty()) {
        return Nothing();
    }

    return ChildrenByLag.rbegin()->first;
}

}
