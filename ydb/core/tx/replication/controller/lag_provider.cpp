#include "lag_provider.h"

namespace NKikimr::NReplication::NController {

void TLagProvider::AddPendingLag(ui64 childId) {
    Pending.insert(childId);
}

void TLagProvider::RemoveLag(const TItemWithLag& child, ui64 childId) {
    Pending.erase(childId);
    if (child.Lag) {
        auto it = ChildrenByLag.find(*child.Lag);
        Y_ABORT_UNLESS(it != ChildrenByLag.end());
        it->second.erase(childId);
        if (it->second.empty()) {
            ChildrenByLag.erase(it);
        }
    }
}

void TLagProvider::InvalidateLag(TItemWithLag& child, ui64 childId) {
    RemoveLag(child, childId);
    child.Lag.reset();
    AddPendingLag(childId);
}

bool TLagProvider::UpdateLag(TItemWithLag& child, ui64 childId, TDuration lag) {
    const auto previous = GetLag();
    RemoveLag(child, childId);
    child.Lag = lag;
    ChildrenByLag[lag].insert(childId);
    return previous != GetLag();
}

const std::optional<TDuration> TLagProvider::GetLag() const {
    if (ChildrenByLag.empty() || !Pending.empty()) {
        return std::nullopt;
    }

    return ChildrenByLag.rbegin()->first;
}

}
