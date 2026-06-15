#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>

#include <optional>

namespace NKikimr::NReplication::NController {

struct TItemWithLag {
    std::optional<TDuration> Lag;
};

class TLagProvider {
public:
    void AddPendingLag(ui64 childId);
    bool UpdateLag(TItemWithLag& child, ui64 childId, TDuration lag);
    const std::optional<TDuration> GetLag() const;

private:
    TMap<TDuration, THashSet<ui64>> ChildrenByLag;
    THashSet<ui64> Pending;
};

}
