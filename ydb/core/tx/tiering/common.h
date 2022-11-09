#pragma once
#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>

#include <library/cpp/actors/core/events.h>

namespace NKikimr::NColumnShard::NTiers {

class TGlobalTierId {
private:
    YDB_ACCESSOR_DEF(TString, OwnerPath);
    YDB_ACCESSOR_DEF(TString, TierName);
public:
    TGlobalTierId(const TString& ownerPath, const TString& tierName)
        : OwnerPath(ownerPath)
        , TierName(tierName) {

    }

    bool operator<(const TGlobalTierId& item) const {
        return std::tie(OwnerPath, TierName) < std::tie(item.OwnerPath, item.TierName);
    }

    TString ToString() const {
        return OwnerPath + "." + TierName;
    }
};

enum EEvents {
    EvTierCleared = EventSpaceBegin(TKikimrEvents::ES_TIERING),
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING)");
}
