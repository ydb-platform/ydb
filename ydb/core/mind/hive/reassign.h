#pragma once

#include "hive.h"

#include <optional>
#include <vector>

namespace NKikimr::NHive {

inline constexpr ui64 MAX_REASSIGNS_WITHOUT_CONFIRMATION = 1000;
inline constexpr i64 REASSIGN_CONFIRMATION_ERROR_MARGIN = 10;

struct TReassignTabletsSettings {
    std::optional<TTabletId> TabletId;
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
    TVector<ui32> TabletChannels;
    ui32 GroupId = 0;
    TString StoragePool;
    TVector<ui32> ForcedGroupIds;
    bool Async = false;
    ui32 TabletPercent = 100;
    std::optional<i64> NumTablets;
};

struct TReassignTabletsPlan {
    std::vector<TReassignOperation> Operations;
    TString Description;
    TString Error;
};

} // namespace NKikimr::NHive
