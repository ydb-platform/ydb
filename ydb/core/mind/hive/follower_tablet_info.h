#pragma once

#include "hive.h"
#include "tablet_info.h"
#include "follower_group.h"

namespace NKikimr {
namespace NHive {

struct TFollowerGroup;

struct TFollowerTabletInfo : TTabletInfo {
public:
    TLeaderTabletInfo& LeaderTablet;
    TFollowerId Id;
    TFollowerGroup& FollowerGroup;

    TFollowerTabletInfo(TLeaderTabletInfo& leaderTablet, TFollowerId id, TFollowerGroup& followerGroup);
};

} // NHive
} // NKikimr
