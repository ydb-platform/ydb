#include "follower_tablet_info.h"
#include "follower_group.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

TFollowerTabletInfo::TFollowerTabletInfo(TLeaderTabletInfo& leaderTablet, TFollowerId id, TFollowerGroup& followerGroup)
    : TTabletInfo(ETabletRole::Follower, leaderTablet.Hive)
    , LeaderTablet(leaderTablet)
    , Id(id)
    , FollowerGroup(followerGroup)
{}

}
}
