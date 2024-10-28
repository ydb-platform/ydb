#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUpdateDcFollowers : public TTransactionBase<THive> {
    TDataCenterId DataCenterId;
    TSideEffects SideEffects;
public:
    TTxUpdateDcFollowers(const TDataCenterId& dataCenter, THive* hive)
        : TBase(hive)
        , DataCenterId(dataCenter)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_DC_FOLLOWERS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxUpdateDcFollowers::Execute(" << DataCenterId << ")");
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        auto& dataCenter = Self->DataCenters[DataCenterId];
        if (!dataCenter.UpdateScheduled) {
            return true;
        }
        dataCenter.UpdateScheduled = false;
        if (dataCenter.IsRegistered()) {
            for (auto& [tabletId, tablet] : Self->Tablets) {
                for (auto& group : tablet.FollowerGroups) {
                    auto& followers = dataCenter.Followers[{tabletId, group.Id}];
                    auto neededCount = group.GetFollowerCountForDataCenter(DataCenterId);
                    while (followers.size() < neededCount) {
                        TFollowerTabletInfo& follower = tablet.AddFollower(group);
                        follower.NodeFilter.AllowedDataCenters = {DataCenterId};
                        follower.Statistics.SetLastAliveTimestamp(TlsActivationContext->Now().MilliSeconds());
                        db.Table<Schema::TabletFollowerTablet>().Key(tabletId, follower.Id).Update(
                                    NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(follower.FollowerGroup.Id),
                                    NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0),
                                    NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(follower.Statistics),
                                    NIceDb::TUpdate<Schema::TabletFollowerTablet::DataCenter>(DataCenterId));
                        follower.InitTabletMetrics();
                        follower.BecomeStopped();
                        follower.InitiateBoot();
                        followers.push_back(std::prev(tablet.Followers.end()));
                        BLOG_D("THive::TTxUpdateDcFollowers::Execute(" << DataCenterId << "): created follower " << follower.GetFullTabletId());
                    }
                }
            }
        } else {
            // deleting followers
            i64 deletedFollowers = 0;
            for (auto& [_, followers] : dataCenter.Followers) {
                for (auto follower : followers) {
                    db.Table<Schema::TabletFollowerTablet>().Key(follower->GetFullTabletId()).Delete();
                    db.Table<Schema::Metrics>().Key(follower->GetFullTabletId()).Delete();
                    follower->InitiateStop(SideEffects);
                    auto& leader = follower->GetLeader();
                    leader.Followers.erase(follower);
                    ++deletedFollowers;
                }
            }
            BLOG_D("THive::TTxUpdateDcFollowers::Execute(" << DataCenterId << "): deleted " << deletedFollowers << " followers");
            Self->UpdateCounterTabletsTotal(-deletedFollowers);
            dataCenter.Followers.clear();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateUpdateDcFollowers(const TDataCenterId& dc) {
    return new TTxUpdateDcFollowers(dc, this);
}

} // NHive
} // NKikimr
