#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxProcessUpdateFollowers : public TTransactionBase<THive> {
    TSideEffects SideEffects;

    static constexpr size_t MAX_UPDATES_PROCESSED = 1000;
public:
    TTxProcessUpdateFollowers(THive* hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_DC_FOLLOWERS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxProcessUpdateFollowers::Execute()");
        NIceDb::TNiceDb db(txc.DB);
        SideEffects.Reset(Self->SelfId());
        for (size_t i = 0; !Self->PendingFollowerUpdates.Empty() && i < MAX_UPDATES_PROCESSED; ++i) {
            auto op = Self->PendingFollowerUpdates.Pop();
            TTabletInfo* tablet = Self->FindTablet(op.TabletId);
            auto& dc = Self->DataCenters[op.DataCenter];
            if (tablet == nullptr) {
                continue;
            }
            switch (op.Action) {
                case TFollowerUpdates::EAction::Create:
                {
                    if (!dc.IsRegistered()) {
                        continue;
                    }
                    TFollowerGroup& group = tablet->AsLeader().GetFollowerGroup(op.GroupId);
                    auto& followers = dc.Followers[{op.TabletId.first, op.GroupId}];
                    if (group.GetFollowerCountForDataCenter(op.DataCenter) <= followers.size()) {
                        continue;
                    }
                    TFollowerTabletInfo& follower = tablet->AsLeader().AddFollower(group);
                    follower.NodeFilter.AllowedDataCenters = {op.DataCenter};
                    follower.Statistics.SetLastAliveTimestamp(TlsActivationContext->Now().MilliSeconds());
                    db.Table<Schema::TabletFollowerTablet>().Key(op.TabletId.first, follower.Id).Update(
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(follower.FollowerGroup.Id),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(follower.Statistics),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::DataCenter>(op.DataCenter));
                    follower.InitTabletMetrics();
                    follower.BecomeStopped();
                    follower.InitiateBoot();
                    followers.push_back(std::prev(tablet->AsLeader().Followers.end()));
                    BLOG_D("THive::TTxProcessUpdateFollowers::Execute(): created follower " << follower.GetFullTabletId());
                    break;
                }
                case TFollowerUpdates::EAction::Update:
                {
                    // This is updated in memory in LoadEverything
                    bool exists = db.Table<Schema::TabletFollowerTablet>().Key(op.TabletId).Select().IsValid();
                    Y_ABORT_UNLESS(exists, "%s", (TStringBuilder() << "trying to update tablet " << op.TabletId).data());
                    db.Table<Schema::TabletFollowerTablet>().Key(op.TabletId).Update<Schema::TabletFollowerTablet::DataCenter>(op.DataCenter);
                    break;
                }
                case TFollowerUpdates::EAction::Delete:
                {
                    if (dc.IsRegistered()) {
                        continue;
                    }
                    db.Table<Schema::TabletFollowerTablet>().Key(op.TabletId).Delete();
                    db.Table<Schema::Metrics>().Key(op.TabletId).Delete();
                    tablet->InitiateStop(SideEffects);
                    auto& followers = dc.Followers[{op.TabletId.first, op.GroupId}]; // Note: there are at most 3 followers here, see TPartitionConfigMerger
                    auto iter = std::find_if(followers.begin(), followers.end(), [tabletId = op.TabletId](const auto& fw) {
                        return fw->GetFullTabletId() == tabletId;
                    });
                    Y_ABORT_UNLESS(iter != followers.end());
                    auto& leader = tablet->GetLeader();
                    leader.Followers.erase(*iter);
                    followers.erase(iter);
                    Self->UpdateCounterTabletsTotal(-1);
                    break;
                }
            }
        }
        if (Self->PendingFollowerUpdates.Empty()) {
            Self->ProcessFollowerUpdatesScheduled = false;
        } else {
            SideEffects.Send(Self->SelfId(), new TEvPrivate::TEvUpdateFollowers);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateProcessUpdateFollowers() {
    return new TTxProcessUpdateFollowers(this);
}

} // NHive
} // NKikimr
