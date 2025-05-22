#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxReleaseTablets : public TTransactionBase<THive> {
    THolder<TEvHive::TEvReleaseTablets::THandle> Request;
    THolder<TEvHive::TEvReleaseTabletsReply> Response = MakeHolder<TEvHive::TEvReleaseTabletsReply>();
    TVector<std::pair<TTabletId, TActorId>> UnlockedFromActor;
    bool NeedToProcessPendingOperations = false;
    TSideEffects SideEffects;

public:
    TTxReleaseTablets(THolder<TEvHive::TEvReleaseTablets::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RELEASE_TABLETS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvReleaseTablets& request(Request->Get()->Record);
        NKikimrHive::TEvReleaseTabletsReply& response(Response->Record);
        SideEffects.Reset(Self->SelfId());
        BLOG_D("THive::TTxReleaseTablets::Execute " << request);
        NIceDb::TNiceDb db(txc.DB);
        for (TTabletId tabletId : request.GetTabletIDs()) {
            TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
            if (tablet != nullptr) {
                Y_ABORT_UNLESS(tablet->SeizedByChild);

                if (tablet->IsAlive() && tablet->Node != nullptr) {
                    tablet->SendStopTablet(tablet->Node->Local, SideEffects);
                    for (TFollowerTabletInfo& follower : tablet->Followers) {
                        if (follower.IsAlive() && follower.Node != nullptr) {
                            follower.SendStopTablet(follower.Node->Local, SideEffects);
                        }
                    }
                }

                db.Table<Schema::Metrics>().Key(tablet->Id, 0).Delete();
                for (const TTabletChannelInfo& channelInfo : tablet->TabletStorageInfo->Channels) {
                    for (const TTabletChannelInfo::THistoryEntry& historyInfo : channelInfo.History) {
                        db.Table<Schema::TabletChannelGen>().Key(tablet->Id, channelInfo.Channel, historyInfo.FromGeneration).Delete();
                    }
                    db.Table<Schema::TabletChannel>().Key(tablet->Id, channelInfo.Channel).Delete();
                }
                for (TFollowerTabletInfo& follower : tablet->Followers) {
                    auto fullTabletId = follower.GetFullTabletId();
                    db.Table<Schema::TabletFollowerTablet>().Key(fullTabletId).Delete();
                    db.Table<Schema::Metrics>().Key(fullTabletId).Delete();
                }
                for (TFollowerGroup& group : tablet->FollowerGroups) {
                    db.Table<Schema::TabletFollowerGroup>().Key(tablet->Id, group.Id).Delete();
                }
                db.Table<Schema::Tablet>().Key(tablet->Id).Delete();
                TActorId unlockedFromActor = tablet->ClearLockedToActor();
                if (unlockedFromActor) {
                    UnlockedFromActor.emplace_back(tabletId, unlockedFromActor);
                }
                if (Self->PendingCreateTablets.count({tablet->Owner.first, tablet->Owner.second}) > 0) {
                    NeedToProcessPendingOperations = true;
                }
                Self->DeleteTablet(tablet->Id);

                ui64 uniqPart = UniqPartFromTabletID(tabletId);
                ui64 newOwnerId = request.GetNewOwnerID();
                TSequencer::TSequence pointSeq(uniqPart);

                db.Table<Schema::TabletOwners>().Key(pointSeq.Begin, pointSeq.End).Update<Schema::TabletOwners::OwnerId>(newOwnerId);
                Self->Keeper.AddOwnedSequence(newOwnerId, pointSeq);
            }

            response.AddTabletIDs(tabletId);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxReleaseTablets::Complete " << Request->Get()->Record << " SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
        for (const auto& unlockedFromActor : UnlockedFromActor) {
            // Notify lock owner that lock has been lost
            ctx.Send(unlockedFromActor.second, new TEvHive::TEvLockTabletExecutionLost(unlockedFromActor.first, NKikimrHive::LOCK_LOST_REASON_TABLET_RELEASED));
        }
        ctx.Send(Request->Sender, Response.Release());
        if (NeedToProcessPendingOperations) {
            BLOG_D("THive::TTxReleaseTablets::Complete - retrying pending operations");
            Self->ProcessPendingOperations();
        }
    }
};

ITransaction* THive::CreateReleaseTablets(TEvHive::TEvReleaseTablets::TPtr event) {
    return new TTxReleaseTablets(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
