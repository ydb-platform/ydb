#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxDeleteTabletResult : public TTransactionBase<THive> {
    TEvTabletBase::TEvDeleteTabletResult::TPtr Result;
    TTabletId TabletId;
    TLeaderTabletInfo* Tablet = nullptr;
    TVector<TActorId> StorageInfoSubscribers;
    TActorId UnlockedFromActor;

public:
    TTxDeleteTabletResult(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev, THive* hive)
        : TBase(hive)
        , Result(ev)
        , TabletId(Result->Get()->TabletId)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_DELETE_TABLET_RESULT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TEvTabletBase::TEvDeleteTabletResult* msg = Result->Get();
        BLOG_D("THive::TTxDeleteTabletResult::Execute(" << TabletId << " " << NKikimrProto::EReplyStatus_Name(msg->Status) << ")");
        Tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (Tablet != nullptr) {
            if (msg->Status == NKikimrProto::OK) {
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Metrics>().Key(Tablet->Id, 0).Delete();
                for (const TTabletChannelInfo& channelInfo : Tablet->TabletStorageInfo->Channels) {
                    for (const TTabletChannelInfo::THistoryEntry& historyInfo : channelInfo.History) {
                        db.Table<Schema::TabletChannelGen>().Key(Tablet->Id, channelInfo.Channel, historyInfo.FromGeneration).Delete();
                    }
                    db.Table<Schema::TabletChannel>().Key(Tablet->Id, channelInfo.Channel).Delete();
                }
                for (TFollowerTabletInfo& follower : Tablet->Followers) {
                    auto fullTabletId = follower.GetFullTabletId();
                    db.Table<Schema::TabletFollowerTablet>().Key(fullTabletId).Delete();
                    db.Table<Schema::Metrics>().Key(fullTabletId).Delete();
                }
                for (TFollowerGroup& group : Tablet->FollowerGroups) {
                    db.Table<Schema::TabletFollowerGroup>().Key(Tablet->Id, group.Id).Delete();
                }
                db.Table<Schema::Tablet>().Key(Tablet->Id).Delete();
                StorageInfoSubscribers.swap(Tablet->StorageInfoSubscribers);
                UnlockedFromActor = Tablet->ClearLockedToActor();
                Self->PendingCreateTablets.erase({Tablet->Owner.first, Tablet->Owner.second});
                Self->DeleteTablet(Tablet->Id);
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TEvTabletBase::TEvDeleteTabletResult* msg = Result->Get();
        BLOG_D("THive::TTxDeleteTabletResult(" << TabletId << " " << NKikimrProto::EReplyStatus_Name(msg->Status) << ")::Complete");
        Tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (Tablet) {
            if (msg->Status == NKikimrProto::OK) {
                //ctx.Send(Tablet.ActorToNotify, new TEvHive::TEvDeleteTabletReply(NKikimrProto::OK, Self->TabletID(), rec.GetTxId()));
            } else {
                BLOG_W("THive::TTxDeleteTabletResult retrying for " << TabletId << " because of " << NKikimrProto::EReplyStatus_Name(msg->Status));
                Y_ENSURE_LOG(Tablet->IsDeleting(), " tablet " << Tablet->Id);
                ctx.Schedule(TDuration::MilliSeconds(1000), new TEvHive::TEvInitiateDeleteStorage(Tablet->Id));
            }
        }
        for (const TActorId& subscriber : StorageInfoSubscribers) {
            ctx.Send(
                subscriber,
                new TEvHive::TEvGetTabletStorageInfoResult(TabletId, NKikimrProto::ERROR, "Tablet deleted"));
        }
        if (UnlockedFromActor) {
            // Notify lock owner that lock has been lost
            ctx.Send(UnlockedFromActor, new TEvHive::TEvLockTabletExecutionLost(TabletId));
        }
    }
};

ITransaction* THive::CreateDeleteTabletResult(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev) {
    return new TTxDeleteTabletResult(ev, this);
}

} // NHive
} // NKikimr
