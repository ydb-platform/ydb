#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxDeleteTabletResult : public TTransactionBase<THive> {
    TEvTabletBase::TEvDeleteTabletResult::TPtr Result;
    TTabletId TabletId;
    TSideEffects SideEffects;

public:
    TTxDeleteTabletResult(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev, THive* hive)
        : TBase(hive)
        , Result(ev)
        , TabletId(Result->Get()->TabletId)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_DELETE_TABLET_RESULT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TEvTabletBase::TEvDeleteTabletResult* msg = Result->Get();
        BLOG_D("THive::TTxDeleteTabletResult::Execute(" << TabletId << " " << NKikimrProto::EReplyStatus_Name(msg->Status) << ")");
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (tablet != nullptr) {
            if (msg->Status == NKikimrProto::OK) {
                NIceDb::TNiceDb db(txc.DB);
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
                TVector<TActorId> storageInfoSubscribers;
                storageInfoSubscribers.swap(tablet->StorageInfoSubscribers);
                for (const TActorId& subscriber : storageInfoSubscribers) {
                    SideEffects.Send(
                        subscriber,
                        new TEvHive::TEvGetTabletStorageInfoResult(TabletId, NKikimrProto::ERROR, "Tablet deleted"));
                }
                TActorId unlockedFromActor = tablet->ClearLockedToActor();
                if (unlockedFromActor) {
                    // Notify lock owner that lock has been lost
                    SideEffects.Send(unlockedFromActor, new TEvHive::TEvLockTabletExecutionLost(TabletId, NKikimrHive::LOCK_LOST_REASON_TABLET_DELETED));
                }
                Self->PendingCreateTablets.erase({tablet->Owner.first, tablet->Owner.second});
                Self->DeleteTablet(tablet->Id);
            } else {
                BLOG_W("THive::TTxDeleteTabletResult retrying for " << TabletId << " because of " << NKikimrProto::EReplyStatus_Name(msg->Status));
                Y_ENSURE_LOG(tablet->IsDeleting(), " tablet " << tablet->Id);
                SideEffects.Schedule(TDuration::MilliSeconds(1000), new TEvHive::TEvInitiateDeleteStorage(tablet->Id));
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxDeleteTabletResult(" << TabletId << ")::Complete SideEffects " << SideEffects);
        SideEffects.Complete(ctx);

    }
};

ITransaction* THive::CreateDeleteTabletResult(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev) {
    return new TTxDeleteTabletResult(ev, this);
}

} // NHive
} // NKikimr
