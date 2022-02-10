#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxDeleteTablet : public TTransactionBase<THive> {
    TEvHive::TEvDeleteTablet::TPtr Event;
    ui64 OwnerId = 0;
    TVector<ui64> LocalIdxs;
    NKikimrProto::EReplyStatus Status = NKikimrProto::ERROR;
    TVector<TTabletId> TabletIds;
    TCompleteNotifications Notifications;
    NKikimrHive::TForwardRequest ForwardRequest;

public:
    TTxDeleteTablet(TEvHive::TEvDeleteTablet::TPtr& ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_DELETE_TABLET; }

    TTabletId DoDeleteTablet(ui64 owner, ui64 idx, NIceDb::TNiceDb& db) {
        TTabletId deletedTablet = 0;

        TOwnerIdxType::TValueType ownerIdx(owner, idx);
        auto it = Self->OwnerToTablet.find(ownerIdx);
        if (it != Self->OwnerToTablet.end()) {
            deletedTablet = it->second;
            BLOG_D("THive::TTxDeleteTablet::Execute Tablet " << it->second);
            TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(it->second);
            Y_VERIFY(tablet != nullptr, "%s", (TStringBuilder() << "Tablet " << it->second << " OwnerIdx " << ownerIdx).data());
            if (tablet->SeizedByChild) {
                BLOG_W("THive::TTxDeleteTablet tablet " << it->second << " seized by child");
                return 0;
            }
            if (tablet->State != ETabletState::Deleting) {
                tablet->State = ETabletState::Deleting;
                tablet->InitiateStop();
                db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State, Schema::Tablet::LeaderNode>(ETabletState::Deleting, 0);
                for (TTabletInfo& follower : tablet->Followers) {
                    follower.InitiateStop();
                    db.Table<Schema::TabletFollowerTablet>().Key(follower.GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                }
                if (!tablet->InitiateBlockStorage(std::numeric_limits<ui32>::max())) {
                    Self->DeleteTabletWithoutStorage(tablet);
                }
            } else {
                BLOG_D("THive::TTxDeleteTablet::Execute Tablet " << it->second << " already in ETabletState::Deleting");
            }
        } else {
            BLOG_W("THive::TTxDeleteTablet tablet " << ownerIdx << " wasn't found");
            Self->PendingCreateTablets.erase({owner, idx});
        }

        return deletedTablet;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvDeleteTablet& rec = Event->Get()->Record;
        NIceDb::TNiceDb db(txc.DB);
        OwnerId = rec.GetShardOwnerId();
        for (ui64 idx : rec.GetShardLocalIdx()) {
            LocalIdxs.push_back(idx);
        }
        for (TTabletId tabletId : rec.GetTabletID()) {
            TTabletId prevForwardHiveTabletId = ForwardRequest.GetHiveTabletId();
            if (Self->CheckForForwardTabletRequest(tabletId, ForwardRequest)) {
                if (prevForwardHiveTabletId != 0 && prevForwardHiveTabletId != ForwardRequest.GetHiveTabletId()) {
                    BLOG_ERROR("Forward of DeleteTablet is not possible - different owners of tablets");
                    Status = NKikimrProto::ERROR; // actually this status from blob storage, but I think it fits this situation perfectly
                    return true; // abort transaction
                }
            }
        }
        if (ForwardRequest.GetHiveTabletId() != 0) {
            Status = NKikimrProto::INVALID_OWNER; // actually this status from blob storage, but I think it fits this situation perfectly
            return true; // abort transaction
        }
        for (ui64 idx : rec.GetShardLocalIdx()) {
            Status = NKikimrProto::OK;
            if (TTabletId deletedTablet = DoDeleteTablet(OwnerId, idx, db)) {
                TabletIds.push_back(deletedTablet);
            }
        }
        for (ui64 tabletId : TabletIds) {
            TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
            if (tablet != nullptr) {
                for (const TActorId& actor : tablet->ActorsToNotifyOnRestart) {
                    Notifications.Send(actor, new TEvPrivate::TEvRestartComplete(tablet->GetFullTabletId(), "delete"));
                }
                tablet->ActorsToNotifyOnRestart.clear();
                for (TTabletInfo& follower : tablet->Followers) {
                    for (const TActorId& actor : follower.ActorsToNotifyOnRestart) {
                        Notifications.Send(actor, new TEvPrivate::TEvRestartComplete(follower.GetFullTabletId(), "delete"));
                    }
                    follower.ActorsToNotifyOnRestart.clear();
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxDeleteTablet::Complete(" << TabletIds << ")");
        THolder<TEvHive::TEvDeleteTabletReply> response = MakeHolder<TEvHive::TEvDeleteTabletReply>(Status, Self->TabletID(), Event->Get()->Record.GetTxId_Deprecated(), OwnerId, LocalIdxs);
        if (ForwardRequest.HasHiveTabletId()) {
            response->Record.MutableForwardRequest()->CopyFrom(ForwardRequest);
        }
        ctx.Send(Event->Sender, response.Release(), 0, Event->Cookie);
        Notifications.Send(ctx);
    }
};

ITransaction* THive::CreateDeleteTablet(TEvHive::TEvDeleteTablet::TPtr& ev) { 
    return new TTxDeleteTablet(ev, this);
}


// TODO: split
class TTxDeleteOwnerTablets : public TTransactionBase<THive> {
    TEvHive::TEvDeleteOwnerTablets::TPtr Event;
    NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
    TVector<ui64> ToDelete;
public:
    TTxDeleteOwnerTablets(TEvHive::TEvDeleteOwnerTablets::TPtr& ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTabletId DoDeleteTablet(ui64 owner, ui64 idx, NIceDb::TNiceDb& db) {
        TTabletId deletedTablet = 0;

        TOwnerIdxType::TValueType ownerIdx(owner, idx);
        auto it = Self->OwnerToTablet.find(ownerIdx);
        if (it != Self->OwnerToTablet.end()) {
            deletedTablet = it->second;
            BLOG_D("THive::TTxDeleteTablet::Execute Tablet " << it->second);
            TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(it->second);
            Y_VERIFY(tablet != nullptr, "%s", (TStringBuilder() << "Tablet " << it->second << " OwnerIdx " << ownerIdx).data());
            if (tablet->SeizedByChild) {
                BLOG_W("THive::TTxDeleteTablet tablet " << it->second << " seized by child");
                return 0;
            }
            if (tablet->State != ETabletState::Deleting) {
                tablet->State = ETabletState::Deleting;
                tablet->InitiateStop();
                db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State, Schema::Tablet::LeaderNode>(ETabletState::Deleting, 0);
                for (TTabletInfo& follower : tablet->Followers) {
                    follower.InitiateStop();
                    db.Table<Schema::TabletFollowerTablet>().Key(follower.GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                }
                if (!tablet->InitiateBlockStorage(std::numeric_limits<ui32>::max())) {
                    Self->DeleteTabletWithoutStorage(tablet);
                }
            } else {
                BLOG_D("THive::TTxDeleteTablet::Execute Tablet " << it->second << " already in ETabletState::Deleting");
            }
        } else {
            BLOG_W("THive::TTxDeleteTablet tablet " << ownerIdx << " wasn't found");
            Self->PendingCreateTablets.erase({owner, idx});
        }

        return deletedTablet;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvDeleteOwnerTablets& rec = Event->Get()->Record;
        BLOG_D("THive::TEvDeleteOwnerTablets::Execute Owner: " << rec.GetOwner());
        for (auto item : Self->OwnerToTablet) {
            if (item.first.first != rec.GetOwner()) {
                continue;
            }
            const TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(item.second);
            if (tablet) {
                if (!tablet->IsDeleting()) {
                    ToDelete.push_back(item.first.second);
                }
            }
        }

        if (ToDelete.empty()) {
            Status = NKikimrProto::ALREADY;
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        for (auto idx : ToDelete) {
            DoDeleteTablet(rec.GetOwner(), idx, db);
        }
        db.Table<Schema::BlockedOwner>().Key(rec.GetOwner()).Update();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TEvDeleteOwnerTablets::Complete(" << Event->Get()->Record.GetOwner() << "), " << ToDelete.size() << " tablet has been deleted");
        Self->BlockedOwners.emplace(Event->Get()->Record.GetOwner());
        ctx.Send(Event->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(Status, Self->TabletID(), Event->Get()->Record.GetOwner(), Event->Get()->Record.GetTxId()));
    }
};

ITransaction* THive::CreateDeleteOwnerTablets(TEvHive::TEvDeleteOwnerTablets::TPtr& ev) {
    return new TTxDeleteOwnerTablets(ev, this);
}


} // NHive
} // NKikimr
