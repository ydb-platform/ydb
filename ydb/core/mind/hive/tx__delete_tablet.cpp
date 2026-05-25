#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxDeleteBase : public TTransactionBase<THive> {
protected:
    TSideEffects SideEffects;

public:
    TTxType GetTxType() const override { return NHive::TXTYPE_DELETE_TABLET; }

    TTxDeleteBase(THive* hive)
        : TBase(hive)
    {}

    void DeleteTablet(TTabletId tabletId, NIceDb::TNiceDb& db) {
        YDB_LOG_DEBUG("THive::TTxDeleteTablet::Execute Tablet",
            {"GetLogPrefix", GetLogPrefix()},
            {"tabletId", tabletId});
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
        if (tablet != nullptr) {
            if (tablet->SeizedByChild) {
                YDB_LOG_WARN("THive::TTxDeleteTablet tablet seized by child",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"tabletId", tabletId});
                return;
            }
            if (tablet->State != ETabletState::Deleting) {
                tablet->State = ETabletState::Deleting;
                db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State, Schema::Tablet::LeaderNode>(ETabletState::Deleting, 0);
                for (const TActorId& actor : tablet->ActorsToNotifyOnRestart) {
                    SideEffects.Send(actor, new TEvPrivate::TEvRestartComplete(tablet->GetFullTabletId(), "delete"));
                }
                tablet->ActorsToNotifyOnRestart.clear();
                tablet->InitiateStop(SideEffects);
                for (TTabletInfo& follower : tablet->Followers) {
                    for (const TActorId& actor : follower.ActorsToNotifyOnRestart) {
                        SideEffects.Send(actor, new TEvPrivate::TEvRestartComplete(follower.GetFullTabletId(), "delete"));
                    }
                    follower.ActorsToNotifyOnRestart.clear();
                    follower.InitiateStop(SideEffects);
                    db.Table<Schema::TabletFollowerTablet>().Key(follower.GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                }
                Self->BlockStorageForDelete(tabletId, SideEffects);
            } else {
                YDB_LOG_DEBUG("THive::TTxDeleteTablet::Execute Tablet already in ETabletState::Deleting",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"tabletId", tabletId});
            }
        } else {
            YDB_LOG_WARN("THive::TTxDeleteTablet tablet wasn't found",
                {"GetLogPrefix", GetLogPrefix()},
                {"tabletId", tabletId});
        }
    }
};

class TTxDeleteTablet : public TTxDeleteBase {
    TEvHive::TEvDeleteTablet::TPtr Event;

public:
    TTxDeleteTablet(TEvHive::TEvDeleteTablet::TPtr& ev, THive* hive)
        : TTxDeleteBase(hive)
        , Event(ev)
    {}

    void RespondToSender(NKikimrProto::EReplyStatus status, const NKikimrHive::TForwardRequest& forwardRequest = {}) {
        const NKikimrHive::TEvDeleteTablet& rec = Event->Get()->Record;
        auto response = MakeHolder<TEvHive::TEvDeleteTabletReply>(status, Self->TabletID(), rec);
        if (forwardRequest.GetHiveTabletId() != 0) {
            response->Record.MutableForwardRequest()->CopyFrom(forwardRequest);
        }
        YDB_LOG_DEBUG("THive::TTxDeleteTablet::Execute() result",
            {"GetLogPrefix", GetLogPrefix()},
            {"ShortDebugString", response->Record.ShortDebugString()});
        SideEffects.Send(Event->Sender, response.Release(), 0, Event->Cookie);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        const NKikimrHive::TEvDeleteTablet& rec = Event->Get()->Record;
        YDB_LOG_DEBUG("THive::TTxDeleteTablet::Execute()",
            {"GetLogPrefix", GetLogPrefix()},
            {"ShortDebugString", rec.ShortDebugString()});
        // resolving ownerid:owneridx to tabletids
        std::vector<TTabletId> tablets;
        tablets.reserve(rec.ShardLocalIdxSize());
        ui64 owner = rec.GetShardOwnerId();
        for (size_t pos = 0; pos < rec.ShardLocalIdxSize(); ++pos) {
            ui64 idx = rec.GetShardLocalIdx(pos);
            TOwnerIdxType::TValueType ownerIdx(owner, idx);
            auto it = Self->OwnerToTablet.find(ownerIdx);
            if (it != Self->OwnerToTablet.end()) {
                tablets.push_back(it->second);
            } else {
                if (pos < rec.TabletIDSize()) {
                    TTabletId tabletId = rec.GetTabletID(pos);
                    YDB_LOG_WARN("THive::TTxDeleteTablet tablet wasn't found - using supplied",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"ownerIdx", ownerIdx},
                        {"tabletId", tabletId});
                    tablets.push_back(tabletId);
                } else {
                    YDB_LOG_WARN("THive::TTxDeleteTablet tablet wasn't found",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"ownerIdx", ownerIdx});
                }
            }
            if (Self->PendingCreateTablets.erase({owner, idx}) != 0) {
                YDB_LOG_NOTICE("THive::TTxDeleteTablet tablet was cleared from pending creates",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"ownerIdx", ownerIdx});
            }
        }
        // checking for possible forwards
        NKikimrHive::TForwardRequest forwardRequest;
        for (TTabletId tabletId : tablets) {
            TTabletId prevForwardHiveTabletId = forwardRequest.GetHiveTabletId();
            if (Self->CheckForForwardTabletRequest(tabletId, forwardRequest)) {
                if (prevForwardHiveTabletId != 0 && prevForwardHiveTabletId != forwardRequest.GetHiveTabletId()) {
                    YDB_LOG_ERROR("Forward of DeleteTablet is not possible - different owners of tablets",
                        {"GetLogPrefix", GetLogPrefix()});
                    RespondToSender(NKikimrProto::ERROR);
                    return true; // abort transaction
                }
            }
        }
        // respond with forward
        if (forwardRequest.GetHiveTabletId() != 0) {
            // actually this status from blob storage, but I think it fits this situation perfectly
            RespondToSender(NKikimrProto::INVALID_OWNER, forwardRequest);
            return true; // abort transaction
        }
        // checking for possible migration
        for (TTabletId tabletId : tablets) {
           TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
            if (tablet != nullptr) {
                if (tablet->SeizedByChild) {
                    YDB_LOG_WARN("THive::TTxDeleteTablet tablet seized by child",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"tabletId", tabletId});
                    RespondToSender(NKikimrProto::ERROR);
                    return true; // abort transaction
                }
            }
        }
        NIceDb::TNiceDb db(txc.DB);
        for (TTabletId tabletId : tablets) {
            DeleteTablet(tabletId, db);
        }
        RespondToSender(NKikimrProto::OK);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxDeleteTablet::Complete()",
            {"GetLogPrefix", GetLogPrefix()},
            {"SideEffects", SideEffects});
        SideEffects.Complete(ctx);
    }
};

class TTxDeleteOwnerTablets : public TTxDeleteBase {
    TEvHive::TEvDeleteOwnerTablets::TPtr Event;

public:
    TTxDeleteOwnerTablets(TEvHive::TEvDeleteOwnerTablets::TPtr& ev, THive* hive)
        : TTxDeleteBase(hive)
        , Event(ev)
    {}

    void RespondToSender(NKikimrProto::EReplyStatus status) {
        const NKikimrHive::TEvDeleteOwnerTablets& rec = Event->Get()->Record;
        auto response = MakeHolder<TEvHive::TEvDeleteOwnerTabletsReply>(status, Self->TabletID(), rec.GetOwner(), rec.GetTxId());
        YDB_LOG_DEBUG("THive::TTxDeleteOwnerTablets::Execute() result",
            {"GetLogPrefix", GetLogPrefix()},
            {"ShortDebugString", response->Record.ShortDebugString()});
        SideEffects.Send(Event->Sender, response.Release(), 0, Event->Cookie);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        const NKikimrHive::TEvDeleteOwnerTablets& rec = Event->Get()->Record;
        YDB_LOG_DEBUG("THive::TEvDeleteOwnerTablets::Execute()",
            {"GetLogPrefix", GetLogPrefix()},
            {"ShortDebugString", rec.ShortDebugString()});
        // resolving owner to tabletids
        std::vector<TTabletId> tablets;
        ui64 owner = rec.GetOwner();
        for (const auto& [ownerIdx, tabletId] : Self->OwnerToTablet) {
            if (ownerIdx.first != owner) {
                continue;
            }
            tablets.push_back(tabletId);
            if (Self->PendingCreateTablets.erase(ownerIdx) != 0) {
                auto id = ownerIdx;
                YDB_LOG_NOTICE("THive::TTxDeleteOwnerTablets tablet was cleared from pending creates",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"id", id});
            }
        }
        // checking for possible migration
        for (TTabletId tabletId : tablets) {
           TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
            if (tablet != nullptr) {
                if (tablet->SeizedByChild) {
                    YDB_LOG_WARN("THive::TTxDeleteTablet tablet seized by child",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"tabletId", tabletId});
                    RespondToSender(NKikimrProto::ERROR);
                    return true; // abort transaction
                }
            }
        }
        NIceDb::TNiceDb db(txc.DB);
        for (TTabletId tabletId : tablets) {
            DeleteTablet(tabletId, db);
        }
        db.Table<Schema::BlockedOwner>().Key(rec.GetOwner()).Update();
        Self->BlockedOwners.emplace(Event->Get()->Record.GetOwner());
        RespondToSender(NKikimrProto::OK);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TEvDeleteOwnerTablets::Complete(",
            {"GetLogPrefix", GetLogPrefix()},
            {"GetOwner", Event->Get()->Record.GetOwner()},
            {"SideEffects", SideEffects});
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateDeleteTablet(TEvHive::TEvDeleteTablet::TPtr& ev) {
    return new TTxDeleteTablet(ev, this);
}

ITransaction* THive::CreateDeleteOwnerTablets(TEvHive::TEvDeleteOwnerTablets::TPtr& ev) {
    return new TTxDeleteOwnerTablets(ev, this);
}

} // NHive
} // NKikimr
