#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUnlockTabletExecution : public TTransactionBase<THive> {
    const ui64 TabletId;
    const TActorId OwnerActor;
    const ui64 SeqNo;
    const NKikimrHive::ELockLostReason Reason = NKikimrHive::LOCK_LOST_REASON_UNKNOWN;

    const TActorId Sender;
    const ui64 Cookie;

    TSideEffects SideEffects;
    TActorId PreviousOwner;

public:
    TTxUnlockTabletExecution(const NKikimrHive::TEvUnlockTabletExecution& rec, const TActorId& sender, const ui64 cookie, THive* hive)
        : TBase(hive)
        , TabletId(rec.GetTabletID())
        , OwnerActor(GetOwnerActor(rec, sender))
        , SeqNo(0)
        , Reason(NKikimrHive::LOCK_LOST_REASON_UNLOCKED)
        , Sender(sender)
        , Cookie(cookie)
    {
        Y_ABORT_UNLESS(!!Sender);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_UNLOCK_TABLET_EXECUTION; }

    TTxUnlockTabletExecution(ui64 tabletId, ui64 seqNo, NKikimrHive::ELockLostReason reason, THive* hive)
        : TBase(hive)
        , TabletId(tabletId)
        , SeqNo(seqNo)
        , Reason(reason)
        , Cookie(0)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_NOTICE("THive::TTxUnlockTabletExecution::Execute TabletId: " << TabletId);
        SideEffects.Reset(Self->SelfId());
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (tablet == nullptr) {
            SideEffects.Send(Sender, new TEvHive::TEvUnlockTabletExecutionResult(TabletId, NKikimrProto::ERROR,
                TStringBuilder() << "Trying to unlock tablet " << TabletId << ", which doesn't exist"), 0, Cookie);
            return true;
        }

        if (OwnerActor && tablet->LockedToActor != OwnerActor) {
            SideEffects.Send(Sender, new TEvHive::TEvUnlockTabletExecutionResult(TabletId, NKikimrProto::ERROR,
                TStringBuilder() << "Trying to unlock tablet " << TabletId
                << ", which is locked to " << tablet->LockedToActor << ", not " << OwnerActor), 0, Cookie);
            return true;
        }

        if (SeqNo && tablet->PendingUnlockSeqNo != SeqNo) {
            SideEffects.Send(Sender, new TEvHive::TEvUnlockTabletExecutionResult(TabletId, NKikimrProto::ERROR,
                TStringBuilder() << "Trying to unlock tablet " << TabletId << ", which is out of sequence"), 0, Cookie);
            return true;
        }

        // Mark tablet unlocked
        PreviousOwner = tablet->ClearLockedToActor();

        // Persist to database
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Tablet>().Key(TabletId).Update(
            NIceDb::TUpdate<Schema::Tablet::LockedToActor>(tablet->LockedToActor),
            NIceDb::TUpdate<Schema::Tablet::LockedReconnectTimeout>(tablet->LockedReconnectTimeout.MilliSeconds()));

        if (PreviousOwner) {
            // Notify previous owner that its lock ownership has been lost
            SideEffects.Send(PreviousOwner, new TEvHive::TEvLockTabletExecutionLost(TabletId, Reason));
        }

        if (!tablet->IsLockedToActor()) {
            // Try to boot it if possible
            tablet->TryToBoot();
        }
        SideEffects.Send(Sender, new TEvHive::TEvUnlockTabletExecutionResult(TabletId, NKikimrProto::OK, {}), 0, Cookie);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_NOTICE("THive::TTxUnlockTabletExecution::Complete TabletId: " << TabletId << " SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
    }

private:
    static TActorId GetOwnerActor(const NKikimrHive::TEvUnlockTabletExecution& rec, const TActorId& sender) {
        TActorId owner = sender;
        if (rec.HasOwnerActor()) {
            owner = ActorIdFromProto(rec.GetOwnerActor());
        }
        return owner;
    }
};

ITransaction* THive::CreateUnlockTabletExecution(const NKikimrHive::TEvUnlockTabletExecution& rec, const TActorId& sender, const ui64 cookie) {
    return new TTxUnlockTabletExecution(rec, sender, cookie, this);
}

ITransaction* THive::CreateUnlockTabletExecution(ui64 tabletId, ui64 seqNo, NKikimrHive::ELockLostReason reason) {
    return new TTxUnlockTabletExecution(tabletId, seqNo, reason, this);
}

void THive::ScheduleUnlockTabletExecution(TNodeInfo& node, NKikimrHive::ELockLostReason reason) {
    // Unlock tablets that have been locked by this node
    BLOG_NOTICE("ScheduleUnlockTabletExecution(" << node.Id << ", " << NKikimrHive::ELockLostReason_Name(reason) << ")");
    for (TLeaderTabletInfo* tablet : node.LockedTablets) {
        Y_ABORT_UNLESS(FindTabletEvenInDeleting(tablet->Id) == tablet);
        Y_ABORT_UNLESS(tablet->LockedToActor.NodeId() == node.Id);
        if (tablet->PendingUnlockSeqNo == 0) {
            tablet->PendingUnlockSeqNo = NextTabletUnlockSeqNo++;
            Y_ABORT_UNLESS(tablet->PendingUnlockSeqNo != 0);
            auto event = new TEvPrivate::TEvUnlockTabletReconnectTimeout(*tablet, reason);
            if (tablet->LockedReconnectTimeout) {
                Schedule(tablet->LockedReconnectTimeout, event);
            } else {
                Send(SelfId(), event);
            }
        }
    }
}

void THive::Handle(TEvPrivate::TEvUnlockTabletReconnectTimeout::TPtr& ev) {
    TTabletId tabletId = ev->Get()->TabletId;
    ui64 seqNo = ev->Get()->SeqNo;
    BLOG_NOTICE("THive::Handle::TEvUnlockTabletReconnectTimeout TabletId=" << tabletId << " Reason=" << NKikimrHive::ELockLostReason_Name(ev->Get()->Reason));
    TLeaderTabletInfo* tablet = FindTabletEvenInDeleting(tabletId);
    if (tablet != nullptr && tablet->IsLockedToActor() && tablet->PendingUnlockSeqNo == seqNo) {
        // We use sequence numbers to make sure unlock happens only if some
        // other pending lock/unlock transaction has not modified the lock.
        //
        // Example sequence of events:
        // - lock (success)
        // - node disconnected
        // - reconnect timeout scheduled
        // - lock/reconnect (transaction scheduled)
        // - reconnect timeout (transaction scheduled)
        // - lock/reconnect (execute, success)
        // - reconnect timeout (execute, failure)
        //   tablet is not unlocked, because logically lock/reconnect
        //   transaction was scheduled before the timeout really happened.
        Execute(CreateUnlockTabletExecution(tabletId, seqNo, ev->Get()->Reason));
    }
}

void THive::Handle(TEvHive::TEvUnlockTabletExecution::TPtr& ev) {
    Execute(CreateUnlockTabletExecution(ev->Get()->Record, ev->Sender, ev->Cookie));
}

} // NHive
} // NKikimr
