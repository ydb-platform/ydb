#include "hive_impl.h" 
#include "hive_log.h" 

namespace NKikimr {
namespace NHive { 

class TTxUnlockTabletExecution : public TTransactionBase<THive> { 
    const ui64 TabletId;
    const TActorId OwnerActor;
    const ui64 SeqNo;

    const TActorId Sender;
    const ui64 Cookie;

    NKikimrProto::EReplyStatus Status;
    TString StatusMessage;
    TActorId PreviousOwner;

public:
    TTxUnlockTabletExecution(const NKikimrHive::TEvUnlockTabletExecution& rec, const TActorId& sender, const ui64 cookie, THive* hive)
        : TBase(hive)
        , TabletId(rec.GetTabletID())
        , OwnerActor(GetOwnerActor(rec, sender))
        , SeqNo(0)
        , Sender(sender)
        , Cookie(cookie)
    {
        Y_VERIFY(!!Sender);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_UNLOCK_TABLET_EXECUTION; } 
 
    TTxUnlockTabletExecution(ui64 tabletId, ui64 seqNo, THive* hive)
        : TBase(hive)
        , TabletId(tabletId)
        , SeqNo(seqNo)
        , Cookie(0)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override { 
        BLOG_D("THive::TTxUnlockTabletExecution::Execute"); 

        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (tablet == nullptr) {
            Status = NKikimrProto::ERROR;
            StatusMessage = TStringBuilder() << "Trying to unlock tablet " << TabletId
                    << ", which doesn't exist";
            return true;
        }

        if (OwnerActor && tablet->LockedToActor != OwnerActor) {
            Status = NKikimrProto::ERROR;
            StatusMessage = TStringBuilder() << "Trying to unlock tablet " << TabletId
                    << ", which is locked to " << tablet->LockedToActor << ", not " << OwnerActor;
            return true;
        }

        if (SeqNo && tablet->PendingUnlockSeqNo != SeqNo) {
            Status = NKikimrProto::ERROR;
            StatusMessage = TStringBuilder() << "Trying to unlock tablet " << TabletId
                    << ", which is out of sequence";
            return true;
        }

        // Mark tablet unlocked
        PreviousOwner = tablet->ClearLockedToActor();

        // Persist to database
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Tablet>().Key(TabletId).Update(
            NIceDb::TUpdate<Schema::Tablet::LockedToActor>(tablet->LockedToActor),
            NIceDb::TUpdate<Schema::Tablet::LockedReconnectTimeout>(tablet->LockedReconnectTimeout.MilliSeconds()));

        Status = NKikimrProto::OK;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxUnlockTabletExecution::Complete TabletId: " << TabletId 
                << " Status: " << Status << " " << StatusMessage);

        if (Status == NKikimrProto::OK) {
            if (PreviousOwner) {
                // Notify previous owner that its lock ownership has been lost
                ctx.Send(PreviousOwner, new TEvHive::TEvLockTabletExecutionLost(TabletId));
            }

            if (TLeaderTabletInfo* tablet = Self->FindTablet(TabletId)) {
                // Tablet still exists by the time transaction finished
                if (!tablet->IsLockedToActor()) {
                    // Try to boot it if possible
                    tablet->TryToBoot(); 
                }
            }
        }

        if (Sender) {
            ctx.Send(Sender, new TEvHive::TEvUnlockTabletExecutionResult(TabletId, Status, StatusMessage), 0, Cookie);
        }
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

ITransaction* THive::CreateUnlockTabletExecution(ui64 tabletId, ui64 seqNo) {
    return new TTxUnlockTabletExecution(tabletId, seqNo, this);
}

void THive::ScheduleUnlockTabletExecution(TNodeInfo& node) { 
    // Unlock tablets that have been locked by this node
    for (TLeaderTabletInfo* tablet : node.LockedTablets) {
        Y_VERIFY(FindTabletEvenInDeleting(tablet->Id) == tablet);
        Y_VERIFY(tablet->LockedToActor.NodeId() == node.Id);
        if (tablet->PendingUnlockSeqNo == 0) {
            tablet->PendingUnlockSeqNo = NextTabletUnlockSeqNo++;
            Y_VERIFY(tablet->PendingUnlockSeqNo != 0);
            auto event = new TEvPrivate::TEvUnlockTabletReconnectTimeout(tablet->Id, tablet->PendingUnlockSeqNo);
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
    BLOG_D("THive::Handle::TEvUnlockTabletReconnectTimeout TabletId=" << tabletId); 
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
        Execute(CreateUnlockTabletExecution(tabletId, seqNo)); 
    }
}

void THive::Handle(TEvHive::TEvUnlockTabletExecution::TPtr& ev) { 
    Execute(CreateUnlockTabletExecution(ev->Get()->Record, ev->Sender, ev->Cookie)); 
}

} // NHive 
} // NKikimr 
