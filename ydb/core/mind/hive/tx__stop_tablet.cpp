#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxStopTablet : public TTransactionBase<THive> {
    const TTabletId TabletId;
    const TActorId ActorToNotify;
    NKikimrProto::EReplyStatus Status;

public:
    TTxStopTablet(ui64 tabletId, const TActorId &actorToNotify, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , ActorToNotify(actorToNotify)
        , Status(NKikimrProto::UNKNOWN)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_STOP_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override { 
        BLOG_D("THive::TTxStopTablet::Execute Tablet: " << TabletId);
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            ETabletState State = tablet->State;
            ETabletState NewState = State;
            NIceDb::TNiceDb db(txc.DB);
            switch (State) {
            case ETabletState::GroupAssignment:
                // switch to StoppingInGroupAssignment
                NewState = ETabletState::StoppingInGroupAssignment;
                Status = NKikimrProto::OK;
                // TODO: Notify of previous request failure
                // TODO: Set new notification receiver
                break;
            case ETabletState::Stopped:
                // notify with OK
                Status = NKikimrProto::ALREADY;
                break;
            case ETabletState::ReadyToWork:
                // Switch to Stopping
                NewState = ETabletState::Stopped;
                for (TTabletInfo& follower : tablet->Followers) {
                    if (follower.IsAlive()) {
                        follower.InitiateStop();
                        db.Table<Schema::TabletFollowerTablet>().Key(follower.GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                    }
                }
                if (tablet->IsAlive()) {
                    tablet->InitiateStop();
                    db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::LeaderNode>(0);
                }
                Status = NKikimrProto::OK;
                break;
            case ETabletState::Deleting:
                Status = NKikimrProto::ERROR;
                break;
            case ETabletState::BlockStorage:
                Status = NKikimrProto::ERROR;
                break;
            case ETabletState::Stopping:
            case ETabletState::StoppingInGroupAssignment:
            case ETabletState::Unknown:
                Status = NKikimrProto::ERROR;
                break;
            }
            if (Status == NKikimrProto::OK && NewState != State) {
                db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::State>(NewState);
                tablet->State = NewState;
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override { 
        BLOG_D("THive::TTxStopTablet::Complete TabletId: " << TabletId);
        if (Status != NKikimrProto::UNKNOWN) {
            ctx.Send(ActorToNotify, new TEvHive::TEvStopTabletResult(Status, TabletId), 0, 0);
            TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
            if (tablet != nullptr) {
                Self->ReportStoppedToWhiteboard(*tablet);
                BLOG_D("Report tablet " << tablet->ToString() << " as stopped to Whiteboard");
            }
        }
        Self->ProcessBootQueue();
    }
};

ITransaction* THive::CreateStopTablet(TTabletId tabletId, const TActorId &actorToNotify) {
    return new TTxStopTablet(tabletId, actorToNotify, this);
}

} // NHive
} // NKikimr


