#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxStopTablet : public TTransactionBase<THive> {
    TTabletId TabletId;
    TActorId ActorToNotify;
    TSideEffects SideEffects;

public:
    TTxStopTablet(ui64 tabletId, const TActorId &actorToNotify, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , ActorToNotify(actorToNotify)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_STOP_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxStopTablet::Execute Tablet: " << TabletId);
        SideEffects.Reset(Self->SelfId());
        NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            ETabletState state = tablet->State;
            ETabletState newState = state;
            NIceDb::TNiceDb db(txc.DB);
            switch (state) {
            case ETabletState::GroupAssignment:
                // switch to StoppingInGroupAssignment
                newState = ETabletState::StoppingInGroupAssignment;
                status = NKikimrProto::OK;
                // TODO: Notify of previous request failure
                // TODO: Set new notification receiver
                break;
            case ETabletState::Stopped:
                // notify with OK
                status = NKikimrProto::ALREADY;
                break;
            case ETabletState::ReadyToWork:
                // Switch to Stopping
                newState = ETabletState::Stopped;
                for (TTabletInfo& follower : tablet->Followers) {
                    if (follower.IsAlive()) {
                        follower.InitiateStop(SideEffects);
                        db.Table<Schema::TabletFollowerTablet>().Key(follower.GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                    }
                }
                if (tablet->IsAlive()) {
                    tablet->InitiateStop(SideEffects);
                    db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::LeaderNode>(0);
                }
                status = NKikimrProto::OK;
                break;
            case ETabletState::Deleting:
                status = NKikimrProto::ERROR;
                break;
            case ETabletState::BlockStorage:
                status = NKikimrProto::ERROR;
                break;
            case ETabletState::Stopping:
            case ETabletState::StoppingInGroupAssignment:
            case ETabletState::Unknown:
                status = NKikimrProto::ERROR;
                break;
            }
            if (status == NKikimrProto::OK && newState != state) {
                db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::State>(newState);
                tablet->State = newState;
            }
            if (status != NKikimrProto::UNKNOWN) {
                SideEffects.Send(ActorToNotify, new NEvHive::TEvStopTabletResult(status, TabletId), 0, 0);
                Self->ReportStoppedToWhiteboard(*tablet);
                BLOG_D("Report tablet " << tablet->ToString() << " as stopped to Whiteboard");
            }
            Self->ProcessBootQueue();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxStopTablet::Complete TabletId: " << TabletId);
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateStopTablet(TTabletId tabletId, const TActorId &actorToNotify) {
    return new TTxStopTablet(tabletId, actorToNotify, this);
}

} // NHive
} // NKikimr


