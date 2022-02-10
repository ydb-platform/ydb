#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxResumeTablet : public TTransactionBase<THive> {
    const TTabletId TabletId;
    const TActorId ActorToNotify;
    NKikimrProto::EReplyStatus Status;

public:
    TTxResumeTablet(ui64 tabletId, const TActorId &actorToNotify, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , ActorToNotify(actorToNotify)
        , Status(NKikimrProto::UNKNOWN)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RESUME_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxResumeTablet::Execute Tablet: " << TabletId);
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId); 
        if (tablet != nullptr) {
            ETabletState State = tablet->State;
            ETabletState NewState = State;
            NIceDb::TNiceDb db(txc.DB);
            switch (State) {
            case ETabletState::GroupAssignment:
                Status = NKikimrProto::ERROR;
                break;
            case ETabletState::ReadyToWork:
                Status = NKikimrProto::ALREADY;
                break;
            case ETabletState::Stopped:
                // Switch to ReadyToWork
                if (tablet->ChannelProfileNewGroup.any()) {
                    NewState = ETabletState::GroupAssignment;
                } else {
                    NewState = ETabletState::ReadyToWork;
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
        BLOG_D("THive::TTxResumeTablet::Complete TabletId: " << TabletId);
        if (Status != NKikimrProto::UNKNOWN) {
            ctx.Send(ActorToNotify, new TEvHive::TEvResumeTabletResult(Status, TabletId), 0, 0);
            if (Status == NKikimrProto::OK) {
                TLeaderTabletInfo* tablet = Self->FindTablet(TabletId); 
                if (tablet != nullptr) {
                    if (tablet->IsReadyToBoot()) {
                        tablet->InitiateBoot();
                    } else if (tablet->IsReadyToAssignGroups()) {
                        tablet->InitiateAssignTabletGroups();
                    }
                }
            }
        }
        Self->ProcessBootQueue();
    }
};

ITransaction* THive::CreateResumeTablet(TTabletId tabletId, const TActorId &actorToNotify) {
    return new TTxResumeTablet(tabletId, actorToNotify, this);
}

} // NHive
} // NKikimr


