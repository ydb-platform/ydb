#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxResumeTablet : public TTransactionBase<THive> {
    const TTabletId TabletId;
    const TActorId ActorToNotify;
    TSideEffects SideEffects;
    bool ByTenant;

public:
    TTxResumeTablet(ui64 tabletId, const TActorId &actorToNotify, bool fromStopByTenant, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , ActorToNotify(actorToNotify)
        , ByTenant(fromStopByTenant)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RESUME_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxResumeTablet::Execute Tablet: " << TabletId);
        SideEffects.Reset(Self->SelfId());
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            if (ByTenant) {
                TDomainInfo* domain = Self->FindDomain(tablet->NodeFilter.ObjectDomain);
                if (domain == nullptr || domain->Stopped || !tablet->StoppedByTenant) {
                    return true;
                }
            }
            NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
            ETabletState State = tablet->State;
            ETabletState NewState = State;
            NIceDb::TNiceDb db(txc.DB);
            switch (State) {
            case ETabletState::GroupAssignment:
                status = NKikimrProto::ERROR;
                break;
            case ETabletState::ReadyToWork:
                status = NKikimrProto::ALREADY;
                break;
            case ETabletState::Stopped:
                // Switch to ReadyToWork
                if (tablet->ChannelProfileNewGroup.any()) {
                    NewState = ETabletState::GroupAssignment;
                } else {
                    NewState = ETabletState::ReadyToWork;
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
            if (status == NKikimrProto::OK) {
                if (NewState != State) {
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::State>(NewState);
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::StoppedByTenant>(false);
                    tablet->State = NewState;
                    tablet->StoppedByTenant = false;
                }
                if (tablet->IsReadyToBoot()) {
                    tablet->InitiateBoot();
                } else if (tablet->IsReadyToAssignGroups()) {
                    tablet->InitiateAssignTabletGroups();
                }
            }
            if (status != NKikimrProto::UNKNOWN && ActorToNotify) {
                SideEffects.Send(ActorToNotify, new TEvHive::TEvResumeTabletResult(status, TabletId), 0, 0);
            }
            Self->ProcessBootQueue();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxResumeTablet::Complete TabletId: " << TabletId);
        SideEffects.Complete(ctx);
        if (ByTenant) {
            Self->ProcessPendingResumeTablet();
        }
    }
};

ITransaction* THive::CreateResumeTablet(TTabletId tabletId, const TActorId &actorToNotify) {
    return new TTxResumeTablet(tabletId, actorToNotify, false, this);
}

ITransaction* THive::CreateResumeTabletByTenant(TTabletId tabletId) {
    return new TTxResumeTablet(tabletId, {}, true, this);
}

} // NHive
} // NKikimr


