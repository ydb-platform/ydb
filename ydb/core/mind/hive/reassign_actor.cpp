#include "hive_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/json/json_writer.h>

namespace NKikimr::NHive {

class TTxUpdateLastReassign : public TTransactionBase<THive> {
public:
    TTxUpdateLastReassign(TSelf* hive) : TTransactionBase(hive) {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::State>().Key(TSchemeIds::LastReassignStatus).Update<Schema::State::StringValue>(Self->LastReassignStatus);
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

class TReassignTabletsActor
    : public TActorBootstrapped<TReassignTabletsActor>
    , public ISubActor
{
public:
    const TActorId Source;
    const std::vector<TReassignOperation> Operations;
    std::vector<TReassignOperation>::const_iterator NextReassign;
    ui32 ReassignInFlight = 0;
    const ui32 MaxInFlight;
    NJson::TJsonValue Response;
    const TString Description;
    ui64 TabletsDone = 0;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TReassignTabletsActor(std::vector<TReassignOperation> operations, const TActorId& source, ui32 maxInFlight, TString description, THive* hive)
        : Source(source)
        , Operations(std::move(operations))
        , NextReassign(Operations.begin())
        , MaxInFlight(maxInFlight)
        , Description(std::move(description))
        , Hive(hive)
    {}

    TReassignTabletsActor(std::vector<TReassignOperation> operations, THive* hive)
        : Operations(std::move(operations))
        , NextReassign(Operations.begin())
        , MaxInFlight(1)
        , Hive(hive)
    {}

    void PassAway() override {
        --Hive->ReassignsRunning;
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TString GetDescription() const override {
        double progress = 0;
        if (!Operations.empty()) {
            progress = 100.0 * (NextReassign - Operations.begin()) / Operations.size();
        }
        return TStringBuilder() << "Reassign(" << Description << "): " << Sprintf("%.2f", progress) << "%";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void CheckCompletion() {
        if (ReassignInFlight == 0 && NextReassign == Operations.end()) {
            if (Source) {
                Response["total"] = TabletsDone;
                Send(Source, new NMon::TEvRemoteJsonInfoRes(NJson::WriteJson(Response, false)));
            }
            Hive->LastReassignStatus = TStringBuilder() << "Last actor reassign: " << Description << " at " << TActivationContext::Now();
            Hive->Execute(new TTxUpdateLastReassign(Hive));
            return PassAway();
        }
    }

    bool CanReassignNextTablet() const {
        return ReassignInFlight < MaxInFlight && NextReassign != Operations.end();
    }

    void ReassignNextTablet() {
        while (CanReassignNextTablet()) {
            const auto& operation = *(NextReassign++);
            TTabletId tabletId = operation.TabletId;
            TLeaderTabletInfo* tablet = Hive->FindTablet(tabletId);
            if (tablet == nullptr) {
                continue;
            }
            tablet->ActorsToNotifyOnRestart.push_back(SelfId());
            ++ReassignInFlight;
            switch (tablet->State) {
                case ETabletState::BlockStorage: {
                    TSideEffects sideEffects;
                    sideEffects.Reset(SelfId());
                    tablet->InitiateBlockStorage(sideEffects);
                    sideEffects.Complete(TActivationContext::AsActorContext());
                    break;
                }
                case ETabletState::GroupAssignment: {
                    tablet->InitiateAssignTabletGroups();
                    break;
                }
                default: {
                    Send(Hive->SelfId(), operation.ToEvent());
                    break;
                }
            }
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr&) {
        --ReassignInFlight;
        ++TabletsDone;
        ReassignNextTablet();
        return CheckCompletion();
    }

    void Bootstrap() {
        ++Hive->ReassignsRunning;
        Become(&TThis::StateWork);
        ReassignNextTablet();
        return CheckCompletion();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
        }
    }
};

void THive::StartReassignActor(std::vector<TReassignOperation> operations, const TActorId& source, ui32 maxInFlight, TString description) {
    auto* actor = new TReassignTabletsActor(std::move(operations), source, maxInFlight, std::move(description), this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

void THive::StartReassignActor(std::vector<TReassignOperation> operations) {
    auto* actor = new TReassignTabletsActor(std::move(operations), this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

} // NKikimr::NHive
