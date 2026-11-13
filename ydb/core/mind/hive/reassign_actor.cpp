#include "hive_impl.h"
#include "hive_log.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/json/json_writer.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
    std::unique_ptr<IReassignCallback> Callback;
    const TString Description;
    ui64 TabletsDone = 0;
    bool Finished = false;
    THive* Hive;
    const bool ContinueInterrupted = false;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TReassignTabletsActor(std::vector<TReassignOperation> operations, const TActorId& source, ui32 maxInFlight, TString description, std::unique_ptr<IReassignCallback> callback, THive* hive)
        : Source(source)
        , Operations(std::move(operations))
        , NextReassign(Operations.begin())
        , MaxInFlight(maxInFlight)
        , Callback(std::move(callback))
        , Description(std::move(description))
        , Hive(hive)
    {}

    TReassignTabletsActor(std::vector<TReassignOperation> operations, THive* hive)
        : Operations(std::move(operations))
        , NextReassign(Operations.begin())
        , MaxInFlight(1)
        , Hive(hive)
        , ContinueInterrupted(true)
    {}

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        Hive->ReassignResults.push_back({GetId(), TActivationContext::Now() + TDuration::Minutes(10), GetResult(true)});
        Hive->PruneReassignResults();
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

    NKikimrReassignTablets::TEvGetResponse GetResult(bool ready = false) const {
        NKikimrReassignTablets::TEvGetResponse result;
        result.SetStatus(Ydb::StatusIds::SUCCESS);
        result.SetReady(ready);
        result.SetCancelled(ready && !Finished);
        result.SetSelectedTablets(Operations.size());
        result.SetCompletedTablets(TabletsDone);
        return result;
    }

    void CheckCompletion() {
        if (ReassignInFlight == 0 && NextReassign == Operations.end()) {
            Finished = true;
            if (Source) {
                Send(Source, Callback->MakeEvent(TabletsDone));
            }
            Hive->LastReassignStatus = TStringBuilder() << "Last actor reassign: " << Description << " at " << TActivationContext::Now();
            Hive->Execute(new TTxUpdateLastReassign(Hive));
            return PassAway();
        }
    }

    bool CanReassignNextTablet() const {
        return ReassignInFlight < MaxInFlight && NextReassign != Operations.end();
    }

    bool ContinueReassign(TLeaderTabletInfo& tablet) {
        TSideEffects sideEffects;
        sideEffects.Reset(SelfId());
        switch (tablet.State) {
            case ETabletState::BlockStorage: {
                tablet.InitiateBlockStorage(sideEffects);
                break;
            }
            case ETabletState::GroupAssignment: {
                tablet.InitiateAssignTabletGroups();
                break;
            }
            default: {
                YDB_LOG_DEBUG("Reassign: tablet is not in the middle of a reassign, skipping it",
                    {"logPrefix", GetLogPrefix()},
                    {"tabletId", tablet.Id},
                    {"state", ETabletStateName(tablet.State)});
                return false;
            }
        }
        sideEffects.Complete(TActivationContext::AsActorContext(), Hive->Requests);
        return true;
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
            if (ContinueInterrupted) {
                if (!ContinueReassign(*tablet)) {
                    std::erase(tablet->ActorsToNotifyOnRestart, SelfId());
                    continue;
                }
            } else {
                Send(Hive->SelfId(), operation.ToEvent());
            }
            ++ReassignInFlight;
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr&) {
        --ReassignInFlight;
        ++TabletsDone;
        ReassignNextTablet();
        return CheckCompletion();
    }

    void Handle(TEvPrivate::TEvRestartCancelled::TPtr& ev) {
        if (TTabletInfo* tablet = Hive->FindTablet(ev->Get()->TabletId)) {
            if (std::erase(tablet->ActorsToNotifyOnRestart, SelfId()) == 0) {
                return;
            }
        } else {
            return;
        }
        --ReassignInFlight;
        ReassignNextTablet();
        return CheckCompletion();
    }

    void Bootstrap() {
        ++Hive->ReassignsRunning;
        Hive->TabletCounters->Cumulative()[NHive::COUNTER_REASSIGN_EXECUTED].Increment(1);
        Become(&TThis::StateWork);
        ReassignNextTablet();
        return CheckCompletion();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
            hFunc(TEvPrivate::TEvRestartCancelled, Handle);
        }
    }
};

TSubActorId THive::StartReassignActor(std::vector<TReassignOperation> operations, const TActorId& source, ui32 maxInFlight, TString description, std::unique_ptr<IReassignCallback> callback) {
    auto* actor = new TReassignTabletsActor(std::move(operations), source, maxInFlight, std::move(description), std::move(callback), this);
    SubActors.emplace_back(actor);
    return RegisterWithSameMailbox(actor).LocalId();
}

void THive::Handle(TEvReassignTablets::TEvCancelRequest::TPtr& ev) {
    const auto& request = ev->Get()->Record;
    auto reply = std::make_unique<TEvReassignTablets::TEvCancelResponse>();
    reply->Record.SetStatus(Ydb::StatusIds::NOT_FOUND);
    if (request.GetHiveGeneration() == HiveGeneration) {
        reply->Record.SetStatus(GetReassignTabletsResult(request.GetReassignId()).GetStatus());
        for (auto* subActor : SubActors) {
            if (subActor->GetId() != request.GetReassignId()) {
                continue;
            }
            if (auto* reassign = dynamic_cast<TReassignTabletsActor*>(subActor)) {
                reassign->Cleanup();
                reply->Record.SetStatus(Ydb::StatusIds::SUCCESS);
                YDB_LOG_INFO("Hive API: reassign stopped",
                    {"logPrefix", GetLogPrefix()},
                    {"user", request.GetUserSID()},
                    {"reassignId", request.GetReassignId()});
            }
            break;
        }
    }
    Send(ev->Sender, reply.release(), 0, ev->Cookie);
}

void THive::PruneReassignResults() {
    const auto now = TActivationContext::Now();
    while (!ReassignResults.empty() && (ReassignResults.front().ExpiresAt <= now || ReassignResults.size() > 1024)) {
        ReassignResults.pop_front();
    }
}

NKikimrReassignTablets::TEvGetResponse THive::GetReassignTabletsResult(TSubActorId id) {
    PruneReassignResults();
    for (auto* subActor : SubActors) {
        if (subActor->GetId() == id) {
            if (auto* reassign = dynamic_cast<TReassignTabletsActor*>(subActor)) {
                return reassign->GetResult();
            }
        }
    }
    for (const auto& result : ReassignResults) {
        if (result.Id == id) {
            return result.Record;
        }
    }
    NKikimrReassignTablets::TEvGetResponse result;
    result.SetStatus(Ydb::StatusIds::NOT_FOUND);
    return result;
}

void THive::Handle(TEvReassignTablets::TEvGetRequest::TPtr& ev) {
    const auto& request = ev->Get()->Record;
    auto reply = std::make_unique<TEvReassignTablets::TEvGetResponse>();
    reply->Record.SetStatus(Ydb::StatusIds::NOT_FOUND);
    if (request.GetHiveGeneration() == HiveGeneration) {
        reply->Record = GetReassignTabletsResult(request.GetReassignId());
    }
    Send(ev->Sender, reply.release(), 0, ev->Cookie);
}

void THive::ContinueInterruptedReassigns(std::vector<TReassignOperation> operations) {
    auto* actor = new TReassignTabletsActor(std::move(operations), this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

} // NKikimr::NHive
