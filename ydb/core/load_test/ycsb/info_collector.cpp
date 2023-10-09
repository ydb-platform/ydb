#include "info_collector.h"
#include "test_load_actor.h"

namespace NKikimr::NDataShardLoad {

namespace {

constexpr ui64 WakeupMs = 500;

class TInfoCollector : public TActorBootstrapped<TInfoCollector> {
    const TActorId Parent;
    TVector<TActorId> Actors;

    TMap<ui64, NKikimrDataShardLoad::TLoadReport> Results;

    ui64 ResponsesPending = 0;

public:
    TInfoCollector(const TActorId& parent, TVector<TActorId>&& actors)
        : Parent(parent)
        , Actors(std::move(actors))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TInfoCollector::StateFunc);

        ResponsesPending = Actors.size();
        for (const auto& actorId: Actors) {
            ctx.Send(actorId, new TEvDataShardLoad::TEvTestLoadInfoRequest());
        }

        ctx.Schedule(TDuration::MilliSeconds(WakeupMs), new TEvents::TEvWakeup());
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.ReportsSize() == 1);
        Results[record.GetReports(0).GetTag()] = std::move(record.GetReports(0));

        --ResponsesPending;
        if (ResponsesPending == 0) {
            Reply(ctx);
            Die(ctx);
        }
    }

    void Reply(const TActorContext& ctx) {
        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadInfoResponse>();
        for (auto& it: Results) {
            *response->Record.AddReports() = std::move(it.second);
        }

        ctx.Send(Parent, response.release());
    }

    void HandleWakeup(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TInfoCollector# " << Parent
            << " actor timeout: waiting# " << ResponsesPending << " out of actorsCount# " << Actors.size());

        Reply(ctx);
        Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TInfoCollector# " << Parent
            << " actor recieved PoisonPill");
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadInfoResponse, Handle)
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
    )
};

} // anonymous

// Requests info from all actors, replies with accumulated result to parent
IActor *CreateInfoCollector(const TActorId& parent, TVector<TActorId>&& actors) {
    return new TInfoCollector(parent, std::move(actors));
}

} // NKikimr::NDataShardLoad
