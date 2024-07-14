#include "melancholic_gopher.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <util/system/hp_timer.h>
#include <util/system/spinlock.h>

namespace NActors {

class TMelancholicGopher : public TActor<TMelancholicGopher> {
    const double SurveyForSeconds;
    const TActorId ReportTo;

    void Round(const TActorContext &ctx) {
        if (SurveyForSeconds > 0.0) {
            THPTimer timer;
            while (timer.Passed() < SurveyForSeconds)
                SpinLockPause();
        }
        ctx.Send(ReportTo, new TEvents::TEvWakeup());
    }
public:
    static constexpr EActivityType ActorActivityType() {
        return EActivityType::ACTORLIB_COMMON;
    }

    TMelancholicGopher(double surveyForSeconds, const TActorId &reportTo)
        : TActor(&TThis::StateFunc)
        , SurveyForSeconds(surveyForSeconds)
        , ReportTo(reportTo)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Round);
        }
    }

};

class TGopherMother : public TActorBootstrapped<TGopherMother> {
    const TVector<std::pair<ui32, double>> LineProfile;
    const ui32 Lines;
    const ui32 ShotsInRound;
    TVector<TActorId> HeadGophers;
    ui32 WaitFor;

    TInstant RoundStart;

    void Round(const TActorContext &ctx) {
        RoundStart = ctx.Now();

        for (const auto &head : HeadGophers)
            for (ui32 i = ShotsInRound; i > 0; --i) {
                ctx.Send(head, new TEvents::TEvWakeup());
                ++WaitFor;
            }
    }

    void Response(const TActorContext &ctx) {
        if (--WaitFor == 0) {
            const TDuration roundTime = ctx.Now() - RoundStart;
            LOG_INFO_S(ctx, NActorsServices::EServiceCommon::TEST, "Gopher Mother round for " << roundTime.ToString());
            Round(ctx);
        }
    }
public:
    static constexpr EActivityType ActorActivityType() {
        return EActivityType::ACTORLIB_COMMON;
    }

    TGopherMother(const TVector<std::pair<ui32, double>> &lineProfile, ui32 lines, ui32 shotsInRound)
        : LineProfile(lineProfile)
        , Lines(lines)
        , ShotsInRound(shotsInRound)
        , WaitFor(0)
    {
        Y_ABORT_UNLESS(!LineProfile.empty());
    }

    void Bootstrap(const TActorContext &ctx) {
        HeadGophers.reserve(Lines);
        for (ui32 lines = Lines; lines > 0; --lines) {
            HeadGophers.push_back(TActorId());
            TActorId &head = HeadGophers.back();
            head = ctx.SelfID;
            for (const auto &xpair : LineProfile)
                head = ctx.ExecutorThread.ActorSystem->Register(CreateMelancholicGopher(xpair.second, head), TMailboxType::Simple, xpair.first);
        }

        Round(ctx);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Response);
        }
    }
};

IActor* CreateMelancholicGopher(double surveyForSeconds, const TActorId &reportTo) {
    return new TMelancholicGopher(surveyForSeconds, reportTo);
}


IActor* CreateGopherMother(const TVector<std::pair<ui32, double>> &lineProfile, ui32 lines, ui32 shotsInRound) {
    return new TGopherMother(lineProfile, lines, shotsInRound);
}

}
