#include "metrics_actor.h"
#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/cache/cache.h>

#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NKikimr::NHttpProxy {

    using namespace NActors;

    class TMetricsActor : public NActors::TActorBootstrapped<TMetricsActor> {
        using TBase = NActors::TActorBootstrapped<TMetricsActor>;
    public:
        explicit TMetricsActor(const TMetricsSettings& settings)
            : Settings(settings)
        {
        }

        void Bootstrap(const TActorContext&) {
            TBase::Become(&TMetricsActor::StateWork);
        }

        TStringBuilder LogPrefix() const {
            return {};
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvServerlessProxy::TEvCounter, Handle);
                HFunc(TEvServerlessProxy::TEvHistCounter, Handle);

            }
        }

        void Handle(TEvServerlessProxy::TEvCounter::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvServerlessProxy::TEvHistCounter::TPtr& ev, const TActorContext& ctx);


        TMetricsSettings Settings;
    };

    void TMetricsActor::Handle(TEvServerlessProxy::TEvCounter::TPtr& ev, const TActorContext&) {
        Y_ABORT_UNLESS(ev->Get()->Labels.size() > 1);
        auto group = Settings.Counters->GetSubgroup(ev->Get()->Labels[0].first, ev->Get()->Labels[0].second);
        for (ui32 i = 1; i + 1 < ev->Get()->Labels.size(); ++i) {
            if (ev->Get()->Labels[i].second.empty())
                continue;
            group = group->GetSubgroup(ev->Get()->Labels[i].first, ev->Get()->Labels[i].second);
        }
        auto counter = group->GetNamedCounter(ev->Get()->Labels.back().first, ev->Get()->Labels.back().second, ev->Get()->Derivative);
        if (ev->Get()->Additive) *counter += ev->Get()->Delta;
        else *counter = ev->Get()->Delta;
    }

    void TMetricsActor::Handle(TEvServerlessProxy::TEvHistCounter::TPtr& ev, const TActorContext&) {
        Y_ABORT_UNLESS(ev->Get()->Labels.size() > 1);
        auto group = Settings.Counters->GetSubgroup(ev->Get()->Labels[0].first, ev->Get()->Labels[0].second);
        for (ui32 i = 1; i + 1 < ev->Get()->Labels.size(); ++i) {
            if (ev->Get()->Labels[i].second.empty())
                continue;
            group = group->GetSubgroup(ev->Get()->Labels[i].first, ev->Get()->Labels[i].second);
        }
        auto counter = group->GetNamedHistogram(ev->Get()->Labels.back().first, ev->Get()->Labels.back().second,
                                                    NMonitoring::ExplicitHistogram({100, 200, 500, 1000, 2000, 5000, 10000, 30000}));
        counter->Collect(ev->Get()->Value, ev->Get()->Count);
    }


    NActors::IActor* CreateMetricsActor(const TMetricsSettings& settings) {
        return new TMetricsActor(settings);
    }

}
