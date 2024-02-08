#include "kafka_metrics_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/cache/cache.h>

#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NKafka {

    using namespace NActors;

    class TKafkaMetricsActor : public NActors::TActorBootstrapped<TKafkaMetricsActor> {
        using TBase = NActors::TActorBootstrapped<TKafkaMetricsActor>;
    public:
        explicit TKafkaMetricsActor(const TKafkaMetricsSettings& settings)
            : Settings(settings)
        {
        }

        void Bootstrap(const TActorContext&) {
            TBase::Become(&TKafkaMetricsActor::StateWork);
        }

        TStringBuilder LogPrefix() const {
            return {};
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKafka::TEvUpdateCounter, Handle);
                HFunc(TEvKafka::TEvUpdateHistCounter, Handle);
            }
        }

        void Handle(TEvKafka::TEvUpdateCounter::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvKafka::TEvUpdateHistCounter::TPtr& ev, const TActorContext& ctx);
        TIntrusivePtr<NMonitoring::TDynamicCounters> GetGroupFromLabels(const TVector<std::pair<TString, TString>>& labels);

    private:
        TKafkaMetricsSettings Settings;
    };

    TIntrusivePtr<NMonitoring::TDynamicCounters> TKafkaMetricsActor::GetGroupFromLabels(const TVector<std::pair<TString, TString>>& labels) {
        Y_ABORT_UNLESS(labels.size() > 1);
        auto group = Settings.Counters->GetSubgroup(labels[0].first, labels[0].second);
        for (ui32 i = 1; i + 1 < labels.size(); ++i) {
            if (labels[i].second.empty())
                continue;
            group = group->GetSubgroup(labels[i].first, labels[i].second);
        }
        return group;
    }

    void TKafkaMetricsActor::Handle(TEvKafka::TEvUpdateCounter::TPtr& ev, const TActorContext&) {
        auto labels = ev->Get()->Labels;
        auto group = GetGroupFromLabels(labels);
        auto counter = group->GetNamedCounter(labels.back().first, labels.back().second, true);
        *counter += ev->Get()->Delta;
    }

    void TKafkaMetricsActor::Handle(TEvKafka::TEvUpdateHistCounter::TPtr& ev, const TActorContext&) {
        auto labels = ev->Get()->Labels;
        auto group = GetGroupFromLabels(labels);
        auto counter = group->GetNamedHistogram(labels.back().first, labels.back().second,
                                                NMonitoring::ExplicitHistogram({100, 200, 500, 1000, 2000, 5000, 10000, 30000}));
        counter->Collect(ev->Get()->Value, ev->Get()->Count);
    }


    NActors::IActor* CreateKafkaMetricsActor(const TKafkaMetricsSettings& settings) {
        return new TKafkaMetricsActor(settings);
    }

} // namespace NKafka
