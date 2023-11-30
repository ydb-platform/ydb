#include "metrics_printer.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/monlib/encode/format.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {
    using namespace NActors;

    class TCountersPrinter : public TActorBootstrapped<TCountersPrinter> {
    public:
        static constexpr char ActorName[] = "COUNTERS_PRINTER";

        TCountersPrinter(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TDuration duration)
            : TActorBootstrapped<TCountersPrinter>(), Counters(std::move(counters)), Duration(duration)
        { }

        void Bootstrap() {
            Become(&TThis::Handler, Duration, new TEvents::TEvWakeup());
        }

    private:
        STRICT_STFUNC(Handler,
        {
            cFunc(TEvents::TSystem::Wakeup, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        });

        void Handle() {
            TString output;
            {
                TStringOutput ostream(output);
                Counters->OutputPlainText(ostream);
            }
            YQL_CLOG(DEBUG, ProviderDq) << "Counters: " << output;
            Schedule(Duration, new TEvents::TEvWakeup());
        }

        TIntrusivePtr <NMonitoring::TDynamicCounters> Counters;
        TDuration Duration;
    };

    NActors::IActor* CreateMetricsPrinter(const NMonitoring::TDynamicCounterPtr& counters)
    {
        return new TCountersPrinter(counters, TDuration::Seconds(30));
    };
} // namespace NYql
