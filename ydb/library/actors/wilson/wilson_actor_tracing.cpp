#include "wilson_actor_tracing.h"
#include "wilson_span.h"
#include "wilson_process_epoch.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/system/datetime.h>

namespace NWilson {
namespace {
class TActorTraceSpan : public NActors::IActorTraceSpan {
public:
    TActorTraceSpan(NActors::IEventHandle& event, NActors::TActorSystem* system,
            ui32 pool, bool handler, ui32 activity = 0)
        : Span(15, TTraceId(event.TraceId), handler ? "Actor.Handler" : "Actor.MailboxSubmit", EFlags::AUTO_END, system)
        , Handler(handler)
    {
        Span.Attribute("ydb.process.epoch", static_cast<i64>(GetProcessTraceEpoch()));
        Span.Attribute("ydb.actor.pool_id", static_cast<i64>(pool));
        Span.Attribute("ydb.actor.event_type", static_cast<i64>(event.GetTypeRewrite()));
        if (Handler) {
            Span.Attribute("ydb.actor.activity_type", static_cast<i64>(activity));
        }
        Span.Event(Handler ? "handler entered" : "mailbox push entered",
            {{"monotonic_us", static_cast<i64>(TMonotonic::Now().MicroSeconds())}});
        event.TraceId = Span.GetTraceId();
        Started = TMonotonic::Now();
        CpuStarted = Handler ? ThreadCPUTime() : 0;
    }

    void Finish(bool acceptedOrReturned) override {
        if (!Span) {
            return;
        }
        Span.Event(Handler ? "handler returned" : "mailbox push returned",
            {{"monotonic_us", static_cast<i64>(TMonotonic::Now().MicroSeconds())}});
        if (Handler) {
            Span.Attribute("ydb.handler.wall_us", static_cast<i64>((TMonotonic::Now() - Started).MicroSeconds()));
            Span.Attribute("ydb.handler.thread_cpu_us", static_cast<i64>(ThreadCPUTime() - CpuStarted));
            Span.Attribute("ydb.handler.returned", acceptedOrReturned);
        } else {
            Span.Attribute("ydb.actor.enqueue_accepted", acceptedOrReturned);
            Span.Attribute("ydb.actor.enqueue_boundary", TString("push_interval"));
        }
        Span.End();
    }

private:
    TSpan Span;
    const bool Handler;
    TMonotonic Started;
    ui64 CpuStarted = 0;
};

class TActorEventTracing : public NActors::IActorEventTracing {
public:
    std::unique_ptr<NActors::IActorTraceSpan> BeginMailboxSubmit(NActors::IEventHandle& event,
            NActors::TActorSystem* system, ui32 pool) const override {
        if (!event.TraceId || event.TraceId.GetVerbosity() < 15) {
            return {};
        }
        return std::make_unique<TActorTraceSpan>(event, system, pool, false);
    }

    std::unique_ptr<NActors::IActorTraceSpan> BeginHandler(NActors::IEventHandle& event,
            NActors::TActorSystem* system, ui32 pool, ui32 activity) const override {
        if (!event.TraceId || event.TraceId.GetVerbosity() < 15) {
            return {};
        }
        return std::make_unique<TActorTraceSpan>(event, system, pool, true, activity);
    }
};
} // namespace

std::shared_ptr<NActors::IActorEventTracing> CreateActorEventTracing() {
    return std::make_shared<TActorEventTracing>();
}
} // namespace NWilson
