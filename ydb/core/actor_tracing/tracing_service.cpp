#include "tracing_service.h"
#include "tracing_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/tracer.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NActorTracing {

    using namespace NActors;

    class TActorTracingServiceActor : public TActorBootstrapped<TActorTracingServiceActor> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::ACTOR_TRACING_SERVICE;
        }

        void Bootstrap(const TActorContext&) {
            Become(&TThis::StateWork);
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTracing::TEvTraceStart, Handle);
                HFunc(TEvTracing::TEvTraceStop, Handle);
                HFunc(TEvTracing::TEvTraceFetch, Handle);
            }
        }

        void Handle(TEvTracing::TEvTraceStart::TPtr& ev, const TActorContext& ctx) {
            auto* tracer = ctx.ActorSystem()->GetActorTracer();
            auto result = MakeHolder<TEvTracing::TEvTraceStartResult>();
            if (tracer) {
                tracer->Start();
                result->Record.SetSuccess(true);
            } else {
                result->Record.SetSuccess(false);
                result->Record.SetError("Tracer not available");
            }
            ctx.Send(ev->Sender, result.Release());
        }

        void Handle(TEvTracing::TEvTraceStop::TPtr& ev, const TActorContext& ctx) {
            auto* tracer = ctx.ActorSystem()->GetActorTracer();
            auto result = MakeHolder<TEvTracing::TEvTraceStopResult>();
            if (tracer) {
                tracer->Stop();
                result->Record.SetSuccess(true);
            } else {
                result->Record.SetSuccess(false);
                result->Record.SetError("Tracer not available");
            }
            ctx.Send(ev->Sender, result.Release());
        }

        void Handle(TEvTracing::TEvTraceFetch::TPtr& ev, const TActorContext& ctx) {
            auto* tracer = ctx.ActorSystem()->GetActorTracer();
            auto result = MakeHolder<TEvTracing::TEvTraceFetchResult>();
            if (tracer) {
                auto chunk = tracer->GetTraceData();
                ui32 nodeId = ctx.SelfID.NodeId();
                auto buf = NActors::NTracing::SerializeTrace(chunk, nodeId);
                result->Record.SetSuccess(true);
                result->Record.SetTraceData(buf.Data(), buf.Size());
            } else {
                result->Record.SetSuccess(false);
                result->Record.SetError("Tracer not available");
            }
            ctx.Send(ev->Sender, result.Release());
        }
    };

    NActors::IActor* CreateActorTracingService() {
        return new TActorTracingServiceActor();
    }

} // namespace NKikimr::NActorTracing
