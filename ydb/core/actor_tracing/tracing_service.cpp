#include "tracing_service.h"
#include "tracing_events.h"
#include "tree_broadcast.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/tracer.h>
#include <ydb/library/services/services.pb.h>

#include <util/datetime/base.h>

namespace NKikimr::NActorTracing {

using namespace NActors;

static constexpr TDuration FetchGathererTimeout = TDuration::Seconds(15);

class TTraceFetchGatherer : public TActorBootstrapped<TTraceFetchGatherer> {
public:
    TTraceFetchGatherer(TActorId replyTo, TString localTrace, TVector<ui32> subtreeNodeIds)
        : ReplyTo(replyTo)
        , BestTrace(std::move(localTrace))
        , SubtreeNodeIds(std::move(subtreeNodeIds))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        auto children = GetDirectChildren(SubtreeNodeIds);
        PendingCount = children.size();

        if (PendingCount == 0) {
            Reply(ctx);
            return;
        }

        for (auto& [childId, childSubtree] : children) {
            auto msg = MakeHolder<TEvTracing::TEvTraceFetch>();
            for (ui32 id : childSubtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(MakeActorTracingServiceId(childId), msg.Release(), IEventHandle::FlagTrackDelivery);
        }

        ctx.Schedule(FetchGathererTimeout, new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTracing::TEvTraceFetchResult, Handle);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvTracing::TEvTraceFetchResult::TPtr& ev, const TActorContext& ctx) {
        auto& rec = ev->Get()->Record;
        if (rec.GetSuccess() && rec.GetTraceData().size() > BestTrace.size()) {
            BestTrace = rec.GetTraceData();
        }
        if (++DoneCount >= PendingCount) {
            Reply(ctx);
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        if (++DoneCount >= PendingCount) {
            Reply(ctx);
        }
    }

    void HandleTimeout(const TActorContext& ctx) {
        Reply(ctx);
    }

    void Reply(const TActorContext& ctx) {
        auto result = MakeHolder<TEvTracing::TEvTraceFetchResult>();
        result->Record.SetSuccess(true);
        result->Record.SetTraceData(BestTrace);
        ctx.Send(ReplyTo, result.Release());
        PassAway();
    }

    TActorId ReplyTo;
    TString BestTrace;
    TVector<ui32> SubtreeNodeIds;
    ui32 PendingCount = 0;
    ui32 DoneCount = 0;
};

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

    template <typename TEvMsg>
    static TVector<ui32> ExtractSubtree(const TEvMsg& ev) {
        const auto& ids = ev.Record.GetSubtreeNodeIds();
        return TVector<ui32>(ids.begin(), ids.end());
    }

    void Handle(TEvTracing::TEvTraceStart::TPtr& ev, const TActorContext& ctx) {
        if (auto* tracer = ctx.ActorSystem()->GetActorTracer()) {
            tracer->Start();
        }

        auto subtree = ExtractSubtree(*ev->Get());
        for (auto& [childId, childSubtree] : GetDirectChildren(subtree)) {
            auto msg = MakeHolder<TEvTracing::TEvTraceStart>();
            for (ui32 id : childSubtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(MakeActorTracingServiceId(childId), msg.Release());
        }
    }

    void Handle(TEvTracing::TEvTraceStop::TPtr& ev, const TActorContext& ctx) {
        if (auto* tracer = ctx.ActorSystem()->GetActorTracer()) {
            tracer->Stop();
        }

        auto subtree = ExtractSubtree(*ev->Get());
        for (auto& [childId, childSubtree] : GetDirectChildren(subtree)) {
            auto msg = MakeHolder<TEvTracing::TEvTraceStop>();
            for (ui32 id : childSubtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(MakeActorTracingServiceId(childId), msg.Release());
        }
    }

    void Handle(TEvTracing::TEvTraceFetch::TPtr& ev, const TActorContext& ctx) {
        TString localTrace;
        if (auto* tracer = ctx.ActorSystem()->GetActorTracer()) {
            auto chunk = tracer->GetTraceData();
            auto buf = NActors::NTracing::SerializeTrace(chunk, ctx.SelfID.NodeId());
            localTrace = TString(buf.Data(), buf.Size());
        }

        auto subtree = ExtractSubtree(*ev->Get());
        if (subtree.empty()) {
            auto result = MakeHolder<TEvTracing::TEvTraceFetchResult>();
            result->Record.SetSuccess(true);
            result->Record.SetTraceData(localTrace);
            ctx.Send(ev->Sender, result.Release());
            return;
        }

        ctx.Register(new TTraceFetchGatherer(ev->Sender, std::move(localTrace), std::move(subtree)));
    }
};

NActors::IActor* CreateActorTracingService() {
    return new TActorTracingServiceActor();
}

NActors::IActor* CreateTraceFetchGatherer(NActors::TActorId replyTo, TString localTrace, TVector<ui32> subtreeNodeIds) {
    return new TTraceFetchGatherer(replyTo, std::move(localTrace), std::move(subtreeNodeIds));
}

} // namespace NKikimr::NActorTracing
