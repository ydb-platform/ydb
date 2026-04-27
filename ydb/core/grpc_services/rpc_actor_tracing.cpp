#include "service_actor_tracing.h"
#include "rpc_deferrable.h"

#include <ydb/core/actor_tracing/tracing_events.h>
#include <ydb/core/actor_tracing/tree_broadcast.h>
#include <ydb/public/api/protos/ydb_actor_tracing.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/services/services.pb.h>

#include <util/datetime/base.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvTraceStartRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceStartRequest, Ydb::ActorTracing::TraceStartResponse>;
using TEvTraceStopRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceStopRequest, Ydb::ActorTracing::TraceStopResponse>;
using TEvTraceFetchRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceFetchRequest, Ydb::ActorTracing::TraceFetchResponse>;

static constexpr TDuration FetchRpcTimeout = TDuration::Seconds(15);

static TVector<ui32> CollectNodeIds(const TEvInterconnect::TEvNodesInfo& info) {
    TVector<ui32> ids;
    ids.reserve(info.Nodes.size());
    for (const auto& node : info.Nodes) {
        ids.push_back(node.NodeId);
    }
    return ids;
}

template <typename TActor>
bool TryDispatchExplicitNodeIds(TActor* self, const TActorContext& ctx) {
    const auto* req = self->GetProtoRequest();
    if (req->node_ids_size() == 0) {
        ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        return false;
    }
    TVector<ui32> nodeIds(req->node_ids().begin(), req->node_ids().end());
    self->OnNodesResolved(std::move(nodeIds), ctx);
    return true;
}

class TActorTracingStartRPC : public TRpcOperationRequestActor<TActorTracingStartRPC, TEvTraceStartRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingStartRPC, TEvTraceStartRequest>;
public:
    using TBase::TBase;
    using TBase::GetProtoRequest;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TThis::StateWork);
        TryDispatchExplicitNodeIds(this, ctx);
    }

    void OnNodesResolved(TVector<ui32> nodeIds, const TActorContext& ctx) {
        for (auto& [nodeId, subtree] : NActorTracing::GetDirectChildren(nodeIds)) {
            auto msg = MakeHolder<NActorTracing::TEvTracing::TEvTraceStart>();
            for (ui32 id : subtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(NActorTracing::MakeActorTracingServiceId(nodeId), msg.Release());
        }

        Ydb::ActorTracing::TraceStartResult result;
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        OnNodesResolved(CollectNodeIds(*ev->Get()), ctx);
    }
};

class TActorTracingStopRPC : public TRpcOperationRequestActor<TActorTracingStopRPC, TEvTraceStopRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingStopRPC, TEvTraceStopRequest>;
public:
    using TBase::TBase;
    using TBase::GetProtoRequest;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TThis::StateWork);
        TryDispatchExplicitNodeIds(this, ctx);
    }

    void OnNodesResolved(TVector<ui32> nodeIds, const TActorContext& ctx) {
        for (auto& [nodeId, subtree] : NActorTracing::GetDirectChildren(nodeIds)) {
            auto msg = MakeHolder<NActorTracing::TEvTracing::TEvTraceStop>();
            for (ui32 id : subtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(NActorTracing::MakeActorTracingServiceId(nodeId), msg.Release());
        }

        Ydb::ActorTracing::TraceStopResult result;
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        OnNodesResolved(CollectNodeIds(*ev->Get()), ctx);
    }
};

class TActorTracingFetchRPC : public TRpcOperationRequestActor<TActorTracingFetchRPC, TEvTraceFetchRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingFetchRPC, TEvTraceFetchRequest>;
public:
    using TBase::TBase;
    using TBase::GetProtoRequest;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TThis::StateWork);
        TryDispatchExplicitNodeIds(this, ctx);
    }

    void OnNodesResolved(TVector<ui32> nodeIds, const TActorContext& ctx) {
        auto rootChildren = NActorTracing::GetDirectChildren(nodeIds);

        PendingCount = rootChildren.size();
        if (PendingCount == 0) {
            ReplyAndDie(ctx);
            return;
        }

        for (auto& [nodeId, subtree] : rootChildren) {
            auto msg = MakeHolder<NActorTracing::TEvTracing::TEvTraceFetch>();
            for (ui32 id : subtree) {
                msg->Record.AddSubtreeNodeIds(id);
            }
            ctx.Send(NActorTracing::MakeActorTracingServiceId(nodeId), msg.Release(), IEventHandle::FlagTrackDelivery);
        }

        ctx.Schedule(FetchRpcTimeout, new TEvents::TEvWakeup());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
            HFunc(NActorTracing::TEvTracing::TEvTraceFetchResult, HandleResult);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        OnNodesResolved(CollectNodeIds(*ev->Get()), ctx);
    }

    void HandleResult(NActorTracing::TEvTracing::TEvTraceFetchResult::TPtr& ev, const TActorContext& ctx) {
        auto& rec = ev->Get()->Record;
        if (rec.GetSuccess() && rec.GetTraceData().size() > BestTrace.size()) {
            BestTrace = rec.GetTraceData();
        }
        if (++DoneCount >= PendingCount) {
            ReplyAndDie(ctx);
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        if (++DoneCount >= PendingCount) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext& ctx) {
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        Ydb::ActorTracing::TraceFetchResult result;
        result.set_trace_data(BestTrace);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    ui32 PendingCount = 0;
    ui32 DoneCount = 0;
    TString BestTrace;
};

void DoActorTracingStart(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TActorTracingStartRPC(p.release()));
}

void DoActorTracingStop(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TActorTracingStopRPC(p.release()));
}

void DoActorTracingFetch(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TActorTracingFetchRPC(p.release()));
}

} // namespace NKikimr::NGRpcService
