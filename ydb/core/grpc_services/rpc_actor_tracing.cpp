#include "service_actor_tracing.h"
#include "rpc_deferrable.h"

#include <ydb/core/actor_tracing/tracing_events.h>
#include <ydb/public/api/protos/ydb_actor_tracing.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvTraceStartRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceStartRequest, Ydb::ActorTracing::TraceStartResponse>;
using TEvTraceStopRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceStopRequest, Ydb::ActorTracing::TraceStopResponse>;
using TEvTraceFetchRequest = TGrpcRequestOperationCall<
    Ydb::ActorTracing::TraceFetchRequest, Ydb::ActorTracing::TraceFetchResponse>;

class TActorTracingStartRPC : public TRpcOperationRequestActor<TActorTracingStartRPC, TEvTraceStartRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingStartRPC, TEvTraceStartRequest>;
public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
            HFunc(NActorTracing::TEvTracing::TEvTraceStartResult, HandleResult);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        for (const auto& node : ev->Get()->Nodes) {
            ctx.Send(NActorTracing::MakeActorTracingServiceId(node.NodeId),
                     new NActorTracing::TEvTracing::TEvTraceStart());
            PendingNodes++;
        }
        if (PendingNodes == 0) {
            Ydb::ActorTracing::TraceStartResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    void HandleResult(NActorTracing::TEvTracing::TEvTraceStartResult::TPtr&, const TActorContext& ctx) {
        if (++DoneNodes >= PendingNodes) {
            Ydb::ActorTracing::TraceStartResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    ui32 PendingNodes = 0;
    ui32 DoneNodes = 0;
};

class TActorTracingStopRPC : public TRpcOperationRequestActor<TActorTracingStopRPC, TEvTraceStopRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingStopRPC, TEvTraceStopRequest>;
public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
            HFunc(NActorTracing::TEvTracing::TEvTraceStopResult, HandleResult);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        for (const auto& node : ev->Get()->Nodes) {
            ctx.Send(NActorTracing::MakeActorTracingServiceId(node.NodeId),
                     new NActorTracing::TEvTracing::TEvTraceStop());
            PendingNodes++;
        }
        if (PendingNodes == 0) {
            Ydb::ActorTracing::TraceStopResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    void HandleResult(NActorTracing::TEvTracing::TEvTraceStopResult::TPtr&, const TActorContext& ctx) {
        if (++DoneNodes >= PendingNodes) {
            Ydb::ActorTracing::TraceStopResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    ui32 PendingNodes = 0;
    ui32 DoneNodes = 0;
};

class TActorTracingFetchRPC : public TRpcOperationRequestActor<TActorTracingFetchRPC, TEvTraceFetchRequest> {
    using TBase = TRpcOperationRequestActor<TActorTracingFetchRPC, TEvTraceFetchRequest>;
public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleNodes);
            HFunc(NActorTracing::TEvTracing::TEvTraceFetchResult, HandleResult);
        }
    }

    void HandleNodes(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        for (const auto& node : ev->Get()->Nodes) {
            ctx.Send(NActorTracing::MakeActorTracingServiceId(node.NodeId),
                     new NActorTracing::TEvTracing::TEvTraceFetch());
            PendingNodes++;
        }
        if (PendingNodes == 0) {
            Ydb::ActorTracing::TraceFetchResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    void HandleResult(NActorTracing::TEvTracing::TEvTraceFetchResult::TPtr& ev, const TActorContext& ctx) {
        auto& rec = ev->Get()->Record;
        if (rec.GetSuccess() && !rec.GetTraceData().empty()) {
            CollectedTraces.push_back(rec.GetTraceData());
        }
        if (++DoneNodes >= PendingNodes) {
            Ydb::ActorTracing::TraceFetchResult result;
            if (CollectedTraces.size() == 1) {
                result.set_trace_data(CollectedTraces[0]);
            } else if (CollectedTraces.size() > 1) {
                size_t maxIdx = 0;
                for (size_t i = 1; i < CollectedTraces.size(); ++i) {
                    if (CollectedTraces[i].size() > CollectedTraces[maxIdx].size()) {
                        maxIdx = i;
                    }
                }
                result.set_trace_data(CollectedTraces[maxIdx]);
            }
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    }

    ui32 PendingNodes = 0;
    ui32 DoneNodes = 0;
    TVector<TString> CollectedTraces;
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
