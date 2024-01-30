#include "resource_allocator.h"
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include "execution_helpers.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

namespace NYql {

using namespace NActors;
using namespace NDqs;
using namespace NMonitoring;

union TDqResourceId {
    struct {
        ui32 u1;
        ui16 u2;
        ui16 u3;
    };
    ui64 Data;
};

class TResourceAllocator: public TRichActor<TResourceAllocator>
{
    struct TRequestInfo {
        NYql::NDqProto::TDqTask Task; // for compute actor
        Yql::DqsProto::TWorkerInfo WorkerInfo;
        bool RequestedFlag;
        TString ClusterName;
        TInstant StartTime;
        ui64 ResourceId; // for retries
        int Retry = 0;
    };


public:
    static constexpr char ActorName[] = "YQL_DQ_RESOURCE_ALLOCATOR";

    TResourceAllocator(
        TActorId gwmActor,
        TActorId senderId,
        TActorId controlId,
        ui32 workerCount,
        const TString& traceId,
        const TDqConfiguration::TPtr settings,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TVector<NYql::NDqProto::TDqTask>& tasks,
        const TString& computeActorType,
        NDqProto::EDqStatsMode statsMode)
        : TRichActor<TResourceAllocator>(&TResourceAllocator::Handle)
        , GwmActor(gwmActor)
        , SenderId(senderId)
        , ControlId(controlId)
        , RequestedCount(workerCount)
        , TraceId(traceId)
        , Settings(settings)
        , NetworkRetries(Settings->MaxNetworkRetries.Get().GetOrElse(TDqSettings::TDefault::MaxNetworkRetries))
        , Timeout(GetAllocationTimeout(workerCount, Settings))
        , Counters(counters->GetSubgroup("counters", "Actor")) // root, component=dq, component=Actor
        , RetryCounter(counters->GetSubgroup("component", "ServiceProxyActor")->GetCounter("RetryCreateActor", /*derivative=*/ true))
        , Tasks(tasks)
        , ComputeActorType(computeActorType)
        , StatsMode(statsMode)
    {
        AllocatedWorkers.resize(workerCount);
        if (!Tasks.empty()) {
            Y_ABORT_UNLESS(workerCount == Tasks.size());
        }
    }

private:
    STRICT_STFUNC(Handle, {
        HFunc(TEvAllocateWorkersResponse, OnAllocateWorkersResponse)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        hFunc(TEvInterconnect::TEvNodeConnected, [this](TEvInterconnect::TEvNodeConnected::TPtr& ev) {
            // Store GWM NodeId. Auto-unsubscribe on actor-death
            Subscribe(ev->Get()->NodeId);
        })
        cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, [this]() {
            Fail((ui64)-1, "GWM Disconnected");
        })
        hFunc(TEvents::TEvUndelivered, [this](TEvents::TEvUndelivered::TPtr& ev) {
            Fail(ev->Cookie, "Undelivered");
        })
    })

    static TDuration GetAllocationTimeout(int workerCount, const TDqConfiguration::TPtr settings) {
        ui64 timeout = workerCount == 1
            ? settings->_LiteralTimeout.Get().GetOrElse(TDqSettings::TDefault::LiteralTimeout)
            : settings->_TableTimeout.Get().GetOrElse(TDqSettings::TDefault::TableTimeout);
        if (timeout & (1ULL<<63)) {
            return TDuration(); // no timeout
        }
        return TDuration::MilliSeconds(timeout << 1);
    }

    void RequestActorIdsFromNodes(TEvAllocateWorkersResponse::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        YQL_CLOG(DEBUG, ProviderDq) << "RequestActorIdsFromNodes " << ev->Sender.NodeId();

        auto& response = ev->Get()->Record;
        auto& nodes = response.GetNodes().GetWorker();
        ResourceId = response.GetNodes().GetResourceId();
        Y_ABORT_UNLESS(nodes.size() > 0);
        Y_ABORT_UNLESS(static_cast<ui32>(nodes.size()) == RequestedCount);
        RequestedNodes.reserve(RequestedCount);

        YQL_CLOG(DEBUG, ProviderDq) << "RequestActorIdsFromNodes " << ev->Sender.NodeId() << " " << ResourceId;
        auto now = TInstant::Now();
        ui16 i = 1;
        for (const auto& node : nodes) {
            YQL_CLOG(DEBUG, ProviderDq) << "RequestedNode: " << node.GetNodeId();
            TString clusterName = node.GetClusterName();
            NYql::NDqProto::TDqTask task;
            if (!Tasks.empty()) {
                task = Tasks[i-1];
            }
            TDqResourceId resourceId;
            resourceId.Data = ResourceId;
            resourceId.u3 = i++;
            RequestedNodes[resourceId.Data] = { task, node, false, clusterName, now, resourceId.Data };
        }

        for (const auto& [_, requestInfo] : RequestedNodes) {
            SendToWorker(requestInfo);
        }
    }

    void OnAllocateWorkersResponse(TEvAllocateWorkersResponse::TPtr& ev, const TActorContext& ctx)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId, SelfId().ToString());
        Y_UNUSED(ctx);

        YQL_CLOG(DEBUG, ProviderDq) << "TEvAllocateWorkersResponse " << ev->Sender.NodeId();

        QueryStat.AddCounters(ev->Get()->Record);

        if (FailState) {
            return;
        }

        if (ev->Get()->Record.GetTResponseCase() == NDqProto::TAllocateWorkersResponse::kError) {
            YQL_CLOG(DEBUG, ProviderDq) << "Forwarding bad state";
            ctx.Send(ev->Forward(SenderId));
            FailState = true;
            return;
        }

        if (ev->Get()->Record.GetTResponseCase() == NDqProto::TAllocateWorkersResponse::kNodes) {
            Unsubscribe(ev->Sender.NodeId()); // don't need subscription anymore
            return RequestActorIdsFromNodes(ev, ctx);
        }

        if (RequestedNodes.empty()) {
            /*work w/o gwm*/
            LocalMode = true;
            AllocatedWorkers.emplace_back(ev->Get()->Record.GetWorkers());
            ctx.Send(ev->Forward(SenderId));
            return;
        }

        auto cookie = ev->Cookie;
        if (cookie == 0) {
            // COMPAT
            for (const auto& [_, v] : RequestedNodes) {
                if (v.WorkerInfo.GetNodeId() == ev->Sender.NodeId()) {
                    cookie = v.ResourceId;
                    break;
                }
            }
        }
        YQL_ENSURE(RequestedNodes.contains(cookie));
        auto& requestedNode = RequestedNodes[cookie];
        if (!requestedNode.RequestedFlag) {
            TDqResourceId resourceId{};
            resourceId.Data = cookie;
            AllocatedWorkers[resourceId.u3 - 1] = ev->Get()->Record.GetWorkers();
            AllocatedCount++;
            requestedNode.RequestedFlag = true;
            auto delta = TInstant::Now() - requestedNode.StartTime;
            // catched and grpc_service
            std::map<TString, TString> labels;
            if (requestedNode.ClusterName) {
                labels.emplace("ClusterName", requestedNode.ClusterName);
            }
            QueryStat.AddCounter(QueryStat.GetCounterName("Actor", labels, "ActorCreateTimeUs"), delta);
        }

        if (AllocatedCount == RequestedCount) {
            TVector<NActors::TActorId> workerIds;
            for (auto& group : AllocatedWorkers) {
                for (const auto& actorIdProto : group.GetWorkerActor()) {
                    workerIds.emplace_back(NActors::ActorIdFromProto(actorIdProto));
                }
            }

            auto response = MakeHolder<TEvAllocateWorkersResponse>(ResourceId, workerIds);
            QueryStat.FlushCounters(response->Record);
            auto* workerGroup = response->Record.MutableWorkers();
            TVector<Yql::DqsProto::TWorkerInfo> workers;
            workers.resize(AllocatedCount);
            for (const auto& [resourceId, requestInfo] : RequestedNodes) {
                TDqResourceId dqResourceId{};
                dqResourceId.Data = resourceId;
                workers[dqResourceId.u3 - 1] = requestInfo.WorkerInfo;
            }
            for (const auto& worker : workers) {
                *workerGroup->AddWorker() = worker;
            }
            Send(SenderId, response.Release());
            Answered = true;
        }
    }

    void DoPassAway() override
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId, SelfId().ToString());
        for (const auto& group : AllocatedWorkers) {
            for (const auto& actorIdProto : group.GetWorkerActor()) {
                auto actorNode = NActors::ActorIdFromProto(actorIdProto).NodeId();
                YQL_CLOG(DEBUG, ProviderDq) << "TEvFreeWorkersNotify " << group.GetResourceId();
                auto request = MakeHolder<TEvFreeWorkersNotify>(group.GetResourceId());
                request->Record.SetTraceId(TraceId);
                Send(MakeWorkerManagerActorID(actorNode), request.Release());
            }
        }
        if (!LocalMode) {
            YQL_CLOG(DEBUG, ProviderDq) << "TEvFreeWorkersNotify " << ResourceId << " (failed)";
            auto request = MakeHolder<TEvFreeWorkersNotify>(ResourceId);
            request->Record.SetTraceId(TraceId);
            for (const auto& failedWorker : FailedWorkers) {
                *request->Record.AddFailedWorkerGuid() = failedWorker.GetGuid();
            }
            Send(GwmActor, request.Release());
        }
    }

    void SendToWorker(const TRequestInfo& node, TDuration backoff = TDuration()) {
        auto nodeId = node.WorkerInfo.GetNodeId();
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1, "", TMaybe<ui64>(node.ResourceId));
        if (Timeout) {
            request->Record.SetFreeWorkerAfterMs(Timeout.MilliSeconds());
        }
        request->Record.SetTraceId(TraceId);
        if (!Tasks.empty()) {
            request->Record.SetCreateComputeActor(true);
            request->Record.SetComputeActorType(ComputeActorType);
            ActorIdToProto(ControlId, request->Record.MutableResultActorId());
            *request->Record.AddTask() = node.Task;
        }
        request->Record.SetStatsMode(StatsMode);
        YQL_CLOG(WARN, ProviderDq) << "Send TEvAllocateWorkersRequest to " << NDqs::NExecutionHelpers::PrettyPrintWorkerInfo(node.WorkerInfo, 0);
        if (backoff) {
            TActivationContext::Schedule(backoff, new IEventHandle(
                MakeWorkerManagerActorID(nodeId), SelfId(), request.Release(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered,
                node.ResourceId));
        } else {
            Send(
                MakeWorkerManagerActorID(nodeId),
                request.Release(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered,
                node.ResourceId);
        }
    }

    void Fail(const ui64 cookie, const TString& reason) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId, SelfId().ToString());
        if (FailState) {
            return;
        }
        auto maybeRequestInfo = RequestedNodes.find(cookie);
        if (!Answered && maybeRequestInfo != RequestedNodes.end() && maybeRequestInfo->second.Retry < NetworkRetries) {
            YQL_CLOG(WARN, ProviderDq) << "Retry Allocate Request";
            auto& requestInfo = RequestedNodes[cookie];
            requestInfo.Retry ++;
            *RetryCounter += 1;
            QueryStat.AddCounter("Retry", 1);
            SendToWorker(requestInfo, TDuration::MilliSeconds(100));
            return;
        }
        FailState = true;
        TString workerInfo;

        if (maybeRequestInfo != RequestedNodes.end()) {
            workerInfo = NDqs::NExecutionHelpers::PrettyPrintWorkerInfo(maybeRequestInfo->second.WorkerInfo, 0);
            FailedWorkers.push_back(maybeRequestInfo->second.WorkerInfo);

            auto delta = TInstant::Now() - maybeRequestInfo->second.StartTime;
            // catched at grpc_service
            QueryStat.AddCounter(
                QueryStat.GetCounterName("Actor", {{"ClusterName", maybeRequestInfo->second.ClusterName}}, "CreateFailTimeUs"),
                delta);
        }
        TString message = "Disconnected from worker: `" + workerInfo + "', reason: " + reason;
        YQL_CLOG(ERROR, ProviderDq) << message;
        auto response = MakeHolder<TEvAllocateWorkersResponse>(message, NYql::NDqProto::StatusIds::UNAVAILABLE);
        QueryStat.FlushCounters(response->Record);
        Send(SenderId, response.Release());
    }

    const TActorId GwmActor;
    const TActorId SenderId;
    const TActorId ControlId;

    THashMap<ui64, TRequestInfo> RequestedNodes;
    ui32 RequestedCount;
    ui64 ResourceId = 0;
    bool LocalMode = false;

    TVector<NDqProto::TWorkerGroup> AllocatedWorkers;
    ui32 AllocatedCount = 0;

    bool FailState = false;
    bool Answered = false;

    TVector<Yql::DqsProto::TWorkerInfo> FailedWorkers;

    const TString TraceId;
    const TDqConfiguration::TPtr Settings;
    int NetworkRetries;
    TDuration Timeout;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TDynamicCounters::TCounterPtr RetryCounter;

    TCounters QueryStat;

    TVector<NYql::NDqProto::TDqTask> Tasks; // for compute actor
    const TString ComputeActorType;
    NDqProto::EDqStatsMode StatsMode;
};

NActors::IActor* CreateResourceAllocator(
    TActorId gwmActor,
    TActorId senderId,
    TActorId controlId,
    ui32 size,
    const TString& traceId,
    const TDqConfiguration::TPtr& settings,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    const TVector<NYql::NDqProto::TDqTask>& tasks,
    const TString& computeActorType,
    NDqProto::EDqStatsMode statsMode)
{
    return new TResourceAllocator(gwmActor, senderId, controlId, size, traceId, settings, counters, tasks, computeActorType, statsMode);
}

} // namespace NYql
