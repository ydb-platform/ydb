#include "local_worker_manager.h"
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/compute_actor.h>
#include <ydb/library/yql/providers/dq/actors/worker_actor.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/dq/common/dq_resource_quoter.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include "worker_manager_common.h"

#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/random/random.h>
#include <util/system/rusage.h>

using namespace NActors;

namespace NYql::NDqs {

union TDqLocalResourceId {
    struct {
        ui32 Counter;
        ui16 Seed;
        ui16 NodeId;
    };
    ui64 Data;
};

static_assert(sizeof(TDqLocalResourceId) == 8);

struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NDq::TResourceQuoter> nodeQuoter, const NDq::TTxId& txId, ui64 limit)
        : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
        , NodeQuoter(nodeQuoter)
        , TxId(txId) {
    }

    ~TMemoryQuotaManager() override {
        if (Limit) {
            NodeQuoter->Free(TxId, 0, Limit);
        }
    }

    bool AllocateExtraQuota(ui64 extraSize) override {
        return NodeQuoter->Allocate(TxId, 0, extraSize);
    }

    void FreeExtraQuota(ui64 extraSize) override {
        NodeQuoter->Free(TxId, 0, extraSize);
    }

    std::shared_ptr<NDq::TResourceQuoter> NodeQuoter;
    NDq::TTxId TxId;
};

class TLocalWorkerManager: public TWorkerManagerCommon<TLocalWorkerManager> {

public:
    static constexpr char ActorName[] = "YQL_DQ_LWM";

    TLocalWorkerManager(const TLocalWorkerManagerOptions& options)
        : TWorkerManagerCommon<TLocalWorkerManager>(&TLocalWorkerManager::Handler)
        , Options(options)
        , TaskCounters(Options.Counters.TaskCounters)
        , MemoryQuoter(std::make_shared<NDq::TResourceQuoter>(Options.MkqlTotalMemoryLimit))
    {
        Options.Counters.MkqlMemoryLimit->Set(Options.MkqlTotalMemoryLimit);
        Options.Counters.MkqlMemoryAllocated->Set(0);

        MemoryQuoter->SetNotifier([limitCounter = Options.Counters.MkqlMemoryLimit, allocatedCounter = Options.Counters.MkqlMemoryAllocated](const ui64 limit, ui64 allocated) {
            limitCounter->Set(limit);
            allocatedCounter->Set(allocated);
        });
    }

private:
    STRICT_STFUNC(Handler, {
        hFunc(TEvAllocateWorkersRequest, OnAllocateWorkersRequest)
        hFunc(TEvFreeWorkersNotify, OnFreeWorkers)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        cFunc(TEvents::TEvBootstrap::EventType, Bootstrap)
        cFunc(TEvents::TEvWakeup::EventType, WakeUp)
        IgnoreFunc(TEvInterconnect::TEvNodeConnected)
        hFunc(TEvInterconnect::TEvNodeDisconnected, OnDisconnected)
        hFunc(TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvConfigureFailureInjectorRequest, OnConfigureFailureInjector)
        HFunc(TEvRoutesRequest, OnRoutesRequest)
        hFunc(TEvQueryStatus, OnQueryStatus)
        hFunc(NActors::NMon::TEvHttpInfo, OnMonitoringPage)
    })

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap(), 0);
    }

    void Bootstrap() {
        ResourceId.Seed = static_cast<ui16>(RandomNumber<ui64>());
        ResourceId.Counter = 0;

        Send(SelfId(), new TEvents::TEvWakeup());
    }

    void OnMonitoringPage(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream html;
        const auto& params = ev->Get()->Request.GetParams();
        if (params.Has("get")) {
            TString txId = params.Get("tx_id");
            ui64 taskId = 0;
            try {
                taskId = std::stoull(params.Get("task_id"));
            } catch (...) {
                // ¯\_(ツ)_/¯
            }

            for (const auto& [_, workersInfo]: AllocatedWorkers) {
                auto* traceId = std::get_if<TString>(&workersInfo.TxId);
                if (traceId && *traceId != txId) {
                    continue;
                }

                for (size_t i = 0; i < workersInfo.WorkerActors.ActorIds.size(); i++) {
                    auto workerTaskId = workersInfo.WorkerActors.TaskIds[i];
                    auto actorId = workersInfo.WorkerActors.ActorIds[i];
                    if (workerTaskId == taskId) {
                        Send(ev->Forward(actorId));
                        return;
                    }
                }
            }

            html << "<div style='border:1px dotted red;padding:2%;'>Couldn't find the worker with parameters: TxId = " <<  txId << ", TaskId: " << taskId << "</div>";
        }

        html << "<form method='get'>";
        html << "<p>TxId:<input name='tx_id' type='text'/></p>";
        html << "<p>TaskId:<input name='task_id' type='text'/></p>";
        html << "<button name='get' type='submit'><b>Get</b></button>";
        html << "</form>";

        // Table with known workers info
        html << "<br>";
        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>ResourceId</th>";
        html << "<th>Sender</th>";
        html << "<th>Deadline</th>";
        html << "<th>TxId</th>";
        html << "<th>WorkerActor</th>";
        html << "<th>TaskId</th>";
        html << "<th>Link</th>";
        html << "</tr></thead><tbody>";

        for (const auto& [ResourceId, workersInfo]: AllocatedWorkers) {
            auto* traceId = std::get_if<TString>(&workersInfo.TxId);
            html << "<tr>";
            html << "<td>" << ResourceId << "</td>";
            html << "<td>" << workersInfo.Sender.ToString() << "</td>";
            html << "<td>" << workersInfo.Deadline.ToString() << "</td>";
            html << "<td>" << (traceId ? *traceId : "<unknown>") << "</td>";
            html << "<td></td><td></td><td></td>";
            html << "</tr>\n";

            for (size_t i = 0; i < workersInfo.WorkerActors.ActorIds.size(); i++) {
                auto workerTaskId = workersInfo.WorkerActors.TaskIds[i];
                auto actorId = workersInfo.WorkerActors.ActorIds[i];
                html << "<tr>";
                html << "<td>" << ResourceId << "</td>";
                html << "<td>" << workersInfo.Sender.ToString() << "</td>";
                html << "<td>" << workersInfo.Deadline.ToString() << "</td>";
                html << "<td>" << (traceId ? *traceId : "<unknown>") << "</td>";
                html << "<td>" << actorId.ToString() << "</td>";
                html << "<td>" << workerTaskId <<"</td>";
                html << "<td>";
                html << "<form method='get'>";
                html << "<input name='tx_id' type='hidden' value='" << (traceId ? *traceId : "<unknown>") << "'/>";
                html << "<input name='task_id' type='hidden' value='" << workerTaskId << "'/>";
                html << "<button name='get' type='submit'><b>Detailed</b></button>";
                html << "</form>";
                html <<"</td>";
                html << "</tr>\n";
            }
        }

        html << "</tbody></table>";

        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(html.Str()));
    }

    void WakeUp() {
        auto currentRusage = TRusage::Get();
        TRusage delta;
        delta.Utime = currentRusage.Utime - Rusage.Utime;
        delta.Stime = currentRusage.Stime - Rusage.Stime;
        delta.MajorPageFaults = currentRusage.MajorPageFaults - Rusage.MajorPageFaults;
        if (Options.RuntimeData) {
            Options.RuntimeData->AddRusageDelta(delta);
        }
        Rusage = currentRusage;

        FreeOnDeadline();

        TActivationContext::Schedule(TDuration::MilliSeconds(800), new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup(), 0));
    }

    void DoPassAway() override {
        for (const auto& [resourceId, _] : AllocatedWorkers) {
            FreeGroup(resourceId);
        }

        AllocatedWorkers.clear();
        _exit(0);
    }

    void Deallocate(ui32 nodeId) {
        TVector<ui64> toDeallocate;

        YQL_CLOG(DEBUG, ProviderDq) << "Deallocate " << nodeId;
        for (const auto& [k, v] : AllocatedWorkers) {
            if (v.Sender.NodeId() == nodeId) {
                toDeallocate.push_back(k);
            }
        }

        for (const auto& k : toDeallocate) {
            FreeGroup(k);
        }
    }

    void Deallocate(const NActors::TActorId& senderId) {
        TVector<ui64> toDeallocate;

        YQL_CLOG(DEBUG, ProviderDq) << "Deallocate " << senderId;
        for (const auto& [k, v] : AllocatedWorkers) {
            if (v.Sender == senderId) {
                toDeallocate.push_back(k);
            }
        }

        for (const auto& k : toDeallocate) {
            FreeGroup(k);
        }
    }

    void OnDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev)
    {
        YQL_CLOG(DEBUG, ProviderDq) << "Disconnected " << ev->Get()->NodeId;
        Unsubscribe(ev->Get()->NodeId);
        Deallocate(ev->Get()->NodeId);
    }

    void OnUndelivered(TEvents::TEvUndelivered::TPtr& ev)
    {
        Y_ABORT_UNLESS(ev->Get()->Reason == TEvents::TEvUndelivered::Disconnected
            || ev->Get()->Reason == TEvents::TEvUndelivered::ReasonActorUnknown);

        YQL_CLOG(DEBUG, ProviderDq) << "Undelivered " << ev->Sender;

        switch (ev->Get()->Reason) {
        case TEvents::TEvUndelivered::Disconnected:
            Deallocate(ev->Sender.NodeId());
            break;
        case TEvents::TEvUndelivered::ReasonActorUnknown:
            Deallocate(ev->Sender);
            break;
        default:
            break;
        }
    }

    void OnConfigureFailureInjector(TEvConfigureFailureInjectorRequest::TPtr& ev) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvConfigureFailureInjectorRequest ";

        auto& request = ev->Get()->Record.GetRequest();
        YQL_ENSURE(request.GetNodeId() == SelfId().NodeId(), "Wrong node id!");

        TFailureInjector::Set(request.GetName(), request.GetSkip(), request.GetCountOfFails());
        YQL_CLOG(DEBUG, ProviderDq) << "Failure injector is configured " << request.GetName();

        auto response = MakeHolder<TEvConfigureFailureInjectorResponse>();
        auto* r = response->Record.MutableResponse();
        r->Setsuccess(true);

        Send(ev->Sender, response.Release());
    }

    void OnAllocateWorkersRequest(TEvAllocateWorkersRequest::TPtr& ev) {
        ui64 resourceId;
        if (ev->Get()->Record.GetResourceId()) {
            resourceId = ev->Get()->Record.GetResourceId();
        } else {
            auto resource = ResourceId;
            resource.NodeId = ev->Sender.NodeId();
            resourceId = resource.Data;
            ResourceId.Counter ++;
        }
        bool createComputeActor = ev->Get()->Record.GetCreateComputeActor();
        TString computeActorType = ev->Get()->Record.GetComputeActorType();

        if (createComputeActor && !Options.CanUseComputeActor) {
            Send(ev->Sender, MakeHolder<TEvAllocateWorkersResponse>("Compute Actor Disabled", NYql::NDqProto::StatusIds::BAD_REQUEST), 0, ev->Cookie);
            return;
        }

        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TLocalWorkerManager::TEvAllocateWorkersRequest " << resourceId;
        TFailureInjector::Reach("allocate_workers_failure", [] { ::_exit(1); });

        auto traceId = ev->Get()->Record.GetTraceId();
        auto count = ev->Get()->Record.GetCount();
        Y_ABORT_UNLESS(count > 0);

        auto& tasks = *ev->Get()->Record.MutableTask();

        ui64 totalInitialTaskMemoryLimit = 0;
        std::vector<ui64> quotas;
        if (createComputeActor) {
            Y_ABORT_UNLESS(static_cast<int>(tasks.size()) == static_cast<int>(count));
            quotas.reserve(count);
            for (auto& task : tasks) {
                auto taskLimit = task.GetInitialTaskMemoryLimit();
                if (taskLimit == 0) {
                    taskLimit = Options.MkqlInitialMemoryLimit;
                }
                quotas.push_back(taskLimit);
                totalInitialTaskMemoryLimit += taskLimit;
            }
        } else {
            totalInitialTaskMemoryLimit = count * Options.MkqlInitialMemoryLimit;
        }

        bool canAllocate = MemoryQuoter->Allocate(traceId, 0, totalInitialTaskMemoryLimit);
        if (!canAllocate) {
            Send(ev->Sender, MakeHolder<TEvAllocateWorkersResponse>("Not enough memory to allocate tasks", NYql::NDqProto::StatusIds::OVERLOADED), 0, ev->Cookie);
            return;
        }

        auto& allocationInfo = AllocatedWorkers[resourceId];
        allocationInfo.TxId = traceId;

        if (allocationInfo.WorkerActors.ActorIds.empty()) {
            allocationInfo.WorkerActors.ActorIds.reserve(count);
            allocationInfo.WorkerActors.TaskIds.reserve(count);
            allocationInfo.Sender = ev->Sender;
            if (ev->Get()->Record.GetFreeWorkerAfterMs()) {
                allocationInfo.Deadline =
                    TInstant::Now() + TDuration::MilliSeconds(ev->Get()->Record.GetFreeWorkerAfterMs());
            }

            auto resultId = ActorIdFromProto(ev->Get()->Record.GetResultActorId());
            ::NMonitoring::TDynamicCounterPtr taskCounters;

            if (createComputeActor && TaskCounters) {
                auto& info = TaskCountersMap[traceId];
                if (!info.TaskCounters) {
                    info.TaskCounters = TaskCounters->GetSubgroup("operation", traceId);
                }
                info.ReferenceCount += count;
                taskCounters = info.TaskCounters;
            }

            for (ui32 i = 0; i < count; i++) {
                THolder<NActors::IActor> actor;
                ui64 taskId = 0;
                if (createComputeActor) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Create compute actor: " << computeActorType;
                    
                    NYql::NDqProto::TDqTask* taskPtr = &(tasks[i]);
                    taskId = taskPtr->id();
                    actor.Reset(NYql::CreateComputeActor(
                        Options,
                        std::make_shared<TMemoryQuotaManager>(MemoryQuoter, allocationInfo.TxId, quotas[i]),
                        resultId,
                        traceId,
                        taskPtr,
                        computeActorType,
                        Options.TaskRunnerActorFactory,
                        taskCounters,
                        ev->Get()->Record.GetStatsMode()));
                } else {
                    actor.Reset(CreateWorkerActor(
                        Options.RuntimeData,
                        traceId,
                        Options.TaskRunnerActorFactory,
                        Options.AsyncIoFactory,
                        Options.FunctionRegistry));
                }
                allocationInfo.WorkerActors.ActorIds.emplace_back(RegisterChild(
                    actor.Release(), createComputeActor ? NYql::NDq::TEvDq::TEvAbortExecution::Unavailable("Aborted by LWM").Release() : nullptr
                ));
                allocationInfo.WorkerActors.TaskIds.emplace_back(taskId);
            }

            Options.Counters.ActiveWorkers->Add(count);
        }

        Send(ev->Sender,
            MakeHolder<TEvAllocateWorkersResponse>(resourceId, allocationInfo.WorkerActors.ActorIds),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            ev->Cookie);
        Subscribe(ev->Sender.NodeId());
    }

    void OnFreeWorkers(TEvFreeWorkersNotify::TPtr& ev) {
        ui64 resourceId = ev->Get()->Record.GetResourceId();
        YQL_CLOG(DEBUG, ProviderDq) << "TEvFreeWorkersNotify " << resourceId;
        FreeGroup(resourceId, ev->Sender);
    }

    void OnQueryStatus(TEvQueryStatus::TPtr& ev) {
        auto response = MakeHolder<TEvQueryStatusResponse>();
        Send(ev->Sender, response.Release());
    }

    void DropTaskCounters(const auto& info) {
        auto traceId = std::get<TString>(info.TxId);
        if (auto it = TaskCountersMap.find(traceId); it != TaskCountersMap.end()) {
            if (it->second.ReferenceCount <= info.WorkerActors.ActorIds.size()) {
                if (TaskCounters) {
                    TaskCounters->RemoveSubgroup("operation", traceId);
                }
                TaskCountersMap.erase(it);
            } else {
                it->second.ReferenceCount -= info.WorkerActors.ActorIds.size();
            }
        }
    }

    void FreeGroup(ui64 id, NActors::TActorId sender = NActors::TActorId()) {
        YQL_CLOG(DEBUG, ProviderDq) << "Free Group " << id;
        auto it = AllocatedWorkers.find(id);
        if (it != AllocatedWorkers.end()) {
            for (const auto& actorId : it->second.WorkerActors.ActorIds) {
                UnregisterChild(actorId);
            }

            if (sender && it->second.Sender != sender) {
                Options.Counters.FreeGroupError->Inc();
                YQL_CLOG(ERROR, ProviderDq) << "Free Group " << id << " mismatched alloc-free senders: " << it->second.Sender << " and " << sender << " TxId: " << it->second.TxId;
            }

            if (Options.DropTaskCountersOnFinish) {
                DropTaskCounters(it->second);
            }

            Options.Counters.ActiveWorkers->Sub(it->second.WorkerActors.ActorIds.size());
            AllocatedWorkers.erase(it);
        }
    }

    void FreeOnDeadline() {
        auto now = TInstant::Now();
        THashSet<ui32> todelete;
        for (const auto& [id, info] : AllocatedWorkers) {
            if (info.Deadline && info.Deadline < now) {
                todelete.insert(id);
            }
        }
        for (const auto& id : todelete) {
            YQL_CLOG(DEBUG, ProviderDq) << "Free on deadline: " << id;
            FreeGroup(id);
        }
    }

    TLocalWorkerManagerOptions Options;
    NMonitoring::TDynamicCounterPtr TaskCounters;

    struct TAllocationInfo {
        struct TWorkerInfo {
            TVector<NActors::TActorId> ActorIds;
            TVector<ui64> TaskIds;
        };

        TWorkerInfo WorkerActors;
        NActors::TActorId Sender;
        TInstant Deadline;
        NDq::TTxId TxId;
    };
    THashMap<ui64, TAllocationInfo> AllocatedWorkers;
    TDqLocalResourceId ResourceId;

    TRusage Rusage;

    std::shared_ptr<NDq::TResourceQuoter> MemoryQuoter;

    struct TCountersInfo {
        ::NMonitoring::TDynamicCounterPtr TaskCounters;
        ui64 ReferenceCount;
    };

    TMap<TString, TCountersInfo> TaskCountersMap;
};


NActors::IActor* CreateLocalWorkerManager(const TLocalWorkerManagerOptions& options)
{
    return new TLocalWorkerManager(options);
}

} // namespace NYql::NDqs
