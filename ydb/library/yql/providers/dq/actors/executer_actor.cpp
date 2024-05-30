#include "executer_actor.h"
#include "resource_allocator.h"

#include "execution_helpers.h"

#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

#include <ydb/library/yql/providers/dq/planner/execution_planner.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <util/string/split.h>
#include <util/system/env.h>

namespace NYql {
namespace NDq {

using namespace NMonitoring;
using namespace NActors;
using namespace NKikimr::NMiniKQL;
using namespace NYql::NDqProto;
using namespace NYql::NDqs;
using namespace NYql;

class TDqExecuter: public TRichActor<TDqExecuter>, NYql::TCounters {
public:
    static constexpr char ActorName[] = "YQL_DQ_EXECUTER";

    TDqExecuter(
        const NActors::TActorId& gwmActorId,
        const NActors::TActorId& printerId,
        const TString& traceId, const TString& username,
        const TDqConfiguration::TPtr& settings,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        TInstant requestStartTime,
        bool createTaskSuspended,
        ui64 executionTimeout)
        : TRichActor<TDqExecuter>(&TDqExecuter::Handler)
        , GwmActorId(gwmActorId)
        , PrinterId(printerId)
        , Settings(settings)
        , TraceId(traceId)
        , Username(username)
        , Counters(counters) // root, component=dq
        , LongWorkersAllocationCounter(Counters->GetSubgroup("component", "ServiceProxyActor")->GetCounter("LongWorkersAllocation"))
        , ExecutionTimeoutCounter(Counters->GetSubgroup("component", "ServiceProxyActor")->GetCounter("ExecutionTimeout", /*derivative=*/ true))
        , Timeout(TDuration::MilliSeconds(executionTimeout))
        , WorkersAllocationFailTimeout(TDuration::MilliSeconds(Settings->_LongWorkersAllocationFailTimeout.Get().GetOrElse(TDqSettings::TDefault::LongWorkersAllocationFailTimeout)))
        , WorkersAllocationWarnTimeout(TDuration::MilliSeconds(Settings->_LongWorkersAllocationWarnTimeout.Get().GetOrElse(TDqSettings::TDefault::LongWorkersAllocationWarnTimeout)))
        , RequestStartTime(requestStartTime)
        , ExecutionHistogram(Counters->GetSubgroup("component", "ServiceProxyActorHistograms")->GetHistogram("ExecutionTime", ExponentialHistogram(10, 3, 1)))
        , AllocationHistogram(Counters->GetSubgroup("component", "ServiceProxyActorHistograms")->GetHistogram("WorkersAllocationTime", ExponentialHistogram(10, 2, 1)))
        , TasksHistogram(Counters->GetSubgroup("component", "ServiceProxyActorHistograms")->GetHistogram("TasksCount", ExponentialHistogram(10, 2, 1)))
        , CreateTaskSuspended(createTaskSuspended)
    { }

    ~TDqExecuter() {
        MaybeResetAllocationWarnCounter();

        if (ExecutionStart) {
            ExecutionHistogram->Collect((TInstant::Now() - ExecutionStart).Seconds());
        }
    }

private:

    STRICT_STFUNC(Handler, {
        HFunc(TEvGraphRequest, OnGraph);
        HFunc(TEvAllocateWorkersResponse, OnAllocateWorkersResponse);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        hFunc(NActors::TEvents::TEvPoisonTaken, Handle);
        hFunc(TEvDqStats, OnDqStats);
        HFunc(TEvDqFailure, OnFailure);
        HFunc(TEvGraphFinished, OnGraphFinished);
        HFunc(TEvQueryResponse, OnQueryResponse);
        // execution timeout
        cFunc(TEvents::TEvBootstrap::EventType, [this]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << "Execution timeout";
            auto issue = TIssue("Execution timeout");
            issue.SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_ERROR);
            Issues.AddIssues({issue});
            *ExecutionTimeoutCounter += 1;
            Finish(NYql::NDqProto::StatusIds::LIMIT_EXCEEDED);
        })
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup)
    })

    Yql::DqsProto::TWorkerFilter GetPragmaFilter() {
        Yql::DqsProto::TWorkerFilter pragmaFilter;
        if (Settings->WorkerFilter.Get()) {
            try {
                TStringInput inputStream1(Settings->WorkerFilter.Get().GetOrElse(""));
                ParseFromTextFormat(inputStream1, pragmaFilter);
            } catch (...) {
                YQL_CLOG(INFO, ProviderDq) << "Cannot parse filter pragma " << CurrentExceptionMessage();
            }
        }
        return pragmaFilter;
    }

    void MergeFilter(Yql::DqsProto::TWorkerFilter* to, const Yql::DqsProto::TWorkerFilter& from)
    {
#define COPY(name)                                         \
        if (from. Get##name()) {                           \
            *to->Mutable##name() = from.Get##name();       \
        }
#define COPYAR(name)                                          \
        if (from. Get##name().size()>0) {                     \
            *to->Mutable##name() = from.Get##name();          \
        }

        COPY(Revision);
        COPY(ClusterName);
        COPY(ClusterNameHint);
        COPYAR(Address);
        COPYAR(NodeId);
        COPYAR(NodeIdHint);

#undef COPY
#undef COPYARR
    }

    void OnGraph(TEvGraphRequest::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        Y_ABORT_UNLESS(!ControlId);
        Y_ABORT_UNLESS(!ResultId);
        YQL_CLOG(DEBUG, ProviderDq) << "TDqExecuter::OnGraph";
        TFailureInjector::Reach("dq_fail_on_graph", [&] {
            // YQL-15117, it's very likely that the status was INTERNAL_ERROR, originated from worker_actor::OnTaskRunnerCreated (with no issues attached)
            auto ev = MakeHolder<TEvDqFailure>(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR);
            Send(SelfId(), std::move(ev));
        });
        ControlId = NActors::ActorIdFromProto(ev->Get()->Record.GetControlId());
        ResultId = NActors::ActorIdFromProto(ev->Get()->Record.GetResultId());
        if (ev->Get()->Record.GetRequest().GetStatsMode() != NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_UNSPECIFIED) {
            StatsMode = ev->Get()->Record.GetRequest().GetStatsMode();
        }
        // These actors will be killed at exit.
        AddChild(ControlId);
        AddChild(ResultId);

        int workerCount = ev->Get()->Record.GetRequest().GetTask().size();
        const bool enableComputeActor = Settings->EnableComputeActor.Get().GetOrElse(false);
        YQL_CLOG(INFO, ProviderDq) << (TStringBuilder() << "Trying to allocate " << workerCount << " workers [EnableComputeActor=" << enableComputeActor << "]");

        THashMap<TString, Yql::DqsProto::TFile> files;
        TVector<NDqProto::TDqTask> tasks;
        for (auto& task : *ev->Get()->Record.MutableRequest()->MutableTask()) {
            Yql::DqsProto::TTaskMeta taskMeta;
            task.GetMeta().UnpackTo(&taskMeta);

            for (const auto& f : taskMeta.GetFiles()) {
                files.emplace(f.GetObjectId(), f);
            }

            if (ev->Get()->Record.GetRequest().GetSecureParams().size() > 0) {
                *taskMeta.MutableSecureParams() = ev->Get()->Record.GetRequest().GetSecureParams();
            }

            if (ev->Get()->Record.GetRequest().GetCommonTaskParams().size() > 0) {
                taskMeta.MutableTaskParams()->insert(ev->Get()->Record.GetRequest().GetCommonTaskParams().begin(), ev->Get()->Record.GetRequest().GetCommonTaskParams().end());
            }

            Settings->Save(taskMeta);

            task.MutableMeta()->PackFrom(taskMeta);
            task.SetCreateSuspended(CreateTaskSuspended);

            tasks.emplace_back(task);
        }
        TasksHistogram->Collect(tasks.size());

        ExecutionPlanner = THolder<IDqsExecutionPlanner>(new TGraphExecutionPlanner(
            tasks,
            ev->Get()->Record.GetRequest().GetSourceId(),
            ev->Get()->Record.GetRequest().GetResultType(),
            SelfId(), ResultId));


        const TString computeActorType = Settings->ComputeActorType.Get().GetOrElse("sync");

        auto resourceAllocator = RegisterChild(CreateResourceAllocator(
            GwmActorId, SelfId(), ControlId, workerCount,
            TraceId, Settings,
            Counters,
            enableComputeActor ? tasks : TVector<NYql::NDqProto::TDqTask>(),
            computeActorType,
            StatsMode));
        auto allocateRequest = MakeHolder<TEvAllocateWorkersRequest>(workerCount, Username);
        allocateRequest->Record.SetTraceId(TraceId);
        allocateRequest->Record.SetCreateComputeActor(enableComputeActor);
        allocateRequest->Record.SetComputeActorType(computeActorType);
        allocateRequest->Record.SetStatsMode(StatsMode);
        if (enableComputeActor) {
            ActorIdToProto(ControlId, allocateRequest->Record.MutableResultActorId());
        }
        for (const auto& [_, f] : files) {
            *allocateRequest->Record.AddFiles() = f;
        }

        if (Settings->WorkersPerOperation.Get()) {
            allocateRequest->Record.SetWorkersCount(*Settings->WorkersPerOperation.Get());
        }

        Yql::DqsProto::TWorkerFilter pragmaFilter = GetPragmaFilter();

        for (const auto& task : tasks) {
            Yql::DqsProto::TTaskMeta taskMeta;
            task.GetMeta().UnpackTo(&taskMeta);

            Yql::DqsProto::TWorkerFilter* filter = allocateRequest->Record.AddWorkerFilterPerTask();
            *filter->MutableFile() = taskMeta.GetFiles();
            filter->SetClusterNameHint(taskMeta.GetClusterNameHint());

            if (enableComputeActor) {
                *allocateRequest->Record.AddTask() = task;
            }

            MergeFilter(filter, pragmaFilter);
        }

        if (CollectBasic()) {
            StartCounter("AllocateWorkersUs");
        }

        TActivationContext::Send(new IEventHandle(
            GwmActorId,
            resourceAllocator,
            allocateRequest.Release(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));

        YQL_CLOG(DEBUG, ProviderDq) << "Dq timeouts are set to: "
            << ToString(Timeout) << " (global), "
            << ToString(WorkersAllocationFailTimeout) << " (workers allocation fail), "
            << ToString(WorkersAllocationWarnTimeout) << " (workers allocation warn) ";

        if (Timeout) {
            if (StartTime - RequestStartTime > Timeout) {
                Send(SelfId(), new TEvents::TEvBootstrap());
            } else {
                Timeout -= StartTime - RequestStartTime;
            }
        }
    }

    void Finish(NYql::NDqProto::StatusIds::StatusCode statusCode)
    {
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__ << " with status=" << static_cast<int>(statusCode) << " issues=" << Issues.ToString();
        if (Finished) {
            YQL_CLOG(WARN, ProviderDq) << "Re-Finish IGNORED with status=" << static_cast<int>(statusCode);
        } else {
            TQueryResponse result;
            if (CollectBasic()) {
                FlushCounter("ExecutionTimeUs");
                FlushCounters(result);
            }
            IssuesToMessage(Issues, result.MutableIssues());
            result.SetStatusCode(statusCode);
            Send(ControlId, MakeHolder<TEvQueryResponse>(std::move(result)));
            Finished = true;
        }
    }

    void OnFailure(TEvDqFailure::TPtr& ev, const NActors::TActorContext&) {
        if (!Finished) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__
                            << ", status=" << static_cast<int>(ev->Get()->Record.GetStatusCode())
                            << ", issues size=" << ev->Get()->Record.IssuesSize()
                            << ", sender=" << ev->Sender;
            if (CollectBasic()) {
                AddCounters(ev->Get()->Record);
            }
            if (ev->Get()->Record.IssuesSize()) {
                TIssues issues;
                IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
                Issues.AddIssues(issues);
                YQL_CLOG(DEBUG, ProviderDq) << "Issues: " << Issues.ToString();
            }
            Y_ABORT_UNLESS(ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::SUCCESS);
            Finish(ev->Get()->Record.GetStatusCode());
        }
    }

    void OnGraphFinished(TEvGraphFinished::TPtr&, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
        if (!Finished) {
            try {
                TFailureInjector::Reach("dq_fail_on_finish", [] { throw yexception() << "dq_fail_on_finish"; });
                Finish(NYql::NDqProto::StatusIds::SUCCESS);
            } catch (...) {
                YQL_CLOG(ERROR, ProviderDq) << " FailureInjector " << CurrentExceptionMessage();
                Issues.AddIssue(TIssue("FailureInjection"));
                Finish(NYql::NDqProto::StatusIds::UNAVAILABLE);
            }
        }
    }

    // TBD: wait for PoisonTaken from CheckPointCoordinator before send TEvQueryResponse to PrinterId

    void OnQueryResponse(TEvQueryResponse::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__ << " status=" << static_cast<int>(ev->Get()->Record.GetStatusCode()) << " issuses_size=" << ev->Get()->Record.IssuesSize();
        Send(PrinterId, ev->Release().Release());
        PassAway();
    }

    void OnDqStats(TEvDqStats::TPtr& ev) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
        Send(PrinterId, ev->Release().Release());
    }

    void Handle(NActors::TEvents::TEvPoisonTaken::TPtr&) {
        // ignore ack from checkpoint coordinator now
    }

    void OnAllocateWorkersResponse(TEvAllocateWorkersResponse::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << "TDqExecuter::TEvAllocateWorkersResponse";

        if (CollectBasic()) {
            AddCounters(ev->Get()->Record);
            FlushCounter("AllocateWorkersUs");
        }

        auto& response = ev->Get()->Record;
        switch (response.GetTResponseCase()) {
            case TAllocateWorkersResponse::kWorkers:
                break;
            case TAllocateWorkersResponse::kError: {
                YQL_CLOG(ERROR, ProviderDq) << "Error on allocate workers "
                    << ev->Get()->Record.GetError().GetMessage() << ":"
                    << NYql::NDqProto::StatusIds_StatusCode_Name(ev->Get()->Record.GetError().GetStatusCode());
                Issues.AddIssue(TIssue(ev->Get()->Record.GetError().GetMessage()).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_ERROR));
                Finish(ev->Get()->Record.GetError().GetStatusCode());
                return;
            }
            case TAllocateWorkersResponse::kNodes:
            case TAllocateWorkersResponse::TRESPONSE_NOT_SET:
                YQL_ENSURE(false, "Unexpected allocate result");
        }

        auto& workerGroup = response.GetWorkers();
        ResourceId = workerGroup.GetResourceId();
        YQL_CLOG(DEBUG, ProviderDq) << "Allocated resource " << ResourceId;
        TVector<NActors::TActorId> workers;
        for (const auto& actorIdProto : workerGroup.GetWorkerActor()) {
            workers.emplace_back(NActors::ActorIdFromProto(actorIdProto));
        }

        auto tasks = ExecutionPlanner->GetTasks(workers);

        THashMap<TString, Yql::DqsProto::TWorkerInfo> uniqueWorkers;
        if (!workerGroup.GetWorker().empty()) {
            ui32 i = 0;
            for (const auto& workerInfo : workerGroup.GetWorker()) {
                Yql::DqsProto::TTaskMeta taskMeta;
                tasks[i].GetMeta().UnpackTo(&taskMeta);

                WorkerInfo[workerInfo.GetNodeId()] = std::make_tuple(workerInfo, taskMeta.GetStageId());

                YQL_CLOG(DEBUG, ProviderDq) << "WorkerInfo: " << NDqs::NExecutionHelpers::PrettyPrintWorkerInfo(workerInfo, taskMeta.GetStageId());
                YQL_CLOG(DEBUG, ProviderDq) << "TaskInfo: " << i << "/" << tasks[i].GetId();
                for (const auto& file: taskMeta.GetFiles()) {
                    YQL_CLOG(DEBUG, ProviderDq) << " ObjectId: " << file.GetObjectId()  << ", " << file.GetName() << ", " << file.GetLocalPath();
                }
                i++;

                uniqueWorkers.insert(std::make_pair(workerInfo.GetGuid(), workerInfo));
            }
            if (CollectBasic()) {
                AddCounter("UniqueWorkers", uniqueWorkers.size());
            }
        }

        YQL_CLOG(INFO, ProviderDq) << workers.size() << " workers allocated";

        YQL_ENSURE(workers.size() == tasks.size());

        auto res = MakeHolder<TEvReadyState>(ExecutionPlanner->GetSourceID(), ExecutionPlanner->GetResultType(), StatsMode);

        if (Settings->EnableComputeActor.Get().GetOrElse(false) == false) {
            for (size_t i = 0; i < tasks.size(); i++) {
                // { fill debug info
                Yql::DqsProto::TTaskMeta taskMeta;
                tasks[i].GetMeta().UnpackTo(&taskMeta);
                for (const auto& [_, v] : uniqueWorkers) {
                    *taskMeta.AddWorkerInfo() = v;
                }
                tasks[i].MutableMeta()->PackFrom(taskMeta);
                // }
                auto workerEv = MakeHolder<TEvDqTask>(std::move(tasks[i]));
                Send(workers[i], workerEv.Release());
            }
        } else {
            for (size_t i = 0; i < tasks.size(); i++) {
                *res->Record.AddTask() = tasks[i];
                ActorIdToProto(workers[i], res->Record.AddActorId());
            }
        }

        WorkersAllocated = true;

        ExecutionStart = TInstant::Now();
        if (CollectBasic()) {
            StartCounter("ExecutionTimeUs");
        }

        AllocationHistogram->Collect((ExecutionStart-StartTime).Seconds());

        auto readyState1 = res->Record;
        Send(ControlId, res.Release());
        if (ResultId != SelfId() && ResultId != ControlId) {
            Send(ResultId, new TEvReadyState(std::move(readyState1)));
        }

        if (Timeout) {
            ExecutionTimeoutCookieHolder.Reset(ISchedulerCookie::Make2Way());
            Schedule(Timeout, new TEvents::TEvBootstrap, ExecutionTimeoutCookieHolder.Get());
        }
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvWakeup(), 0);
    }

    void OnWakeup() {
        if (WorkersAllocated) {
            MaybeResetAllocationWarnCounter();
        } else {
            MaybeSetAllocationWarnCounter();
            MaybeFailOnWorkersAllocation();
            CheckStateCookieHolder.Reset(ISchedulerCookie::Make2Way());
            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup, CheckStateCookieHolder.Get());
        }
    }

    void MaybeResetAllocationWarnCounter() {
        if (AllocationLongWait) {
            *LongWorkersAllocationCounter -= AllocationLongWait;
            AllocationLongWait = 0;
        }
    }

    void MaybeFailOnWorkersAllocation() {
        if (TInstant::Now() - StartTime > WorkersAllocationFailTimeout)
        {
            YQL_CLOG(ERROR, ProviderDq) << "Timeout on workers allocation has reached";
            Send(SelfId(), new TEvents::TEvBootstrap);
        }
    }

    void MaybeSetAllocationWarnCounter() {
        if (TInstant::Now() - StartTime > WorkersAllocationWarnTimeout)
        {
            if (!AllocationLongWait) {
                AllocationLongWait = 1;
                *LongWorkersAllocationCounter += AllocationLongWait;
            }
        }
    }

    bool CollectBasic() {
        return StatsMode >= NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_BASIC;
    }

    NActors::TActorId GwmActorId;
    NActors::TActorId PrinterId;
    TDqConfiguration::TPtr Settings;

    NActors::TActorId ControlId;
    NActors::TActorId ResultId;
    TExprNode::TPtr ExprRoot;
    THolder<IDqsExecutionPlanner> ExecutionPlanner;
    ui64 ResourceId = 0;
    const TString TraceId;
    const TString Username;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TDynamicCounters::TCounterPtr LongWorkersAllocationCounter;
    TDynamicCounters::TCounterPtr ExecutionTimeoutCounter;

    THashMap<ui32, std::tuple<Yql::DqsProto::TWorkerInfo, ui64>> WorkerInfo; // DEBUG
    TDuration Timeout;
    TDuration WorkersAllocationFailTimeout;
    TDuration WorkersAllocationWarnTimeout;
    TSchedulerCookieHolder ExecutionTimeoutCookieHolder;
    TSchedulerCookieHolder CheckStateCookieHolder;
    bool WorkersAllocated = false;
    const TInstant StartTime = TInstant::Now();
    const TInstant RequestStartTime;
    int AllocationLongWait = 0;
    TInstant ExecutionStart;
    THistogramPtr ExecutionHistogram;
    THistogramPtr AllocationHistogram;
    THistogramPtr TasksHistogram;

    TIssues Issues;
    bool CreateTaskSuspended;
    bool Finished = false;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_FULL;
};

NActors::IActor* MakeDqExecuter(
    const NActors::TActorId& gwmActorId,
    const NActors::TActorId& printerId,
    const TString& traceId, const TString& username,
    const TDqConfiguration::TPtr& settings,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    TInstant requestStartTime,
    bool createTaskSuspended,
    ui64 executionTimeout
) {
    return new TLogWrapReceive(new TDqExecuter(gwmActorId, printerId, traceId, username, settings, counters, requestStartTime, createTaskSuspended, executionTimeout), traceId);
}

} // namespace NDq
} // namespace NYql
