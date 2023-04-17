#include "task_controller.h"
#include "execution_helpers.h"
#include "events.h"
#include "proto_builder.h"
#include "actor_helpers.h"
#include "executer_actor.h"
#include "grouped_issues.h"

#include <ydb/library/yql/providers/dq/counters/counters.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/size_literals.h>
#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/system/types.h>

namespace NYql {

using namespace NActors;
using namespace NDqs;

template<typename TDerived>
class TTaskControllerImpl: public NActors::TActor<TDerived> {
public:
    using NActors::TActor<TDerived>::PassAway;
    using NActors::TActor<TDerived>::Schedule;
    using NActors::TActor<TDerived>::SelfId;
    using NActors::TActor<TDerived>::Send;

    static constexpr ui64 PING_TIMER_TAG = 1;
    static constexpr ui64 AGGR_TIMER_TAG = 2;

    static constexpr char ActorName[] = "YQL_DQ_TASK_CONTROLLER";

    explicit TTaskControllerImpl(
        const TString& traceId,
        const NActors::TActorId& executerId,
        const NActors::TActorId& resultId,
        const TDqConfiguration::TPtr& settings,
        const NYql::NCommon::TServiceCounters& serviceCounters,
        const TDuration& pingPeriod,
        const TDuration& aggrPeriod,
        void (TDerived::*func)(TAutoPtr<NActors::IEventHandle>&)
    )
        : NActors::TActor<TDerived>(func)
        , ExecuterId(executerId)
        , ResultId(resultId)
        , TraceId(traceId)
        , Settings(settings)
        , ServiceCounters(serviceCounters, "task_controller")
        , PingPeriod(pingPeriod)
        , AggrPeriod(aggrPeriod)
        , Issues(CreateDefaultTimeProvider())
    {
        if (Settings) {
            if (Settings->_AllResultsBytesLimit.Get()) {
                YQL_CLOG(DEBUG, ProviderDq) << "_AllResultsBytesLimit = " << *Settings->_AllResultsBytesLimit.Get();
            }
            if (Settings->_RowsLimitPerWrite.Get()) {
                YQL_CLOG(DEBUG, ProviderDq) << "_RowsLimitPerWrite = " << *Settings->_RowsLimitPerWrite.Get();
            }
        }
    }

    ~TTaskControllerImpl() override {
        SetTaskCountMetric(0);
    }

public:
    STRICT_STFUNC(Handler, {
        hFunc(TEvReadyState, OnReadyState);
        hFunc(TEvQueryResponse, OnQueryResult);
        hFunc(TEvDqFailure, OnResultFailure);
        hFunc(NDq::TEvDqCompute::TEvState, OnComputeActorState);
        hFunc(NDq::TEvDq::TEvAbortExecution, OnAbortExecution);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        hFunc(TEvents::TEvUndelivered, OnUndelivered);
        hFunc(TEvents::TEvWakeup, OnWakeup);
    })

    void OnUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        auto it = TaskIds.find(ev->Sender);
        if (it != TaskIds.end() && FinishedTasks.contains(it->second)) {
            // ignore undelivered from finished CAs
            return;
        }

        TStringBuilder message;
        message << "Undelivered Event " << ev->Get()->SourceType
            << " from " << SelfId() << " (Self) to " << ev->Sender
            << " Reason: " << ev->Get()->Reason << " Cookie: " << ev->Cookie;
        OnError(NYql::NDqProto::StatusIds::UNAVAILABLE, message);
    }

    void OnAbortExecution(NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        auto statusCode = ev->Get()->Record.GetStatusCode();
        TIssues issues = ev->Get()->GetIssues();
        YQL_CLOG(DEBUG, ProviderDq) << "AbortExecution from " << ev->Sender << ":" << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode) << " " << issues.ToOneLineString();
        OnError(statusCode, issues);
    }

    void OnInternalError(const TString& message, const TIssues& subIssues = {}) {
        OnError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, message, subIssues);
    }

private:
    void SendNonFatalIssues() {
        auto req = MakeHolder<TEvDqStats>(Issues.ToIssues());
        Send(ExecuterId, req.Release());
    }

    void SendNonFinalStat() {
        auto ev = MakeHolder<TEvDqStats>();
        FinalStat().CopyCounters(ev->Record);
        Send(ExecuterId, ev.Release());
    }

    void TrySendNonFinalStat() {
        auto now = Now();
        if (now - LastStatReport > PingPeriod) {
            SendNonFinalStat();
            LastStatReport = now;
        }
    }

public:
    void OnComputeActorState(NDq::TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(TRACE, ProviderDq)
            << SelfId()
            << " EvState TaskId: " << taskId
            << " State: " << state.GetState()
            << " PingCookie: " << ev->Cookie
            << " StatusCode: " << NYql::NDqProto::StatusIds_StatusCode_Name(state.GetStatusCode());

        if (state.HasStats() && TryAddStatsFromExtra(state.GetStats())) {
            if (ServiceCounters.Counters && !AggrPeriod) {
                ExportStats(TaskStat, taskId);
                TrySendNonFinalStat();
            }
        } else if (state.HasStats() && state.GetStats().GetTasks().size()) {
            YQL_CLOG(TRACE, ProviderDq) << " " << SelfId() << " AddStats " << taskId;
            AddStats(state.GetStats());
            if (ServiceCounters.Counters && !AggrPeriod) {
                ExportStats(TaskStat, taskId);
                TrySendNonFinalStat();
            }
        }


        TIssues localIssues;
        // TODO: don't convert issues to string
        NYql::IssuesFromMessage(state.GetIssues(), localIssues);

        switch (state.GetState()) {
            case NDqProto::COMPUTE_STATE_UNKNOWN: {
                // TODO: use issues
                TString message = "unexpected state from " + ToString(computeActor) + ", task: " + ToString(taskId);
                OnError(NYql::NDqProto::StatusIds::BAD_REQUEST, message);
                break;
            }
            case NDqProto::COMPUTE_STATE_FAILURE: {
                Issues.AddIssues(localIssues);
                OnError(state.GetStatusCode(), Issues.ToIssues());
                break;
            }
            case NDqProto::COMPUTE_STATE_EXECUTING: {
                Issues.AddIssues(localIssues);
                YQL_CLOG(TRACE, ProviderDq) << " " << SelfId() << " Executing TaskId: " << taskId;
                if (!FinishedTasks.contains(taskId)) {
                    // may get late/reordered? message
                    Executing[taskId] = Now();
                }
                SendNonFatalIssues();
                break;
            }
            case NDqProto::COMPUTE_STATE_FINISHED: {
                Executing.erase(taskId);
                FinishedTasks.insert(taskId);
                YQL_CLOG(DEBUG, ProviderDq) << " " << SelfId() << " Finish TaskId: " << taskId << ". Tasks finished: " << FinishedTasks.size() << "/" << Tasks.size();
                break;
            }
        }

        MaybeUpdateChannels();
        MaybeFinish();
    }

    void OnWakeup(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case PING_TIMER_TAG:
            if (PingPeriod) {
                auto now = Now();
                for (auto& taskActors: Executing) {
                    if (now > taskActors.second + PingPeriod) {
                        PingCookie++;
                        YQL_CLOG(TRACE, ProviderDq) << " Ping TaskId: " << taskActors.first << ", Compute ActorId: " << ActorIds[taskActors.first] << ", PingCookie: " << PingCookie;
                        Send(ActorIds[taskActors.first], new NDq::TEvDqCompute::TEvStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered, PingCookie);
                        taskActors.second = now;
                    }
                }
                Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup(PING_TIMER_TAG));
            }
            break;
        case AGGR_TIMER_TAG:
            if (AggrPeriod) {
                if (ServiceCounters.Counters) {
                    ExportStats(AggregateQueryStatsByStage(TaskStat, Stages), 0);
                }
                SendNonFinalStat();
                Schedule(AggrPeriod, new TEvents::TEvWakeup(AGGR_TIMER_TAG));
            }
            break;
        }
    };

    ::NMonitoring::TDynamicCounterPtr GroupForExport(const TCounters& stat, const TString& counterName, ui64 taskId, TString& name, std::map<TString, TString>& labels) {
        Y_UNUSED(stat);
        TString prefix;
        if (NCommon::ParseCounterName(&prefix, &labels, &name, counterName)) {
            if (prefix == "TaskRunner" && (taskId == 0 || labels["Task"] == ToString(taskId))) {
                auto group = (taskId == 0) ? ServiceCounters.Counters : ServiceCounters.Counters->GetSubgroup("Stage", ToString(Stages[taskId]));
                for (const auto& [k, v] : labels) {
                    group = group->GetSubgroup(k, v);
                }
                return group;
            }
        }
        return nullptr;
    }

private:
    static bool IsAggregatedStage(const std::map<TString, TString>& labels) {
        const auto it = labels.find("Stage");
        return it != labels.end() && it->second == "Total";
    }

    void ExportStats(const TCounters& stat, ui64 taskId) {
        YQL_CLOG(TRACE, ProviderDq) << " " << SelfId() << " ExportStats " << (taskId ? ToString(taskId) : "Summary");
        TString name;
        std::map<TString, TString> labels;
        static const TString SourceLabel = "Source";
        static const TString SinkLabel = "Sink";
        for (const auto& [k, v] : stat.Get()) {
            labels.clear();
            if (auto group = GroupForExport(stat, k, taskId, name, labels)) {
                *group->GetCounter(name) = v.Count;
                if (ServiceCounters.PublicCounters && taskId == 0 && IsAggregatedStage(labels)) {
                    TString publicCounterName;
                    bool isDeriv = false;
                    if (name == "MkqlMaxMemoryUsage") {
                        publicCounterName = "query.memory_usage_bytes";
                    } else if (name == "CpuTimeUs") {
                        publicCounterName = "query.cpu_usage_us";
                        isDeriv = true;
                    } else if (name == "Bytes") {
                        if (labels.count(SourceLabel)) publicCounterName = "query.input_bytes";
                        else if (labels.count(SinkLabel)) publicCounterName = "query.output_bytes";
                        isDeriv = true;
                    } else if (name == "RowsIn") {
                        if (labels.count(SourceLabel)) publicCounterName = "query.source_input_records";
                        else if (labels.count(SinkLabel)) publicCounterName = "query.sink_output_records"; // RowsIn == RowsOut for Sinks
                        isDeriv = true;
                    } else if (name == "MultiHop_LateThrownEventsCount") {
                        publicCounterName = "query.late_events";
                        isDeriv = true;
                    }

                    if (publicCounterName) {
                        auto& counter = *ServiceCounters.PublicCounters->GetNamedCounter("name", publicCounterName, isDeriv);
                        if (name == "MultiHop_LateThrownEventsCount") {
                            // the only incremental sensor from TaskRunner
                            counter += v.Count;
                        } else {
                            counter = v.Count;
                        }
                    }
                }
            }
        }
        for (const auto& [k, v] : stat.GetHistograms()) {
            labels.clear();
            if (auto group = GroupForExport(stat, k, taskId, name, labels)) {
                auto hist = group->GetHistogram(name, NMonitoring::ExponentialHistogram(6, 10, 10));
                hist->Reset();
                for (const auto& [bound, value] : v) {
                    hist->Collect(bound, value);
                }
            }
        }
    }

    bool TryAddStatsFromExtra(const NDqProto::TDqComputeActorStats& x) {
        NDqProto::TExtraStats extraStats;
        if (x.HasExtra() && x.GetExtra().UnpackTo(&extraStats)) {
            YQL_CLOG(TRACE, ProviderDq) << " " << SelfId() << " AddStats from extra";
            for (const auto& [name, m] : extraStats.GetStats()) {
                NYql::TCounters::TEntry value;
                value.Sum = m.GetSum();
                value.Max = m.GetMax();
                value.Min = m.GetMin();
                //value.Avg = m.GetAvg();
                value.Count = m.GetCnt();
                TaskStat.AddCounter(name, value);
            }
            return true;
        }
        return false;
    }

    void AddStats(const NDqProto::TDqComputeActorStats& x) {
        YQL_ENSURE(x.GetTasks().size() == 1);
        auto& s = x.GetTasks(0);
        ui64 taskId = s.GetTaskId();

#define ADD_COUNTER(name) \
        if (stats.Get ## name()) { \
            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, #name), stats.Get ## name ()); \
        }

        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)}
        };

        auto& stats = s;
        // basic stats
        ADD_COUNTER(CpuTimeUs)
        ADD_COUNTER(ComputeCpuTimeUs)
        ADD_COUNTER(SourceCpuTimeUs)
        ADD_COUNTER(PendingInputTimeUs)
        ADD_COUNTER(PendingOutputTimeUs)
        ADD_COUNTER(FinishTimeUs)
        ADD_COUNTER(InputRows)
        ADD_COUNTER(InputBytes)
        ADD_COUNTER(OutputRows)
        ADD_COUNTER(OutputBytes)

        // profile stats
        ADD_COUNTER(BuildCpuTimeUs)
        ADD_COUNTER(WaitTimeUs)
        ADD_COUNTER(WaitOutputTimeUs)

        for (const auto& ingress : s.GetIngress()) {
            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Ingress" + ingress.GetName() + "Bytes"), ingress.GetBytes());
        }

        for (const auto& egress : s.GetEgress()) {
            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Egress" + egress.GetName() + "Bytes"), egress.GetBytes());
        }

        if (auto v = x.GetMkqlMaxMemoryUsage()) {
            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "MkqlMaxMemoryUsage"), v);
        }

        for (const auto& stat : s.GetMkqlStats()) {
            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, stat.GetName()), stat.GetValue());
        }

        if (stats.ComputeCpuTimeByRunSize()) {
            auto& hist = TaskStat.GetHistogram(TaskStat.GetCounterName("TaskRunner", labels, "ComputeTimeByRunMs"));
            for (const auto& bucket : s.GetComputeCpuTimeByRun()) {
                hist[bucket.GetBound()] = bucket.GetValue();
            }
        }

        // compilation stats
//        ADD_COUNTER(MkqlTotalNodes)
//        ADD_COUNTER(MkqlCodegenFunctions)
//        ADD_COUNTER(CodeGenTotalInstructions)
//        ADD_COUNTER(CodeGenTotalFunctions)
//
//        ADD_COUNTER(CodeGenFullTime)
//        ADD_COUNTER(CodeGenFinalizeTime)
//        ADD_COUNTER(CodeGenModulePassTime)

//        if (stats.GetFinishTs() >= stats.GetStartTs()) {
//            TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Total"), stats.GetFinishTs() - stats.GetStartTs());
//        }

        for (const auto& stats : s.GetInputChannels()) {
            std::map<TString, TString> labels = {
                {"Task", ToString(taskId)},
                {"InputChannel", ToString(stats.GetChannelId())}
            };

            ADD_COUNTER(Chunks);
            ADD_COUNTER(Bytes);
            ADD_COUNTER(RowsIn);
            ADD_COUNTER(RowsOut);
            ADD_COUNTER(MaxMemoryUsage);
            ADD_COUNTER(DeserializationTimeUs);

//            if (stats.GetFinishTs() >= stats.GetStartTs()) {
//                TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Total"), stats.GetFinishTs() - stats.GetStartTs());
//            }
        }

        for (const auto& stats : s.GetOutputChannels()) {
            std::map<TString, TString> labels = {
                {"Task", ToString(taskId)},
                {"OutputChannel", ToString(stats.GetChannelId())}
            };

            ADD_COUNTER(Chunks)
            ADD_COUNTER(Bytes);
            ADD_COUNTER(RowsIn);
            ADD_COUNTER(RowsOut);
            ADD_COUNTER(MaxMemoryUsage);

            ADD_COUNTER(SerializationTimeUs);
            ADD_COUNTER(BlockedByCapacity);

            ADD_COUNTER(SpilledBytes);
            ADD_COUNTER(SpilledRows);
            ADD_COUNTER(SpilledBlobs);

//            if (stats.GetFinishTs() >= stats.GetStartTs()) {
//                TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Total"), stats.GetFinishTs() - stats.GetStartTs());
//            }
        }

        for (const auto& stats : s.GetSources()) {
            std::map<TString, TString> labels = {
                {"Task", ToString(taskId)},
                {"Source", ToString(stats.GetInputIndex())}
            };

            ADD_COUNTER(Chunks);
            ADD_COUNTER(Bytes);
            ADD_COUNTER(IngressBytes)
            ADD_COUNTER(RowsIn);
            ADD_COUNTER(RowsOut);
            ADD_COUNTER(MaxMemoryUsage);

            ADD_COUNTER(ErrorsCount);

//            if (stats.GetFinishTs() >= stats.GetStartTs()) {
//                TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Total"), stats.GetFinishTs() - stats.GetStartTs());
//            }
        }

        for (const auto& stats : s.GetSinks()) {
            std::map<TString, TString> labels = {
                {"Task", ToString(taskId)},
                {"Sink", ToString(stats.GetOutputIndex())}
            };

            ADD_COUNTER(Chunks)
            ADD_COUNTER(Bytes);
            ADD_COUNTER(EgressBytes)
            ADD_COUNTER(RowsIn);
            ADD_COUNTER(RowsOut);
            ADD_COUNTER(MaxMemoryUsage);

            ADD_COUNTER(ErrorsCount);

//            if (stats.GetFinishTs() >= stats.GetStartTs()) {
//                TaskStat.SetCounter(TaskStat.GetCounterName("TaskRunner", labels, "Total"), stats.GetFinishTs() - stats.GetStartTs());
//            }
        }

#undef ADD_COUNTER
    }

    void MaybeFinish() {
        if (!Finished && !Tasks.empty() && FinishedTasks.size() == Tasks.size()) {
            Finish();
        }
    }

    void SetTaskCountMetric(ui64 count) {
        if (!ServiceCounters.Counters) {
            return;
        }
        *ServiceCounters.Counters->GetCounter("TaskCount") = count;

        if (!ServiceCounters.PublicCounters) {
            return;
        }
        *ServiceCounters.PublicCounters->GetNamedCounter("name", "query.running_tasks") = count;
    }

public:
    void OnReadyState(TEvReadyState::TPtr& ev) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);

        TaskStat.AddCounters(ev->Get()->Record);

        const auto& tasks = ev->Get()->Record.GetTask();
        const auto& actorIds = ev->Get()->Record.GetActorId();
        Y_VERIFY(tasks.size() == actorIds.size());

        SetTaskCountMetric(tasks.size());

        for (int i = 0; i < static_cast<int>(tasks.size()); ++i) {
            auto actorId = ActorIdFromProto(actorIds[i]);
            auto& task = tasks[i];
            Tasks.emplace_back(task, actorId);
            ActorIds.emplace(task.GetId(), actorId);
            TaskIds.emplace(actorId, task.GetId());
            Yql::DqsProto::TTaskMeta taskMeta;
            task.GetMeta().UnpackTo(&taskMeta);
            Stages.emplace(task.GetId(), taskMeta.GetStageId());
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Ready State: " << SelfId();

        MaybeUpdateChannels();

        if (PingPeriod) {
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup(PING_TIMER_TAG));
        }
        if (AggrPeriod) {
            Schedule(AggrPeriod, new TEvents::TEvWakeup(AGGR_TIMER_TAG));
        }
    }

private:
    void MaybeUpdateChannels() {
        if (Tasks.empty() ||  ChannelsUpdated || Tasks.size() != Executing.size()) {
            return;
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Update channels";
        for (const auto& [task, actorId] : Tasks) {
            auto ev = MakeHolder<NDq::TEvDqCompute::TEvChannelsInfo>();

            for (const auto& input : task.GetInputs()) {
                for (const auto& channel : input.GetChannels()) {
                    *ev->Record.AddUpdate() = channel;
                }
            }

            for (const auto& output : task.GetOutputs()) {
                for (const auto& channel : output.GetChannels()) {
                    *ev->Record.AddUpdate() = channel;
                }
            }

            YQL_CLOG(DEBUG, ProviderDq) << task.GetId() << " " << ev->Record.ShortDebugString();

            Send(actorId, ev.Release());
        }
        ChannelsUpdated = true;
    }

public:
    void OnResultFailure(TEvDqFailure::TPtr& ev) {
        if (Finished) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(WARN, ProviderDq) << "TEvDqFailure IGNORED when Finished from " << ev->Sender;
        } else {
            FinalStat().FlushCounters(ev->Get()->Record); // histograms will NOT be reported
            Send(ExecuterId, ev->Release().Release());
            Finished = true;
        }
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& message, const TIssues& subIssues) {
        TIssue issue(message);
        for (const TIssue& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<TIssue>(i));
        }
        TIssues issues;
        issues.AddIssue(std::move(issue));
        OnError(statusCode, issues);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << "OnError " << issues.ToOneLineString() << " " << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode);
        if (Finished) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(WARN, ProviderDq) << "OnError IGNORED when Finished";
        } else {
            auto req = MakeHolder<TEvDqFailure>(statusCode, issues);
            FinalStat().FlushCounters(req->Record);
            Send(ExecuterId, req.Release());
            Finished = true;
        }
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& message) {
        auto issueCode = NCommon::NeedFallback(statusCode)
            ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR
            : TIssuesIds::DQ_GATEWAY_ERROR;
        OnError(statusCode, TIssues({TIssue(message).SetCode(issueCode, TSeverityIds::S_ERROR)}));
    }

private:
    void Finish() {
        if (ServiceCounters.Counters && AggrPeriod) {
            ExportStats(AggregateQueryStatsByStage(TaskStat, Stages), 0); // force metrics upload on Finish when Aggregated
        }
        Send(ExecuterId, new TEvGraphFinished());
        Finished = true;
    }

public:
    void OnQueryResult(TEvQueryResponse::TPtr& ev) {
        YQL_ENSURE(!ev->Get()->Record.HasResultSet() && ev->Get()->Record.GetYson().empty());
        FinalStat().FlushCounters(ev->Get()->Record);
        if (!Issues.Empty()) {
            IssuesToMessage(Issues.ToIssues(), ev->Get()->Record.MutableIssues());
        }
        Send(ResultId, ev->Release().Release());
    }

private:
    TCounters FinalStat() {
        return AggrPeriod ? AggregateQueryStatsByStage(TaskStat, Stages) : TaskStat;
    }


    bool ChannelsUpdated = false;
    TVector<std::pair<NDqProto::TDqTask, TActorId>> Tasks;
    THashSet<ui64> FinishedTasks;
    THashMap<ui64, TInstant> Executing;
    THashMap<ui64, TActorId> ActorIds;
    THashMap<TActorId, ui64> TaskIds;
    THashMap<ui64, ui64> Stages;
    const NActors::TActorId ExecuterId;
    const NActors::TActorId ResultId;
    const TString TraceId;
    TDqConfiguration::TPtr Settings;
    bool Finished = false;
    TCounters TaskStat;
    NYql::NCommon::TServiceCounters ServiceCounters;
    TDuration PingPeriod = TDuration::Zero();
    TDuration AggrPeriod = TDuration::Zero();
    NYql::NDq::GroupedIssues Issues;
    ui64 PingCookie = 0;
    TInstant LastStatReport;
};

} /* namespace NYql */
