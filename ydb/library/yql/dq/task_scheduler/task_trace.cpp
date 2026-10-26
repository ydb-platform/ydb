#include "task_trace.h"
#include "task_scheduler.h"

#include <library/cpp/json/json_writer.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYql::NDq {
namespace {

NJson::TJsonValue Object() { return NJson::TJsonValue(NJson::JSON_MAP); }
NJson::TJsonValue Array() { return NJson::TJsonValue(NJson::JSON_ARRAY); }

template <class T>
void Optional(NJson::TJsonValue& json, TStringBuf name, const std::optional<T>& value) {
    if (value) {
        json[name] = *value;
    } else {
        json[name] = NJson::TJsonValue();
    }
}

TStringBuf StateName(ETaskSchedulingState state) {
    switch (state) {
        case ETaskSchedulingState::Blocked: return "blocked";
        case ETaskSchedulingState::Ready: return "ready";
        case ETaskSchedulingState::Dispatched: return "dispatched";
        case ETaskSchedulingState::Running: return "running";
        case ETaskSchedulingState::Cancelled: return "cancelled";
        case ETaskSchedulingState::Finished: return "finished";
    }
    return "unknown";
}
TStringBuf StopName(ETaskStopReason reason) {
    switch (reason) {
        case ETaskStopReason::None: return "none";
        case ETaskStopReason::Waiting: return "waiting";
        case ETaskStopReason::Runnable: return "runnable";
        case ETaskStopReason::Budget: return "budget";
        case ETaskStopReason::RunnerFinished: return "runner_finished";
        case ETaskStopReason::CpuQuota: return "cpu_quota";
        case ETaskStopReason::Cancelled: return "cancelled";
    }
    return "unknown";
}
TStringBuf WaitName(ETaskWaitReason reason) {
    switch (reason) {
        case ETaskWaitReason::Unknown: return "unknown";
        case ETaskWaitReason::Input: return "input";
        case ETaskWaitReason::Output: return "output";
        case ETaskWaitReason::SpillRead: return "spill_read";
        case ETaskWaitReason::SpillWrite: return "spill_write";
        case ETaskWaitReason::AsyncIo: return "async_io";
    }
    return "unknown";
}
TStringBuf SideName(ETaskInputSide side) {
    switch (side) {
        case ETaskInputSide::None: return "none";
        case ETaskInputSide::Left: return "left";
        case ETaskInputSide::Right: return "right";
        case ETaskInputSide::Build: return "build";
        case ETaskInputSide::Probe: return "probe";
    }
    return "none";
}
NJson::TJsonValue ProgressJson(const TTaskRunProgress& progress) {
    auto json = Object();
    json["cpu_us"] = progress.CpuTimeUs;
    json["elapsed_us"] = progress.ElapsedTimeUs;
    json["input_items"] = progress.InputItems;
    json["input_rows"] = progress.InputRows;
    json["input_bytes"] = progress.InputBytes;
    json["output_rows"] = progress.OutputRows;
    json["output_bytes"] = progress.OutputBytes;
    json["runner_invoked"] = progress.RunnerInvoked;
    json["quota_rejected"] = progress.QuotaRejected;
    json["finished"] = progress.Finished;
    json["budget_exhausted"] = progress.BudgetExhausted;
    json["observed_progress"] = progress.HasObservedProgress();
    return json;
}

NJson::TJsonValue ConditionJson(const TTaskWaitCondition& condition) {
    auto any = Array();
    for (const auto& group : condition.AnyOf) {
        auto all = Array();
        for (const auto& dependency : group) {
            auto term = Object();
            term["dependency"] = ToString(dependency.Id);
            term["version"] = dependency.Version;
            all.AppendValue(std::move(term));
        }
        any.AppendValue(std::move(all));
    }
    return any;
}
}

NJson::TJsonValue TTaskChannelSnapshot::ToJson(ui64 channelId, bool input) const {
    auto json = Object();
    json["channel_id"] = ToString(channelId);
    json["direction"] = input ? "input" : "output";
    Optional(json, "buffered_bytes", BufferedBytes);
    Optional(json, "buffered_chunks", BufferedChunks);
    Optional(json, "inflight_bytes", InflightBytes);
    Optional(json, "spilled_bytes", SpilledBytes);
    Optional(json, "capacity_bytes", CapacityBytes);
    Optional(json, "empty", Empty);
    Optional(json, "finished", Finished);
    Optional(json, "finish_pushed", FinishPushed);
    Optional(json, "early_finished", EarlyFinished);
    Optional(json, "aborted", Aborted);
    Optional(json, "paused", Paused);
    Optional(json, "fill_level", FillLevel);
    return json;
}

TQueryTaskTrace::TQueryTaskTrace(NActors::TActorId query, ui32 nodeId, TTaskTraceSettings settings)
    : Query(query), NodeId(nodeId), Settings(settings) {}

void TQueryTaskTrace::Event(TStringBuf name, ui64 taskId, NActors::TActorId actor, NJson::TJsonValue args,
    TMonotonic at, std::optional<ui64> durationUs)
{
    auto json = Object();
    json["name"] = name;
    json["cat"] = "dq";
    json["ph"] = durationUs ? "X" : "i";
    if (!durationUs) {
        json["s"] = "t";
    } else {
        json["dur"] = *durationUs;
    }
    // A wall-clock anchor permits approximate cross-node alignment. Local
    // durations/order use the monotonic clock, never an adjustable wall clock.
    json["ts"] = WallStart.MicroSeconds() + (at - Start).MicroSeconds();
    json["pid"] = NodeId;
    json["tid"] = taskId;
    args["query"] = ToString(Query);
    args["task_id"] = ToString(taskId);
    args["actor"] = ToString(actor);
    json["args"] = std::move(args);
    auto record = NJson::WriteJson(json, false);
    std::lock_guard lock(Mutex);
    if (Events.size() >= Settings.MaxEvents || record.size() > Settings.MaxBytes - Bytes) {
        ++Dropped;
        return;
    }
    Bytes += record.size();
    Events.push_back(std::move(record));
}

void TQueryTaskTrace::State(const TTaskSchedulingSnapshot& state) {
    auto args = Object();
    args["revision"] = state.Revision;
    args["generation"] = state.Generation;
    args["state"] = StateName(state.State);
    args["stop_reason"] = StopName(state.StopReason);
    args["priority"] = static_cast<ui32>(state.Priority);
    args["last_run_progress"] = ProgressJson(state.Progress);
    auto classification = Object();
    classification["readiness"] = TaskReadinessName(state.Classification.Readiness);
    classification["work"] = TaskWorkName(state.Classification.Work);
    classification["coverage"] = TaskCoverageName(state.Classification.Coverage);
    classification["legacy_wakeups"] = state.Classification.LegacyWakeups;
    classification["operator_id"] = ToString(state.Classification.OperatorId);
    classification["operator_name"] = state.Classification.OperatorName;
    classification["phase"] = state.Classification.Phase;
    args["classification"] = std::move(classification);
    args["resume_any_of_all"] = ConditionJson(state.ResumeCondition);
    auto waits = Array();
    for (const auto& wait : state.Waits) {
        auto item = Object();
        item["reason"] = WaitName(wait.Reason);
        item["precision"] = wait.Precision == ETaskWaitPrecision::Exact ? "exact"
            : wait.Precision == ETaskWaitPrecision::ObservedInputs ? "observed_inputs" : "opaque";
        item["origin"] = wait.Origin == ETaskWaitOrigin::Operator ? "operator"
            : wait.Origin == ETaskWaitOrigin::ChannelProbe ? "channel_probe" : "opaque_fallback";
        item["operator_id"] = ToString(wait.OperatorId);
        item["operator_name"] = wait.OperatorName;
        item["input_side"] = SideName(wait.InputSide);
        auto path = Array();
        for (const auto& input : wait.InputPath) {
            auto frame = Object();
            frame["operator_id"] = ToString(input.OperatorId);
            frame["operator_name"] = input.OperatorName;
            frame["side"] = SideName(input.Side);
            path.AppendValue(std::move(frame));
        }
        item["input_path"] = std::move(path);
        item["any_of_all"] = ConditionJson(wait.Condition);
        waits.AppendValue(std::move(item));
    }
    args["waits"] = std::move(waits);
    Event("state", state.TaskId, state.Actor, std::move(args));
}

void TQueryTaskTrace::Graph(const TQueryTaskGraph& graph, const TVector<ui64>& localTasks) {
    auto args = Object();
    auto tasks = Array();
    for (const auto& task : graph.Tasks) {
        auto json = Object();
        json["task_id"] = ToString(task.TaskId);
        json["stage_id"] = task.StageId;
        json["planned_node_id"] = task.PlannedNodeId;
        json["inputs"] = task.InputsCount;
        json["outputs"] = task.OutputsCount;
        json["source"] = task.HasSource;
        json["sink"] = task.HasSink;
        json["effects"] = task.HasEffects;
        tasks.AppendValue(std::move(json));
    }
    args["tasks"] = std::move(tasks);
    auto channels = Array();
    for (const auto& channel : graph.Channels) {
        auto json = Object();
        json["channel_id"] = ToString(channel.ChannelId);
        json["src_task"] = ToString(channel.SrcTaskId);
        json["dst_task"] = ToString(channel.DstTaskId);
        json["src_output"] = channel.SrcOutputIndex;
        json["dst_input"] = channel.DstInputIndex;
        json["routing_kind"] = channel.RoutingKind;
        channels.AppendValue(std::move(json));
    }
    args["channels"] = std::move(channels);
    auto local = Array();
    for (ui64 task : localTasks) {
        local.AppendValue(ToString(task));
    }
    args["local_tasks"] = std::move(local);
    Event("graph", 0, {}, std::move(args));
}

void TQueryTaskTrace::Run(ui64 taskId, NActors::TActorId actor, TMonotonic start, const TTaskRunProgress& progress,
    NJson::TJsonValue before, NJson::TJsonValue after, bool failed, std::optional<ui32> runnerStatus)
{
    auto args = Object();
    args["progress"] = ProgressJson(progress);
    args["channels_before"] = std::move(before);
    args["channels_after"] = std::move(after);
    args["failed"] = failed;
    Optional(args, "runner_status", runnerStatus);
    Event("runner", taskId, actor, std::move(args), start, progress.ElapsedTimeUs);
}

void TQueryTaskTrace::Finish() {
    std::lock_guard lock(Mutex);
    if (!Completed) {
        Completed = TMonotonic::Now();
    }
}
bool TQueryTaskTrace::IsCompleted() const {
    std::lock_guard lock(Mutex);
    return bool(Completed);
}
bool TQueryTaskTrace::IsExpired(TMonotonic now) const {
    std::lock_guard lock(Mutex);
    return Completed && now - Completed >= Settings.Retention;
}
NJson::TJsonValue TQueryTaskTrace::SummaryLocked() const {
    auto json = Object();
    json["query"] = ToString(Query);
    json["node_id"] = NodeId;
    json["schema_version"] = 1;
    json["clock"] = "monotonic_with_wall_anchor";
    json["scope"] = "synchronous_channels_v2";
    json["completion_scope"] = "local_graph_released";
    json["started_unix_us"] = WallStart.MicroSeconds();
    json["started_monotonic_us"] = Start.GetValue();
    json["completed"] = bool(Completed);
    json["events"] = ui64(Events.size());
    json["bytes"] = Bytes;
    json["dropped_events"] = Dropped;
    json["truncated"] = Dropped != 0;
    return json;
}
NJson::TJsonValue TQueryTaskTrace::Summary() const {
    std::lock_guard lock(Mutex);
    return SummaryLocked();
}
TString TQueryTaskTrace::Export() const {
    std::lock_guard lock(Mutex);
    TStringBuilder out;
    out << "{\"dqTrace\":" << NJson::WriteJson(SummaryLocked(), false) << ",\"traceEvents\":[";
    bool first = true;
    for (const auto& record : Events) {
        if (!first) { out << ','; }
        first = false;
        out << record;
    }
    out << "],\"displayTimeUnit\":\"ms\"}";
    return out;
}

TTaskTraceRegistry::TTaskTraceRegistry(ui32 nodeId, TTaskTraceSettings settings) : NodeId(nodeId), Settings(settings) {}
void TTaskTraceRegistry::PruneLocked() {
    const auto now = TMonotonic::Now();
    for (auto it = Queries.begin(); it != Queries.end();) {
        if (it->second->IsExpired(now)) {
            const auto expired = it++;
            Queries.erase(expired);
        } else {
            ++it;
        }
    }
}
std::shared_ptr<TQueryTaskTrace> TTaskTraceRegistry::GetOrCreate(NActors::TActorId query) {
    if (!Settings.Enabled) { return {}; }
    std::lock_guard lock(Mutex);
    PruneLocked();
    if (auto it = Queries.find(query); it != Queries.end()) { return it->second; }
    if (Queries.size() >= Settings.MaxQueries) {
        // Prefer keeping active captures. Completed captures can be evicted
        // earlier than retention under pressure; the list exposes admission loss.
        auto victim = Queries.end();
        for (auto it = Queries.begin(); it != Queries.end(); ++it) {
            if (it->second.use_count() == 1 && it->second->IsCompleted()) {
                victim = it;
                break;
            }
        }
        if (victim != Queries.end()) { Queries.erase(victim); }
    }
    if (Queries.size() >= Settings.MaxQueries) { ++Rejected; return {}; }
    auto trace = std::make_shared<TQueryTaskTrace>(query, NodeId, Settings);
    Queries.emplace(query, trace);
    return trace;
}
TString TTaskTraceRegistry::Export(TStringBuf query) {
    std::shared_ptr<TQueryTaskTrace> trace;
    auto list = Object();
    auto queries = Array();
    {
        std::lock_guard lock(Mutex);
        PruneLocked();
        for (const auto& [id, value] : Queries) {
            if (query && ToString(id) == query) { trace = value; break; }
            if (!query) { queries.AppendValue(value->Summary()); }
        }
        list["enabled"] = Settings.Enabled;
        list["rejected_admissions"] = Rejected;
    }
    if (query) { return trace ? trace->Export() : "{\"error\":\"trace_not_found\"}"; }
    list["queries"] = std::move(queries);
    return NJson::WriteJson(list, false);
}

} // namespace NYql::NDq
