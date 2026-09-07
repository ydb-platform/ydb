#include "dq_compute_actor_tracing.h"
#include "dq_compute_actor.h"

namespace NYql::NDq {

void FillComputeTraceStats(const TDqTaskRunnerStats& source, NDqProto::TDqTaskStats& target) {
    if (!target.GetStartTimeMs()) {
        target.SetStartTimeMs(source.StartTs.MilliSeconds());
    }
    target.SetSpillingComputeWriteBytes(source.SpillingComputeWriteBytes);
    target.SetSpillingChannelWriteBytes(source.SpillingChannelWriteBytes);
}

void AddComputeTraceAttributes(NWilson::TSpan& span, const NDqProto::TDqComputeActorStats& stats) {
    if (!span) {
        return;
    }
    span.Attribute("ydb.cpu_us", static_cast<i64>(stats.GetCpuTimeUs()));
    if (stats.TasksSize() == 1) {
        const auto& task = stats.GetTasks(0);
        span.Attribute("ydb.input_rows", static_cast<i64>(task.GetInputRows()));
        span.Attribute("ydb.output_rows", static_cast<i64>(task.GetOutputRows()));
        span.Attribute("ydb.wait_us", static_cast<i64>(task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs()));
        span.Attribute("ydb.compute_cpu_us", static_cast<i64>(task.GetComputeCpuTimeUs()));
        span.Attribute("ydb.build_cpu_us", static_cast<i64>(task.GetBuildCpuTimeUs()));
        span.Attribute("ydb.node_id", static_cast<i64>(task.GetNodeId()));
        span.Attribute("ydb.spilled_bytes", static_cast<i64>(
            task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes()));
        if (task.GetCreateTimeMs() && task.GetStartTimeMs() >= task.GetCreateTimeMs()) {
            span.Attribute("ydb.queue_delay_us", static_cast<i64>(
                (task.GetStartTimeMs() - task.GetCreateTimeMs()) * 1000));
        }
    }
}

}
