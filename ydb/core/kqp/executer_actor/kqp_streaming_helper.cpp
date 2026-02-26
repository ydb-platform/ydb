#include "kqp_streaming_helper.h"

namespace NKikimr::NKqp {

namespace {

struct TStreamingQueryCounters : public IStreamingQueryCounters {

    TStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path)
        : Path(path)
    {
        SubGroup = counters->GetSubgroup("subsystem", "streaming_queries");
        auto queryGroup = SubGroup->GetSubgroup("path", Path);
        CpuMs = queryGroup->GetCounter("streaming.query.cpu.usage.milliseconds");
        MemoryUsageBytes = queryGroup->GetCounter("streaming.query.memory.usage.bytes");
        UptimeSeconds = queryGroup->GetCounter("streaming.query.uptime.seconds");
        TaskCount = queryGroup->GetCounter("streaming.query.tasks.count");
        InputBytes = queryGroup->GetCounter("streaming.query.input.bytes");
        OutputBytes = queryGroup->GetCounter("streaming.query.output.bytes");
    }

    ~TStreamingQueryCounters() {
        SubGroup->RemoveSubgroup("path", Path);
    }

    void Update(const TAggExecStat& stats) override {
        CpuMs->Set(stats.CpuTimeMs);
        MemoryUsageBytes->Set(stats.MemoryUsageBytes);
        UptimeSeconds->Set(stats.DurationSeconds);
        TaskCount->Set(stats.TasksCount);
        InputBytes->Set(stats.InputBytes);
        OutputBytes->Set(stats.OutputBytes);
    }

    const TString Path;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr CpuMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr MemoryUsageBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr UptimeSeconds;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBytes;
};

} // namespace

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path) {
    return MakeIntrusive<TStreamingQueryCounters>(counters, path);
}

} // namespace NKikimr::NKqp
