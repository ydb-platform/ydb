#include "kqp_streaming_helper.h"

namespace NKikimr::NKqp {

namespace {

class TStreamingQueryCounters : public IStreamingQueryCounters {
public:
    TStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path)
        : Path(path)
        , Counters(counters)
    {}

    ~TStreamingQueryCounters() {
        if (!HostGroup) {
            return;
        }
        HostGroup->RemoveSubgroup("path", Path);
    }

    void Update(const TAggExecStat& stats) override {
        if (!SubGroup) {
            Init();
        }
        CpuMs->Set(stats.CpuTimeMs);
        MemoryUsageBytes->Set(stats.MemoryUsageBytes);
        UptimeSeconds->Set(stats.DurationSeconds);
        TaskCount->Set(stats.TasksCount);
        InputBytes->Set(stats.InputBytes);
        OutputBytes->Set(stats.OutputBytes);
    }

private:
    void Init() {
        SubGroup = Counters->GetSubgroup("subsystem", "streaming_queries");
        HostGroup = SubGroup->GetSubgroup("host", "");
        auto queryGroup = HostGroup->GetSubgroup("path", Path);
        CpuMs = queryGroup->GetCounter("streaming.query.cpu.usage.milliseconds", true);
        MemoryUsageBytes = queryGroup->GetCounter("streaming.query.memory.usage.bytes");
        UptimeSeconds = queryGroup->GetCounter("streaming.query.uptime.seconds");
        TaskCount = queryGroup->GetCounter("streaming.query.tasks.count");
        InputBytes = queryGroup->GetCounter("streaming.query.input.bytes", true);
        OutputBytes = queryGroup->GetCounter("streaming.query.output.bytes", true);
    }

private:
    const TString Path;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounterPtr HostGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr CpuMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr MemoryUsageBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr UptimeSeconds;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBytes;
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

} // namespace

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path) {
    return MakeIntrusive<TStreamingQueryCounters>(counters, path);
}

} // namespace NKikimr::NKqp
