#pragma once

#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <util/string/split.h>

namespace NYql {

struct TCounters {

    static TString GetCounterName(const TString& prefix, const std::map<TString, TString>& labels, const TString& name) {
        TStringBuilder counterName;
        counterName << prefix << ":";
        for (const auto& [k, v] : labels) {
            counterName << k << "=" << v << ",";
        }
        counterName << "Name=" << name;
        return counterName;
    }

    void CopyCounters(TOperationStatistics& statistics) const {
        for (const auto& [k, v] : Counters) {
            TOperationStatistics::TEntry entry(
                k,
                v.Sum,
                v.Max,
                v.Min,
                v.Avg,
                v.Count
            );
            statistics.Entries.push_back(entry);
        }
    }

    template<typename T>
    void CopyCounters(T& t) const {
        for (const auto& [k, v] : Counters) {
            auto* metric = t.AddMetric();
            metric->SetName(k);
            metric->SetSum(v.Sum);
            metric->SetMax(v.Max);
            metric->SetMin(v.Min);
            metric->SetAvg(v.Avg);
            metric->SetCount(v.Count);
        }
    }

    template<typename T>
    void FlushCounters(T& t) const {
        CopyCounters(t);
        Counters.clear();
    }

    template<typename T>
    void AddCounter(const TString& name, T value) const {
        Counters[name].Add(TEntry(value));
    }

    template<typename T>
    void SetCounter(const TString& name, T value) const {
        Counters[name] = TEntry(value);
    }

    THashMap<i64, ui64>& GetHistogram(const TString& name) {
        return Histograms[name];
    }

    void AddTimeCounter(const TString& name, i64 value) const {
        AddCounter(name, TDuration::MilliSeconds(value));
    }

    void AddCounter(const TString& name, TDuration value) const {
        auto val = value.MilliSeconds();
        auto& counter = Counters[name];
        counter.Sum += val;
        counter.Min = counter.Count == 0
            ? val
            : Min<i64>(counter.Min, val);
        counter.Max = counter.Count == 0
            ? val
            : Max<i64>(counter.Max, val);
        counter.Avg = counter.Sum / (counter.Count + 1);
        counter.Count += 1;
    }

    template<typename T>
    void AddCounters(const T& t) const {
        for (const auto& m : t.GetMetric()) {
            TEntry value;
            value.Sum = m.GetSum();
            value.Max = m.GetMax();
            value.Min = m.GetMin();
            value.Avg = m.GetAvg();
            value.Count = m.GetCount();
            AddCounter(m.GetName(), value);
        }
    }

    template<typename T>
    void AddCounters2(const T& t) const {
        for (const auto& m : t) {
            TEntry value;
            value.Sum = m.Sum;
            value.Max = m.Max;
            value.Min = m.Min;
            value.Avg = m.Avg;
            value.Count = m.Count;
            AddCounter(m.Name, value);
        }
    }

    void StartCounter(const TString& name, TInstant now = TInstant::Now()) const {
        if (!Start.contains(name)) {
            Start[name] = now;
        }
    }

    void FlushCounter(const TString& name) const {
        auto it = Start.find(name);
        if (it != Start.end()) {
            AddCounter(name, TInstant::Now() - it->second);
            Start.erase(it);
        }
    }

    const auto& Get() const {
        return Counters;
    }

    const auto& GetHistograms() const {
        return Histograms;
    }

    struct TEntry {
        i64 Sum = 0;
        i64 Max = 0;
        i64 Min = 0;
        i64 Avg = 0;
        i64 Count = 0;

        TEntry() = default;
        explicit TEntry(i64 value) {
            Sum = value;
            Max = value;
            Min = value;
            Avg = value;
            Count = 1;
        }

        void Add(const TEntry& entry) {
            if (entry.Count) {
                Sum += entry.Sum;
                Min = (Count == 0) ? entry.Min : ::Min(Min, entry.Min);
                Max = (Count == 0) ? entry.Max : ::Max(Max, entry.Max);
                Count += entry.Count;
                Avg = Sum / Count;
            }
        }
    };

    struct TCounterBlock {
        TCounterBlock(const TCounters* parent, const TString& name)
            : Name(name)
            , StartTime(TInstant::Now())
            , Parent(parent)
        { }

        ~TCounterBlock() {
            Parent->AddCounter(Name, TInstant::Now() - StartTime);
        }

        const TString Name;
        const TInstant StartTime;
        const TCounters* Parent;
    };

    std::unique_ptr<TCounterBlock> MeasureBlock(const TString& name) const {
        return std::make_unique<TCounterBlock>(this, name);
    }

    template<typename T>
    T Measure(const TString& name, const std::function<T(void)>& f) const {
        MeasureBlock(name);
        return f();
    }

    void AddCounter(const TString& name, const TEntry& value) const {
        Counters[name].Add(value);
    }

    void Clear() const {
        Counters.clear();
        Start.clear();
    }

#define ADD_COUNTER(name) \
    do {                                                                \
        auto value = currentStats.name - oldStats.name;                 \
        if (value) {                                                    \
            AddCounter(GetCounterName("TaskRunner", labels, #name), value); \
        }                                                               \
        oldStats.name = currentStats.name;                              \
    } while (0);

    void AddInputChannelStats(
        const NDq::TDqInputChannelStats& currentStats,
        NDq::TDqInputChannelStats& oldStats,
        ui64 taskId,
        ui64 channelId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"InputChannel", ToString(channelId)}
        };

        ADD_COUNTER(Chunks);
        ADD_COUNTER(Bytes);
        ADD_COUNTER(RowsIn);
        ADD_COUNTER(RowsOut);
        ADD_COUNTER(RowsInMemory);
        ADD_COUNTER(MaxMemoryUsage);
        ADD_COUNTER(DeserializationTime);
    }

    void AddSourceStats(
        const NDq::TDqAsyncInputBufferStats& currentStats,
        NDq::TDqAsyncInputBufferStats& oldStats,
        ui64 taskId, ui64 inputIndex)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"SourceIndex", ToString(inputIndex)}
        };

        ADD_COUNTER(Chunks);
        ADD_COUNTER(Bytes);
        ADD_COUNTER(RowsIn);
        ADD_COUNTER(RowsOut);
        ADD_COUNTER(RowsInMemory);
        ADD_COUNTER(MaxMemoryUsage);
        ADD_COUNTER(InputIndex);
    }

    void AddOutputChannelStats(
        const NDq::TDqOutputChannelStats& currentStats,
        NDq::TDqOutputChannelStats& oldStats,
        ui64 taskId, ui64 channelId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"OutputChannel", ToString(channelId)}
        };

        ADD_COUNTER(Chunks)
        ADD_COUNTER(Bytes);
        ADD_COUNTER(RowsIn);
        ADD_COUNTER(RowsOut);
        ADD_COUNTER(MaxMemoryUsage);
        ADD_COUNTER(MaxRowsInMemory);

        ADD_COUNTER(SerializationTime);

        ADD_COUNTER(SpilledBytes);
        ADD_COUNTER(SpilledRows);
        ADD_COUNTER(SpilledBlobs);
    }

    void AddTaskRunnerStats(
        const NDq::TDqTaskRunnerStats& currentStats,
        NDq::TDqTaskRunnerStats& oldStats,
        ui64 taskId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)}
        };

        // basic stats
        ADD_COUNTER(ComputeCpuTime)
        ADD_COUNTER(BuildCpuTime)

        // profile stats
        ADD_COUNTER(WaitTime)
        ADD_COUNTER(WaitOutputTime)
    }

#undef ADD_COUNTER

protected:

    mutable THashMap<TString, TEntry> Counters;
    mutable THashMap<TString, THashMap<i64, ui64>> Histograms;
    mutable THashMap<TString, TInstant> Start;
};

TCounters AggregateQueryStatsByStage(TCounters& queryStat, const THashMap<ui64, ui64>& task2Stage);

} // namespace NYql
