#pragma once

#include "cube.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProducerCounters)

struct TProducerCounters final
{
    std::string Prefix;
    TTagSet ProducerTags;
    TSensorOptions Options;

    THashMap<std::string, std::pair<TGauge, i64>> Gauges;
    THashMap<std::string, std::tuple<TCounter, i64, i64>> Counters;
    THashMap<TTag, std::pair<TProducerCountersPtr, i64>> Tags;

    void ClearOutdated(i64 lastIteration);
    bool IsEmpty() const;
};

class TCounterWriter final
    : public ISensorWriter
{
public:
    TCounterWriter(
        IRegistryPtr registry,
        TProducerCountersPtr counters,
        i64 iteration);

    void PushTag(TTag tag) override;
    void PopTag() override;
    void AddGauge(const std::string& name, double value) override;
    void AddCounter(const std::string& name, i64 value) override;

private:
    IRegistryPtr Registry_;
    std::vector<TProducerCountersPtr> Counters_;
    i64 Iteration_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProducerState)

struct TProducerState final
{
    TProducerState(
        const std::string& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        TWeakPtr<ISensorProducer> producer)
        : Producer(std::move(producer))
        , Counters(New<TProducerCounters>())
    {
        Counters->Prefix = prefix;
        Counters->ProducerTags = tags;
        Counters->Options = options;
    }

    const TWeakPtr<ISensorProducer> Producer;
    TWeakPtr<TSensorBuffer> LastBuffer;

    i64 LastUpdateIteration = 0;
    TProducerCountersPtr Counters;
};

////////////////////////////////////////////////////////////////////////////////

class TProducerSet
{
public:
    void AddProducer(TProducerStatePtr state);

    void Collect(IRegistryPtr profiler, IInvokerPtr invoker);

    void Profile(const TWeakProfiler& profiler);

    void SetCollectionBatchSize(int batchSize);

private:
    THashSet<TProducerStatePtr> Producers_;

    TWeakProfiler SelfProfiler_;
    TEventTimer ProducerCollectDuration_;

    int BatchSize_ = DefaultProducerCollectionBatchSize;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
