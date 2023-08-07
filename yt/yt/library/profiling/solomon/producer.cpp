#include "producer.h"
#include "private.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TProducerState)
DEFINE_REFCOUNTED_TYPE(TProducerCounters)

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

void TProducerCounters::ClearOutdated(i64 lastIteration)
{
    std::vector<TString> countersToRemove;
    for (const auto& [name, counter] : Counters) {
        if (std::get<1>(counter) != lastIteration) {
            countersToRemove.push_back(name);
        }
    }
    for (const auto& name : countersToRemove) {
        Counters.erase(name);
    }

    std::vector<TString> gaugesToRemove;
    for (const auto& [name, gauge] : Gauges) {
        if (gauge.second != lastIteration) {
            gaugesToRemove.push_back(name);
        }
    }
    for (const auto& name : gaugesToRemove) {
        Gauges.erase(name);
    }

    std::vector<TTag> tagsToRemove;
    for (auto& [tag, set] : Tags) {
        set.first->ClearOutdated(lastIteration);

        if (set.second != lastIteration || set.first->IsEmpty()) {
            tagsToRemove.push_back(tag);
        }
    }
    for (const auto& tag : tagsToRemove) {
        Tags.erase(tag);
    }
}

bool TProducerCounters::IsEmpty() const
{
    return Counters.empty() && Gauges.empty() && Tags.empty();
}

////////////////////////////////////////////////////////////////////////////////

TCounterWriter::TCounterWriter(
    IRegistryImplPtr registry,
    TProducerCountersPtr counters,
    i64 iteration)
    : Registry_(std::move(registry))
    , Counters_{{std::move(counters)}}
    , Iteration_{iteration}
{ }

void TCounterWriter::PushTag(TTag tag)
{
    auto& [nested, iteration] = Counters_.back()->Tags[tag];
    iteration = Iteration_;

    if (!nested) {
        nested = New<TProducerCounters>();
        nested->Prefix = Counters_.back()->Prefix;
        nested->ProducerTags = Counters_.back()->ProducerTags;
        nested->Options = Counters_.back()->Options;
        nested->ProducerTags.AddTag(std::move(tag));
    }

    Counters_.push_back(nested);
}

void TCounterWriter::PopTag()
{
    Counters_.pop_back();
}

void TCounterWriter::AddGauge(const TString& name, double value)
{
    auto& [gauge, iteration] = Counters_.back()->Gauges[name];
    iteration = Iteration_;

    if (!gauge) {
        TProfiler profiler{
            Counters_.back()->Prefix,
            "",
            Counters_.back()->ProducerTags,
            Registry_,
            Counters_.back()->Options,
        };

        gauge = profiler.Gauge(name);
    }

    gauge.Update(value);
}

void TCounterWriter::AddCounter(const TString& name, i64 value)
{
    auto& [counter, iteration, lastValue] = Counters_.back()->Counters[name];
    iteration = Iteration_;

    if (!counter) {
        TProfiler profiler{
            Counters_.back()->Prefix,
            "",
            Counters_.back()->ProducerTags,
            Registry_,
            Counters_.back()->Options,
        };

        counter = profiler.Counter(name);
    }

    if (value >= lastValue) {
        auto delta = value - lastValue;
        counter.Increment(delta);
        lastValue = value;
    } else {
        // Some producers use counter incorrectly.
        lastValue = value;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TProducerSet::AddProducer(TProducerStatePtr state)
{
    Producers_.insert(std::move(state));
}

void TProducerSet::Collect(IRegistryImplPtr profiler, IInvokerPtr invoker)
{
    std::vector<TFuture<void>> offloadFutures;
    std::deque<TProducerStatePtr> toRemove;
    for (const auto& producer : Producers_) {
        auto owner = producer->Producer.Lock();
        if (!owner) {
            toRemove.push_back(producer);
            continue;
        }

        auto future = BIND([profiler, owner, producer, collectDuration = ProducerCollectDuration_] () {
            auto startTime = TInstant::Now();
            auto reportTime = Finally([&] {
                collectDuration.Record(TInstant::Now() - startTime);
            });

            try {
                auto buffer = owner->GetBuffer();
                if (buffer) {
                    auto lastBuffer = producer->LastBuffer.Lock();
                    if (lastBuffer == buffer) {
                        return;
                    }

                    TCounterWriter writer(profiler, producer->Counters, ++producer->LastUpdateIteration);
                    buffer->WriteTo(&writer);
                    producer->LastBuffer = buffer;
                    if (producer->Counters->Options.ProducerRemoveSupport) {
                        producer->Counters->ClearOutdated(producer->LastUpdateIteration);
                    }
                } else {
                    producer->Counters->Counters.clear();
                    producer->Counters->Gauges.clear();
                    producer->Counters->Tags.clear();
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Producer read failed");
                return;
            }
        })
            .AsyncVia(invoker)
            .Run();

        offloadFutures.push_back(future);
    }

    // Use blocking Get(), because we want to lock current thread while data structure is updating.
    for (const auto& future : offloadFutures) {
        future.Get();
    }

    for (const auto& producer : toRemove) {
        Producers_.erase(producer);
    }
}

void TProducerSet::Profile(const TProfiler& profiler)
{
    SelfProfiler_ = profiler;
    ProducerCollectDuration_ = profiler.Timer("/producer_collect_duration");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
