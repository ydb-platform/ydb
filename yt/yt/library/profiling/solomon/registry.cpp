#include "registry.h"

#include "config.h"

#include <yt/yt/library/profiling/per_cpu_sensor_impl.h>
#include <yt/yt/library/profiling/simple_sensor_impl.h>

#ifdef __linux__
#include <yt/yt/library/profiling/rseq_sensor_impl.h>

#include <library/cpp/yt/rseq/rseq.h>
#endif

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// The rseq-backed hot sensors are Linux-only (rseq_sensor_impl.h); off Linux the
// alias is a dummy (never instantiated -- the rseq branch is compiled out below).
#ifdef __linux__
using TPortableRseqCounter = TRseqCounter;
using TPortableRseqTimeCounter = TRseqTimeCounter;
using TPortableRseqGauge = TRseqGauge;
#else
using TPortableRseqCounter = TPerCpuCounter;
using TPortableRseqTimeCounter = TPerCpuTimeCounter;
using TPortableRseqGauge = TPerCpuGauge;
#endif

////////////////////////////////////////////////////////////////////////////////

TSolomonRegistry::TSolomonRegistry() = default;

void TSolomonRegistry::Configure(const TSolomonRegistryConfigPtr& config)
{
    // Use the rseq fast path only when it is both requested and supported in this process: a
    // runtime probe confirms the cached thread-pointer offset is stable across threads -- not
    // so when our __rseq_abi lands in a dlopen'd module's dynamically allocated TLS.
    bool enabled = config->EnableRseq;
#ifdef __linux__
    enabled = enabled && NRseq::IsPerCpuFastPathSupported();
#else
    enabled = false;
#endif
    RseqEnabled_.store(enabled, std::memory_order::relaxed);
}

bool TSolomonRegistry::IsRseqEnabled() const
{
    return RseqEnabled_.load(std::memory_order::relaxed);
}

template <class TBase, class TSimple, class TPerCpu, class TFn>
TIntrusivePtr<TBase> SelectImpl(bool hot, const TFn& fn)
{
    auto sensor = [&] () -> TIntrusivePtr<TBase> {
        if (!hot) {
            return New<TSimple>();
        }
        return New<TPerCpu>();
    }();
    fn(sensor);
    return sensor;
}

// Like SelectImpl, but the hot sensor is chosen at construction time between the rseq fast
// path (when enabled and available) and the atomic sharded fallback. The choice is made
// once per sensor here, so the hot Increment/Update path carries no dispatch.
template <class TBase, class TSimple, class TSharded, class TRseq, class TFn>
TIntrusivePtr<TBase> SelectFastImpl(bool hot, bool rseqEnabled, const TFn& fn)
{
    auto sensor = [&] () -> TIntrusivePtr<TBase> {
        if (!hot) {
            return New<TSimple>();
        }
#ifdef __linux__
        if (rseqEnabled) {
            // The rseq sensors fold their per-CPU shard array into the object allocation
            // (NewWithExtraSpace), so they are built via a factory rather than New.
            return TRseq::Create();
        }
#else
        Y_UNUSED(rseqEnabled);
#endif
        return New<TSharded>();
    }();
    fn(sensor);
    return sensor;
}

ICounterPtr TSolomonRegistry::RegisterCounter(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectFastImpl<ICounter, TSimpleCounter, TPerCpuCounter, TPortableRseqCounter>(options.Hot, IsRseqEnabled(), [&, this] (const auto& counter) {
        DoRegister([this, name = std::string(name), tags, options = std::move(options), counter] {
            auto reader = [ptr = counter.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddCounter(New<TCounterState>(counter, reader, EncodeTagSet(tags)));
        });
    });
}

ITimeCounterPtr TSolomonRegistry::RegisterTimeCounter(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectFastImpl<ITimeCounter, TSimpleTimeCounter, TPerCpuTimeCounter, TPortableRseqTimeCounter>(
        options.Hot,
        IsRseqEnabled(),
        [&, this] (const auto& counter) {
            DoRegister([this, name = std::string(name), tags, options = std::move(options), counter] {
                auto set = FindSet(name, options);
                set->AddTimeCounter(New<TTimeCounterState>(counter, EncodeTagSet(tags)));
            });
        });
}

IGaugePtr TSolomonRegistry::RegisterGauge(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectFastImpl<IGauge, TSimpleGauge, TPerCpuGauge, TPortableRseqGauge>(options.Hot, IsRseqEnabled(), [&, this] (const auto& gauge) {
        if (options.DisableDefault) {
            gauge->Update(std::numeric_limits<double>::quiet_NaN());
        }

        DoRegister([this, name = std::string(name), tags, options = std::move(options), gauge] {
            auto reader = [ptr = gauge.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddGauge(New<TGaugeState>(gauge, reader, EncodeTagSet(tags)));
        });
    });
}

ITimeGaugePtr TSolomonRegistry::RegisterTimeGauge(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleTimeGauge>();

    DoRegister([this, name = std::string(name), tags, options = std::move(options), gauge] {
        auto reader = [ptr = gauge.Get()] {
            return ptr->GetValue().SecondsFloat();
        };

        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(gauge, reader, EncodeTagSet(tags)));
    });

    return gauge;
}

ISummaryPtr TSolomonRegistry::RegisterSummary(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ISummary, TSimpleSummary<double>, TPerCpuSummary<double>>(options.Hot, [&, this] (const auto& summary) {
        DoRegister([this, name = std::string(name), tags, options = std::move(options), summary] {
            auto set = FindSet(name, options);
            set->AddSummary(New<TSummaryState>(summary, EncodeTagSet(tags)));
        });
    });
}

IGaugePtr TSolomonRegistry::RegisterGaugeSummary(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleGauge>();
    DoRegister([this, name = std::string(name), tags, options = std::move(options), gauge] {
        auto set = FindSet(name, options);
        set->AddSummary(New<TSummaryState>(gauge, EncodeTagSet(tags)));
    });

    return gauge;
}

ITimeGaugePtr TSolomonRegistry::RegisterTimeGaugeSummary(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleTimeGauge>();
    DoRegister([this, name = std::string(name), tags, options = std::move(options), gauge] {
        auto set = FindSet(name, options);
        set->AddTimerSummary(New<TTimerSummaryState>(gauge, EncodeTagSet(tags)));
    });

    return gauge;
}

ITimerPtr TSolomonRegistry::RegisterTimerSummary(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ITimer, TSimpleSummary<TDuration>, TPerCpuSummary<TDuration>>(
        options.Hot,
        [&, this] (const auto& timer) {
            DoRegister([this, name = std::string(name), tags, options = std::move(options), timer] {
                auto set = FindSet(name, options);
                set->AddTimerSummary(New<TTimerSummaryState>(timer, EncodeTagSet(tags)));
            });
        });
}

ITimerPtr TSolomonRegistry::RegisterTimeHistogram(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name = std::string(name), tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddTimeHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

IHistogramPtr TSolomonRegistry::RegisterGaugeHistogram(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name = std::string(name), tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddGaugeHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

IHistogramPtr TSolomonRegistry::RegisterRateHistogram(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name = std::string(name), tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddRateHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

void TSolomonRegistry::RegisterFuncCounter(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<i64()> reader)
{
    DoRegister([this, name = std::string(name), tags, options = std::move(options), weakOwner = MakeWeak(owner), reader] {
        auto set = FindSet(name, options);
        set->AddCounter(New<TCounterState>(std::move(weakOwner), reader, EncodeTagSet(tags)));
    });
}

void TSolomonRegistry::RegisterFuncGauge(
    TStringBuf name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<double()> reader)
{
    DoRegister([this, name = std::string(name), tags, options = std::move(options), reader, weakOwner = MakeWeak(owner)] {
        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(std::move(weakOwner), reader, EncodeTagSet(tags)));
    });
}

void TSolomonRegistry::RegisterProducer(
    TStringBuf prefix,
    const TTagSet& tags,
    TSensorOptions options,
    const ISensorProducerPtr& producer)
{
    DoRegister([this, prefix = std::string(prefix), tags, options = std::move(options), weakProducer = MakeWeak(producer)] {
        Producers_.AddProducer(New<TProducerState>(prefix, tags, options, std::move(weakProducer)));
    });
}

void TSolomonRegistry::RenameDynamicTag(
    const TDynamicTagPtr& tag,
    TStringBuf name,
    TStringBuf value)
{
    DoRegister([this, tag, name = std::string(name), value = std::string(value)] {
        auto tagId = TagRegistry_.Encode(TTag{name, value});

        for (auto& [name, sensorSet] : Sensors_) {
            sensorSet.RenameDynamicTag(tag, tagId);
        }
    });
}

TSolomonRegistryPtr TSolomonRegistry::Get()
{
    return LeakyRefCountedSingleton<TSolomonRegistry>();
}

i64 TSolomonRegistry::GetNextIteration() const
{
    return Iteration_;
}

void TSolomonRegistry::SetGridFactor(std::function<int(TStringBuf)> gridFactor)
{
    GridFactor_ = gridFactor;
}

void TSolomonRegistry::SetWindowSize(int windowSize)
{
    if (WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is already set");
    }

    WindowSize_ = windowSize;
}

void TSolomonRegistry::SetProducerCollectionBatchSize(int batchSize)
{
    Producers_.SetCollectionBatchSize(batchSize);
}

void TSolomonRegistry::SetLabelSanitizationPolicy(ELabelSanitizationPolicy LabelSanitizationPolicy)
{
    TagRegistry_.SetLabelSanitizationPolicy(LabelSanitizationPolicy);
}

int TSolomonRegistry::GetWindowSize() const
{
    if (!WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is not configured");
    }

    return *WindowSize_;
}

int TSolomonRegistry::IndexOf(i64 iteration) const
{
    return iteration % GetWindowSize();
}

void TSolomonRegistry::Profile(const TWeakProfiler& profiler)
{
    SelfProfiler_ = profiler.WithPrefix("/solomon_registry");

    Producers_.Profile(SelfProfiler_);

    SensorCollectDuration_ = SelfProfiler_.Timer("/sensor_collect_duration");
    ReadDuration_ = SelfProfiler_.Timer("/read_duration");
    SensorCount_ = SelfProfiler_.Gauge("/sensor_count");
    ProjectionCount_ = SelfProfiler_.Gauge("/projection_count");
    TagCount_ = SelfProfiler_.Gauge("/tag_count");
    RegistrationCount_ = SelfProfiler_.Counter("/registration_count");
}

const TWeakProfiler& TSolomonRegistry::GetSelfProfiler() const
{
    return SelfProfiler_;
}

template <class TFn>
void TSolomonRegistry::DoRegister(TFn fn)
{
    if (Disabled_) {
        return;
    }

    RegistrationQueue_.Enqueue(std::move(fn));
}

void TSolomonRegistry::SetDynamicTags(std::vector<TTag> dynamicTags)
{
    auto guard = Guard(DynamicTagsLock_);
    std::swap(DynamicTags_, dynamicTags);
}

std::vector<TTag> TSolomonRegistry::GetDynamicTags() const
{
    auto guard = Guard(DynamicTagsLock_);
    return DynamicTags_;
}

void TSolomonRegistry::Disable()
{
    Disabled_ = true;
    RegistrationQueue_.DequeueAll();
}

void TSolomonRegistry::ProcessRegistrations()
{
    GetWindowSize();

    RegistrationQueue_.DequeueAll(true, [this] (const std::function<void()>& fn) {
        RegistrationCount_.Increment();

        fn();

        TagCount_.Update(TagRegistry_.GetSize());
    });
}

void TSolomonRegistry::Collect(IInvokerPtr offloadInvoker)
{
    Producers_.Collect(MakeStrong(this), offloadInvoker);
    ProcessRegistrations();

    auto projectionCount = std::make_shared<std::atomic<int>>(0);

    std::vector<TFuture<void>> offloadFutures;
    for (auto& [name, set] : Sensors_) {
        if (Iteration_ % set.GetGridFactor() != 0) {
            continue;
        }

        auto future = BIND([sensorSet = &set, projectionCount, collectDuration = SensorCollectDuration_] {
            auto start = TInstant::Now();
            *projectionCount += sensorSet->Collect();
            collectDuration.Record(TInstant::Now() - start);
        })
            .AsyncVia(offloadInvoker)
            .Run();

        offloadFutures.push_back(future);
    }

    // Use BlockingGet(), because we want to lock current thread while data structure is updating.
    for (const auto& future : offloadFutures) {
        future.BlockingGet();
    }

    ProjectionCount_.Update(*projectionCount);
    Iteration_++;
}

void TSolomonRegistry::ReadSensors(
    const TReadOptions& options,
    ::NMonitoring::IMetricConsumer* consumer) const
{
    auto readOptions = options;
    {
        auto guard = Guard(DynamicTagsLock_);
        readOptions.InstanceTags.insert(
            readOptions.InstanceTags.end(),
            DynamicTags_.begin(),
            DynamicTags_.end());
    }

    TTagWriter tagWriter(TagRegistry_, consumer);
    for (const auto& [name, set] : Sensors_) {
        if (readOptions.SensorFilter && !readOptions.SensorFilter(name)) {
            continue;
        }

        auto start = TInstant::Now();
        set.ReadSensors(name, readOptions, &tagWriter, consumer);
        ReadDuration_.Record(TInstant::Now() - start);
    }
}

void TSolomonRegistry::ReadRecentSensorValues(
    TStringBuf name,
    const TTagList& tags,
    const TReadOptions& options,
    TFluentAny fluent) const
{
    if (Iteration_ == 0) {
        THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
            "No sensors have been collected so far");
    }

    auto it = Sensors_.find(name);
    if (it == Sensors_.end()) {
        THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
            "No such sensor")
                << TErrorAttribute("name", name);
    }

    const auto& sensorSet = it->second;
    auto index = IndexOf((Iteration_ - 1) / sensorSet.GetGridFactor());

    auto readOptions = options;
    {
        auto guard = Guard(DynamicTagsLock_);
        readOptions.InstanceTags.insert(
            readOptions.InstanceTags.end(),
            DynamicTags_.begin(),
            DynamicTags_.end());
    }

    auto encodedTagIds = TagRegistry_.TryEncode(tags);
    std::optional<TTagIdList> tagIds = TTagIdList{};
    for (int i = 0; i < std::ssize(encodedTagIds); ++i) {
        if (encodedTagIds[i]) {
            tagIds->push_back(*encodedTagIds[i]);
            continue;
        }

        auto tagIt = std::find(
            readOptions.InstanceTags.begin(),
            readOptions.InstanceTags.end(),
            tags[i]);
        if (tagIt == readOptions.InstanceTags.end()) {
            tagIds.reset();
            break;
        }
    }

    int valuesRead = 0;
    if (tagIds) {
        std::sort(tagIds->begin(), tagIds->end());
        valuesRead = sensorSet.ReadSensorValues(*tagIds, index, readOptions, TagRegistry_, fluent);
    }

    if (!readOptions.ReadAllProjections) {
        if (valuesRead == 0) {
            THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                "Projection not found for sensor")
                    << TErrorAttribute("name", name)
                    << TErrorAttribute("tags", tags);
        } else if (valuesRead > 1) {
            THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                "More than one projection found for sensor")
                    << TErrorAttribute("name", name)
                    << TErrorAttribute("tags", tags)
                    << TErrorAttribute("values_read", valuesRead);
        }
    } else if (valuesRead == 0) {
        fluent.BeginList().EndList();
    }
}

std::vector<TSensorInfo> TSolomonRegistry::ListSensors() const
{
    std::vector<TSensorInfo> list;
    for (const auto& [name, set] : Sensors_) {
        list.push_back(TSensorInfo{name, set.GetObjectCount(), set.GetCubeSize(), set.GetError()});
    }
    return list;
}

const TTagRegistry& TSolomonRegistry::GetTagRegistry() const
{
    return TagRegistry_;
}

TSensorSet* TSolomonRegistry::FindSet(TStringBuf name, const TSensorOptions& options)
{
    if (auto it = Sensors_.find(name); it != Sensors_.end()) {
        it->second.ValidateOptions(options);
        return &it->second;
    } else {
        int gridFactor = 1;
        if (GridFactor_) {
            gridFactor = GridFactor_(name);
        }

        it = Sensors_.emplace(name, TSensorSet{options, Iteration_ / gridFactor, GetWindowSize(), gridFactor}).first;
        it->second.Profile(SelfProfiler_.WithTag("metric_name", name));
        SensorCount_.Update(Sensors_.size());
        return &it->second;
    }
}

NProto::TSensorDump TSolomonRegistry::DumpSensors(TTagSet customTagSet)
{
    TTagIdList extraTags;
    {
        auto guard = Guard(DynamicTagsLock_);

        for (const auto& [key, value] : DynamicTags_) {
            extraTags.push_back(TagRegistry_.Encode(std::pair(key, value)));
        }
    }

    std::vector<TTagIdList> customProjections;
    EncodeTagSet(customTagSet).Range([&] (auto tagIds) {
        for (auto tag : extraTags) {
            tagIds.push_back(tag);
        }
        customProjections.push_back(std::move(tagIds));
    });

    NProto::TSensorDump dump;
    TagRegistry_.DumpTags(&dump);

    for (const auto& [name, set] : Sensors_) {
        if (!set.GetError().IsOK()) {
            continue;
        }

        auto* cube = dump.add_cubes();
        cube->set_name(ToProto(name));

        set.DumpCube(cube, customProjections);
    }

    return dump;
}

NProto::TSensorDump TSolomonRegistry::DumpSensors()
{
    return DumpSensors(TTagSet{});
}

TTagIdSet TSolomonRegistry::EncodeTagSet(const TTagSet& tags)
{
    auto tagSet = tags;
    for (const auto& [dynamicTag, _] : tagSet.DynamicTags()) {
        tagSet.ApplyDynamicTag(dynamicTag);
    }
    return TTagIdSet(tagSet, TagRegistry_.Encode(tagSet));
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_
// This function overrides weak symbol defined in impl.cpp
IRegistryPtr GetGlobalRegistry()
{
    return TSolomonRegistry::Get();
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
