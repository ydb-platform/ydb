#include "registry.h"

#include "sensor.h"

#include <yt/yt/library/profiling/percpu.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSolomonRegistry::TSolomonRegistry() = default;

template <class TBase, class TSimple, class TPerCpu, class TFn>
TIntrusivePtr<TBase> SelectImpl(bool hot, const TFn& fn)
{
    if (!hot) {
        auto counter = New<TSimple>();
        fn(counter);
        return counter;
    } else {
        auto counter = New<TPerCpu>();
        fn(counter);
        return counter;
    }
}

ICounterPtr TSolomonRegistry::RegisterCounter(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ICounter, TSimpleCounter, TPerCpuCounter>(options.Hot, [&, this] (const auto& counter) {
        DoRegister([this, name, tags, options = std::move(options), counter] {
            auto reader = [ptr = counter.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddCounter(New<TCounterState>(counter, reader, EncodeTagSet(tags)));
        });
    });
}

ITimeCounterPtr TSolomonRegistry::RegisterTimeCounter(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ITimeCounter, TSimpleTimeCounter, TPerCpuTimeCounter>(
        options.Hot,
        [&, this] (const auto& counter) {
            DoRegister([this, name, tags, options = std::move(options), counter] {
                auto set = FindSet(name, options);
                set->AddTimeCounter(New<TTimeCounterState>(counter, EncodeTagSet(tags)));
            });
        });
}

IGaugePtr TSolomonRegistry::RegisterGauge(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<IGauge, TSimpleGauge, TPerCpuGauge>(options.Hot, [&, this] (const auto& gauge) {
        if (options.DisableDefault) {
            gauge->Update(std::numeric_limits<double>::quiet_NaN());
        }

        DoRegister([this, name, tags, options = std::move(options), gauge] {
            auto reader = [ptr = gauge.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddGauge(New<TGaugeState>(gauge, reader, EncodeTagSet(tags)));
        });
    });
}

ITimeGaugePtr TSolomonRegistry::RegisterTimeGauge(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleTimeGauge>();

    DoRegister([this, name, tags, options = std::move(options), gauge] {
        auto reader = [ptr = gauge.Get()] {
            return ptr->GetValue().SecondsFloat();
        };

        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(gauge, reader, EncodeTagSet(tags)));
    });

    return gauge;
}

ISummaryPtr TSolomonRegistry::RegisterSummary(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ISummary, TSimpleSummary<double>, TPerCpuSummary<double>>(options.Hot, [&, this] (const auto& summary) {
        DoRegister([this, name, tags, options = std::move(options), summary] {
            auto set = FindSet(name, options);
            set->AddSummary(New<TSummaryState>(summary, EncodeTagSet(tags)));
        });
    });
}

IGaugePtr TSolomonRegistry::RegisterGaugeSummary(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleGauge>();
    DoRegister([this, name, tags, options = std::move(options), gauge] {
        auto set = FindSet(name, options);
        set->AddSummary(New<TSummaryState>(gauge, EncodeTagSet(tags)));
    });

    return gauge;
}

ITimeGaugePtr TSolomonRegistry::RegisterTimeGaugeSummary(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleTimeGauge>();
    DoRegister([this, name, tags, options = std::move(options), gauge] {
        auto set = FindSet(name, options);
        set->AddTimerSummary(New<TTimerSummaryState>(gauge, EncodeTagSet(tags)));
    });

    return gauge;
}

ITimerPtr TSolomonRegistry::RegisterTimerSummary(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return SelectImpl<ITimer, TSimpleSummary<TDuration>, TPerCpuSummary<TDuration>>(
        options.Hot,
        [&, this] (const auto& timer) {
            DoRegister([this, name, tags, options = std::move(options), timer] {
                auto set = FindSet(name, options);
                set->AddTimerSummary(New<TTimerSummaryState>(timer, EncodeTagSet(tags)));
            });
        });
}

ITimerPtr TSolomonRegistry::RegisterTimeHistogram(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name, tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddTimeHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

IHistogramPtr TSolomonRegistry::RegisterGaugeHistogram(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name, tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddGaugeHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

IHistogramPtr TSolomonRegistry::RegisterRateHistogram(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name, tags, options = std::move(options), hist] {
        auto set = FindSet(name, options);
        set->AddRateHistogram(New<THistogramState>(hist, EncodeTagSet(tags)));
    });
    return hist;
}

void TSolomonRegistry::RegisterFuncCounter(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<i64()> reader)
{
    DoRegister([this, name, tags, options = std::move(options), weakOwner = MakeWeak(owner), reader] {
        auto set = FindSet(name, options);
        set->AddCounter(New<TCounterState>(std::move(weakOwner), reader, EncodeTagSet(tags)));
    });
}

void TSolomonRegistry::RegisterFuncGauge(
    const std::string& name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<double()> reader)
{
    DoRegister([this, name, tags, options = std::move(options), reader, weakOwner = MakeWeak(owner)] {
        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(std::move(weakOwner), reader, EncodeTagSet(tags)));
    });
}

void TSolomonRegistry::RegisterProducer(
    const std::string& prefix,
    const TTagSet& tags,
    TSensorOptions options,
    const ISensorProducerPtr& producer)
{
    DoRegister([this, prefix, tags, options = std::move(options), weakProducer = MakeWeak(producer)] {
        Producers_.AddProducer(New<TProducerState>(prefix, tags, options, std::move(weakProducer)));
    });
}

void TSolomonRegistry::RenameDynamicTag(
    const TDynamicTagPtr& tag,
    const std::string& name,
    const std::string& value)
{
    DoRegister([this, tag, name, value] {
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

void TSolomonRegistry::SetGridFactor(std::function<int(const std::string&)> gridFactor)
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

    // Use blocking Get(), because we want to lock current thread while data structure is updating.
    for (const auto& future : offloadFutures) {
        future.Get();
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
    const std::string& name,
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

TSensorSet* TSolomonRegistry::FindSet(const std::string& name, const TSensorOptions& options)
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
