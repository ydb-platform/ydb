#include "event_counter_profiler.h"

#include "event_counter.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "Profiling");

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TPerfEventCounterDynamicConfig)

struct TPerfEventCounterDynamicConfig
    : public TYsonStruct
{
    std::optional<THashMap<std::string, EPerfEventType>> Events;

    REGISTER_YSON_STRUCT(TPerfEventCounterDynamicConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("events", &TThis::Events)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TPerfEventCounterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TPerfEventCounterConfig)

struct TPerfEventCounterConfig
    : public TYsonStruct
{
    THashMap<std::string, EPerfEventType> Events;

    TPerfEventCounterConfigPtr ApplyDynamic(const TPerfEventCounterDynamicConfigPtr& dynamicConfig) const
    {
        auto config = CloneYsonStruct(MakeStrong(this));

        UpdateYsonStructField(config->Events, dynamicConfig->Events);

        return config;
    }

    REGISTER_YSON_STRUCT(TPerfEventCounterConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("events", &TThis::Events)
            .Default(THashMap<std::string, EPerfEventType>({
                {"/cpu/cycles", EPerfEventType::CpuCycles},
                {"/cpu/instructions", EPerfEventType::Instructions},
                {"/cpu/branch_instructions", EPerfEventType::BranchInstructions},
                {"/cpu/branch_misses", EPerfEventType::BranchMisses},
                {"/cpu/context_switches", EPerfEventType::ContextSwitches},
                {"/memory/page_faults", EPerfEventType::PageFaults},
                {"/memory/minor_page_faults", EPerfEventType::MinorPageFaults},
                {"/memory/major_page_faults", EPerfEventType::MajorPageFaults},
            }))
            .ResetOnLoad();
    }
};

DEFINE_REFCOUNTED_TYPE(TPerfEventCounterConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPerfEventCounterProfilerImpl)

class TPerfEventCounterProfilerImpl
    : public ISensorProducer
{
public:
    void Enable()
    {
        std::call_once(EnabledFlag_, [&] {
            TProfiler("")
                .WithProducerRemoveSupport()
                .AddProducer("/perf", this);
        });
    }

    void Configure(const TPerfEventCounterConfigPtr& config)
    {
        Config_ = config;
    }

    static TPerfEventCounterProfilerImplPtr Get()
    {
        return LeakyRefCountedSingleton<TPerfEventCounterProfilerImpl>();
    }

private:
    TEnumIndexedArray<EPerfEventType, NThreading::TAtomicObject<std::unique_ptr<IPerfEventCounter>>> TypeToCounter_;

    TAtomicIntrusivePtr<TPerfEventCounterConfig> Config_{New<TPerfEventCounterConfig>()};

    std::once_flag EnabledFlag_;

    IPerfEventCounter* GetCounter(EPerfEventType type)
    {
        return TypeToCounter_[type].Transform([&] (auto& counter) {
            if (!counter) {
                counter = CreatePerfEventCounter(type);
            }
            return counter.get();
        });
    }

    void CollectSensors(ISensorWriter* writer) final
    {
        YT_TLOG_DEBUG("Started collecting perf event sensors");

        auto config = Config_.Acquire();

        for (const auto& [sensorName, eventType] : config->Events) {
            try {
                auto value = GetCounter(eventType)->Read();
                writer->AddCounter(sensorName, value);
            } catch (const std::exception& ex) {
                YT_TLOG_DEBUG("Failed to collect perf event sensor")
                    .With("Sensor", sensorName)
                    .With(ex);
            }
        }

        YT_TLOG_DEBUG("Finished collecting perf event sensors");
    }
};

DEFINE_REFCOUNTED_TYPE(TPerfEventCounterProfilerImpl)

////////////////////////////////////////////////////////////////////////////////

void EnablePerfEventCounterProfiling()
{
    TPerfEventCounterProfilerImpl::Get()->Enable();
}

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TPerfEventCounterConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TPerfEventCounterDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TPerfEventCounterConfigPtr& config)
{
    TPerfEventCounterProfilerImpl::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TPerfEventCounterConfigPtr& config,
    const TPerfEventCounterDynamicConfigPtr& dynamicConfig)
{
    ConfigureSingleton(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "perf_event_counter",
    TPerfEventCounterConfig,
    TPerfEventCounterDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
