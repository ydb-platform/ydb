#include <ydb/core/base/counters.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NMemory {

namespace {

using namespace NActors;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

struct TMemoryConsumerSnapshot {
    EConsumerKind Kind;
    ui64 Consumption;
};

class TMemoryConsumer : public IMemoryConsumer {
public:
    TMemoryConsumer(EConsumerKind kind)
        : Kind(kind) 
    {
    }

    EConsumerKind GetKind() const {
        return Kind;
    }

    ui64 GetLimit() const override {
        TGuard<TMutex> guard(Mutex);
        return Limit;
    }

    void SetLimit(ui64 value) {
        TGuard<TMutex> guard(Mutex);
        Limit = value;
    }

    ui64 GetConsumption() const {
        TGuard<TMutex> guard(Mutex);
        return Consumption;
    }

    void SetConsumption(ui64 value) override {
        TGuard<TMutex> guard(Mutex);
        Consumption = value;
    }

    TMemoryConsumerSnapshot GetSnapshot() {
        TGuard<TMutex> guard(Mutex);
        return {Kind, Consumption};
    }

private:
    TMutex Mutex;
    const EConsumerKind Kind;
    ui64 Consumption = 0;
    ui64 Limit = 0;
};

class TMemoryConsumers : public IMemoryConsumers {
public:
    TIntrusivePtr<IMemoryConsumer> Register(EConsumerKind consumer) {
        TGuard<TMutex> guard(Mutex);

        auto inserted = Consumers.emplace(consumer, MakeIntrusive<TMemoryConsumer>(consumer));

        Y_ABORT_UNLESS(inserted.second, "Consumers should be unique");

        return inserted.first->second;
    }

    TVector<TMemoryConsumerSnapshot> GetConsumersSnapshot() const {
        TGuard<TMutex> guard(Mutex);
        
        TVector<TMemoryConsumerSnapshot> result(::Reserve(Consumers.size()));

        // Note: don't care about destroyed consumers as they are possible only during process stopping

        for (const auto& consumer : Consumers) {
            result.push_back(consumer.second->GetSnapshot());
        }

        return result;
    }

private:
    TMutex Mutex;
    TMap<EConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
};

struct TConsumerCounters {
    TCounterPtr Consumption;
    TCounterPtr LimitBytes;
    TCounterPtr LimitMinPercent;
    TCounterPtr LimitMaxPercent;
    TCounterPtr LimitMinBytes;
    TCounterPtr LimitMaxBytes;
};

struct TConsumerConfig {
    std::optional<float> MinPercent;
    std::optional<ui64> MinBytes;
    std::optional<float> MaxPercent;
    std::optional<ui64> MaxBytes;
    bool CanZeroLimit = false;
};

struct TConsumerLimitBounds {
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;
};

class TMemoryController : public TActorBootstrapped<TMemoryController> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MEMORY_CONTROLLER;
    }

    TMemoryController(TDuration interval, TIntrusivePtr<IMemoryConsumers> consumers, NKikimrConfig::TMemoryControllerConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : Interval(interval)
        , Consumers_(std::move(consumers))
        , Consumers(dynamic_cast<TMemoryConsumers&>(*Consumers_))
        , Config(config)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), 
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                    NKikimrConsole::TConfigItem::MemoryControllerConfigItem}));

        Become(&TThis::StateWork);
        ctx.Schedule(Interval, new TEvents::TEvWakeup());

        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Bootstrapped");
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
        }
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        Config.Swap(ev->Get()->Record.MutableConfig()->MutableMemoryControllerConfig());
        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Config updated " << Config.DebugString());
    }

    void HandleWakeup(const TActorContext& ctx) noexcept {
        std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();
        auto allocatedMemory = TAllocState::GetAllocatedMemoryEstimate();

        ui64 hardLimitBytes = GetHardLimitBytes(memoryUsage);
        ui64 softLimitBytes = GetSoftLimitBytes(hardLimitBytes);
        ui64 targetUtilizationBytes = GetTargetUtilizationBytes(hardLimitBytes);

        auto consumers = Consumers.GetConsumersSnapshot();
        TVector<TConsumerLimitBounds> consumersLimitBounds;
        TConsumerLimitBounds consumersLimitBoundsTotal;
        ui64 consumersConsumption = 0;
        for (const auto& consumer : consumers) {
            auto bounds = GetConsumerLimitBounds(consumer.Kind, softLimitBytes);
            consumersLimitBounds.push_back(bounds);
            consumersLimitBoundsTotal.MinBytes += bounds.MinBytes;
            consumersLimitBoundsTotal.MaxBytes += bounds.MaxBytes;
            consumersConsumption += consumer.Consumption;
        }
        // allocatedMemory = externalConsumption + consumersConsumption
        ui64 externalConsumption = allocatedMemory - Min(allocatedMemory, consumersConsumption);
        // targetConsumersConsumption + externalConsumption = targetUtilizationBytes
        ui64 targetConsumersConsumption = targetUtilizationBytes - Min(targetUtilizationBytes, externalConsumption);
        targetConsumersConsumption = Max(targetConsumersConsumption, consumersLimitBoundsTotal.MinBytes);
        targetConsumersConsumption = Min(targetConsumersConsumption, consumersLimitBoundsTotal.MaxBytes);

        double coefficient = static_cast<double>(targetConsumersConsumption - consumersLimitBoundsTotal.MinBytes) 
            / Max<ui64>(1, consumersLimitBoundsTotal.MaxBytes - consumersLimitBoundsTotal.MinBytes);

        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats"
            << " AnonRss: " << (memoryUsage ? memoryUsage->AnonRss : 0) << " CGroupLimit: " << (memoryUsage ? memoryUsage->CGroupLimit : 0)
            << " HardLimitBytes: " << hardLimitBytes << " SoftLimitBytes: " << softLimitBytes << " TargetUtilizationBytes: " << targetUtilizationBytes
            << " AllocatedMemory: " << allocatedMemory << " ExternalConsumption: " << externalConsumption
            << " ConsumersConsumption: " << consumersConsumption << " TargetConsumersConsumption: " << targetConsumersConsumption);
        Counters->GetCounter("Stats/AnonRss")->Set(memoryUsage ? memoryUsage->AnonRss : 0);
        Counters->GetCounter("Stats/CGroupLimit")->Set(memoryUsage ? memoryUsage->CGroupLimit : 0);
        Counters->GetCounter("Stats/HardLimitBytes")->Set(hardLimitBytes);
        Counters->GetCounter("Stats/SoftLimitBytes")->Set(softLimitBytes);
        Counters->GetCounter("Stats/TargetUtilizationBytes")->Set(targetUtilizationBytes);
        Counters->GetCounter("Stats/AllocatedMemory")->Set(allocatedMemory);
        Counters->GetCounter("Stats/ExternalConsumption")->Set(externalConsumption);
        Counters->GetCounter("Stats/ConsumersConsumption")->Set(consumersConsumption);
        Counters->GetCounter("Stats/TargetConsumersConsumption")->Set(targetConsumersConsumption);

        for (auto index : xrange(consumers.size())) {
            auto& consumer = consumers[index];
            auto config = GetConsumerConfig(consumer.Kind);
            auto& bounds = consumersLimitBounds[index];
            auto& counters = GetConsumerCounters(consumer.Kind);

            ui64 limitBytes = bounds.MinBytes + static_cast<ui64>((bounds.MaxBytes - bounds.MinBytes) * coefficient);
            if (targetConsumersConsumption + externalConsumption > softLimitBytes && config.CanZeroLimit) {
                limitBytes -= Min(limitBytes, targetConsumersConsumption + externalConsumption - softLimitBytes);
            } 

            LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind 
                << " Consumption: " << consumer.Consumption << " LimitBytes: " << limitBytes
                << " MinPercent: " << config.MinPercent << " MinBytes: " << config.MinBytes 
                << " MaxPercent: " << config.MaxPercent << " MaxBytes: " << config.MaxBytes
                << " MinBound: " << bounds.MinBytes << " MaxBound: " << bounds.MaxBytes);
            counters.Consumption->Set(consumer.Consumption);
            counters.LimitBytes->Set(limitBytes);
            counters.LimitMinPercent->Set(config.MinPercent.value_or(0));
            counters.LimitMinBytes->Set(config.MinBytes.value_or(0));
            counters.LimitMaxPercent->Set(config.MaxPercent.value_or(0));
            counters.LimitMaxBytes->Set(config.MaxBytes.value_or(0));

            ApplyLimit(consumer, limitBytes);
        }

        ctx.Schedule(Interval, new TEvents::TEvWakeup());
    }

    void ApplyLimit(TMemoryConsumerSnapshot consumer, ui64 limitBytes) {
        // TODO: don't send if queued already?
        switch (consumer.Kind) {
            case EConsumerKind::SharedCache:
                Send(MakeSharedPageCacheId(), new TEvMemoryLimit(limitBytes));
                break;
            case EConsumerKind::MemTable:
                // TODO
                break;
            default:
                Y_ABORT("Unhandled consumer");
        }
    }

    TConsumerCounters& GetConsumerCounters(EConsumerKind consumer) {
        auto it = ConsumerCounters.FindPtr(consumer);
        if (it) {
            return *it;
        }

        return ConsumerCounters.emplace(consumer, TConsumerCounters{
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Consumption"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinPercent"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxPercent"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxBytes"),
        }).first->second;
    }

    TConsumerLimitBounds GetConsumerLimitBounds(EConsumerKind consumer, ui64 availableMemory) const {
        auto config = GetConsumerConfig(consumer);
        
        std::optional<ui64> minBytes;
        std::optional<ui64> maxBytes;

        if (config.MinPercent.has_value() && config.MinBytes.has_value()) {
            minBytes = Max(GetPercent(config.MinPercent.value(), availableMemory), config.MinBytes.value());
        } else if (config.MinPercent.has_value()) {
            minBytes = GetPercent(config.MinPercent.value(), availableMemory);
        } else if (config.MinBytes.has_value()) {
            minBytes = config.MinBytes.value();
        }

        if (config.MaxPercent.has_value() && config.MaxBytes.has_value()) {
            maxBytes = Min(GetPercent(config.MaxPercent.value(), availableMemory), config.MaxBytes.value());
        } else if (config.MaxPercent.has_value()) {
            maxBytes = GetPercent(config.MaxPercent.value(), availableMemory);
        } else if (config.MaxBytes.has_value()) {
            maxBytes = config.MaxBytes.value();
        }

        if (minBytes.has_value() && !maxBytes.has_value()) {
            maxBytes = minBytes;
        }
        if (!minBytes.has_value() && maxBytes.has_value()) {
            minBytes = maxBytes;
        }

        TConsumerLimitBounds result(minBytes.value_or(0), maxBytes.value_or(0));
        if (result.MinBytes > result.MaxBytes) {
            result.MinBytes = result.MaxBytes;
        }

        return result;
    }

    TConsumerConfig GetConsumerConfig(EConsumerKind consumer) const {
        TConsumerConfig result;

        switch (consumer) {
            case EConsumerKind::SharedCache: {
                if (Config.HasSharedCacheMinPercent() || Config.GetSharedCacheMinPercent()) {
                    result.MinPercent = Config.GetSharedCacheMinPercent();
                }
                if (Config.HasSharedCacheMinBytes() || Config.GetSharedCacheMinBytes()) {
                    result.MinBytes = Config.GetSharedCacheMinBytes();
                }
                if (Config.HasSharedCacheMaxPercent() || Config.GetSharedCacheMaxPercent()) {
                    result.MaxPercent = Config.GetSharedCacheMaxPercent();
                }
                if (Config.HasSharedCacheMaxBytes() || Config.GetSharedCacheMaxBytes()) {
                    result.MaxBytes = Config.GetSharedCacheMaxBytes();
                }
                result.CanZeroLimit = true;
                break;
            }
            case EConsumerKind::MemTable: {
                if (Config.HasMemTableMinPercent() || Config.GetMemTableMinPercent()) {
                    result.MinPercent = Config.GetMemTableMinPercent();
                }
                if (Config.HasMemTableMinBytes() || Config.GetMemTableMinBytes()) {
                    result.MinBytes = Config.GetMemTableMinBytes();
                }
                if (Config.HasMemTableMaxPercent() || Config.GetMemTableMaxPercent()) {
                    result.MaxPercent = Config.GetMemTableMaxPercent();
                }
                if (Config.HasMemTableMaxBytes() || Config.GetMemTableMaxBytes()) {
                    result.MaxBytes = Config.GetMemTableMaxBytes();
                }
                break;
            }
            default:
                Y_ABORT("Unhandled consumer");
        }

        return result;
    }

    ui64 GetHardLimitBytes(std::optional<TMemoryUsage> memoryUsage) const {
        if (Config.HasHardLimitBytes()) {
            return Config.GetHardLimitBytes();
        }
        if (memoryUsage && memoryUsage->CGroupLimit) {
            return memoryUsage->CGroupLimit;
        }
        // TODO: get total RAM
        return Config.GetHardLimitBytes();
    }

    ui64 GetSoftLimitBytes(ui64 hardLimitBytes) const {
        if (Config.HasSoftLimitPercent() && Config.HasSoftLimitBytes()) {
            return Min(GetPercent(hardLimitBytes, Config.GetSoftLimitPercent()), Config.GetSoftLimitBytes());
        }
        if (Config.HasSoftLimitBytes()) {
            return Config.GetSoftLimitBytes();
        }
        return GetPercent(hardLimitBytes, Config.GetSoftLimitPercent());
    }

    ui64 GetTargetUtilizationBytes(ui64 hardLimitBytes) const {
        if (Config.HasTargetUtilizationPercent() && Config.HasTargetUtilizationBytes()) {
            return Min(GetPercent(hardLimitBytes, Config.GetTargetUtilizationPercent()), Config.GetTargetUtilizationBytes());
        }
        if (Config.HasTargetUtilizationBytes()) {
            return Config.GetTargetUtilizationBytes();
        }
        return GetPercent(hardLimitBytes, Config.GetTargetUtilizationPercent());
    }

    ui64 GetPercent(float percent, ui64 value) const {
        return static_cast<ui64>(static_cast<double>(value) * percent / 100);
    }

private:
    const TDuration Interval;
    const TIntrusivePtr<IMemoryConsumers> Consumers_;
    const TMemoryConsumers& Consumers;
    NKikimrConfig::TMemoryControllerConfig Config;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<EConsumerKind, TConsumerCounters> ConsumerCounters;
};

}

TIntrusivePtr<IMemoryConsumers> CreateMemoryConsumers() {
    return MakeIntrusive<TMemoryConsumers>();
}

IActor* CreateMemoryController(TDuration interval, TIntrusivePtr<IMemoryConsumers> consumers, NKikimrConfig::TMemoryControllerConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TMemoryController(interval, consumers, config,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}