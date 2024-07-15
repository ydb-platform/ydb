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

struct TConsumerSnapshot {
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

    TConsumerSnapshot GetConsumerSnapshot() {
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

    TVector<TConsumerSnapshot> GetConsumersSnapshot() const {
        TGuard<TMutex> guard(Mutex);
        
        TVector<TConsumerSnapshot> result(::Reserve(Consumers.size()));

        // Note: don't care about destroyed consumers as that is possible only during process stopping

        for (const auto& consumer : Consumers) {
            result.push_back(consumer.second->GetConsumerSnapshot());
        }

        return result;
    }

private:
    TMutex Mutex;
    TMap<EConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
};

struct TConsumerConfig {
    std::optional<float> MinPercent;
    std::optional<ui64> MinBytes;
    std::optional<float> MaxPercent;
    std::optional<ui64> MaxBytes;
    bool CanZeroLimit = false;
};

struct TConsumerState : TConsumerSnapshot {
    TConsumerConfig Config;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;

    ui64 GetLimit(double coefficient) const {
        return static_cast<ui64>(MinBytes + coefficient * (MaxBytes - MinBytes));
    }
};

struct TConsumerCounters {
    TCounterPtr Consumption;
    TCounterPtr LimitBytes;
    TCounterPtr LimitMinBytes;
    TCounterPtr LimitMaxBytes;
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

        auto consumers_ = Consumers.GetConsumersSnapshot();
        TVector<TConsumerState> consumers(::Reserve(consumers_.size()));
        ui64 consumersConsumption = 0;
        for (auto& consumer : consumers) {
            consumersConsumption += consumer.Consumption;
            consumers.push_back(GetConsumerState(consumer, hardLimitBytes));
        }

        // allocatedMemory = externalConsumption + consumersConsumption
        ui64 externalConsumption = SafeDiff(allocatedMemory, consumersConsumption);
        // targetConsumersConsumption + externalConsumption = targetUtilizationBytes
        ui64 targetConsumersConsumption = SafeDiff(targetUtilizationBytes, externalConsumption);

        // want to find maximum possible coefficient in range [0..1] so that
        // Sum(
        //     Max(
        //         consumers[i].Consumption, 
        //         consumers[i].MinBytes + coefficient * (consumers[i].MaxBytes - consumers[i].MinBytes
        //        )
        //    ) <= targetConsumersConsumption

        auto coefficient = BinarySearchCoefficient(consumers, targetConsumersConsumption);

        ui64 resultingConsumersConsumption = 0;
        for (const auto& consumer : consumers) {
            resultingConsumersConsumption += Max(consumer.Consumption, consumer.GetLimit(coefficient));
        }

        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats"
            << " AnonRss: " << (memoryUsage ? memoryUsage->AnonRss : 0) << " CGroupLimit: " << (memoryUsage ? memoryUsage->CGroupLimit : 0) << " AllocatedMemory: " << allocatedMemory
            << " HardLimitBytes: " << hardLimitBytes << " SoftLimitBytes: " << softLimitBytes << " TargetUtilizationBytes: " << targetUtilizationBytes
            << " ConsumersConsumption: " << consumersConsumption << " ExternalConsumption: " << externalConsumption 
            << " TargetConsumersConsumption: " << targetConsumersConsumption << " ResultingConsumersConsumption: " << resultingConsumersConsumption
            << " Coefficient: " << coefficient);
        Counters->GetCounter("Stats/AnonRss")->Set(memoryUsage ? memoryUsage->AnonRss : 0);
        Counters->GetCounter("Stats/CGroupLimit")->Set(memoryUsage ? memoryUsage->CGroupLimit : 0);
        Counters->GetCounter("Stats/AllocatedMemory")->Set(allocatedMemory);
        Counters->GetCounter("Stats/HardLimitBytes")->Set(hardLimitBytes);
        Counters->GetCounter("Stats/SoftLimitBytes")->Set(softLimitBytes);
        Counters->GetCounter("Stats/TargetUtilizationBytes")->Set(targetUtilizationBytes);
        Counters->GetCounter("Stats/ConsumersConsumption")->Set(consumersConsumption);
        Counters->GetCounter("Stats/ExternalConsumption")->Set(externalConsumption);
        Counters->GetCounter("Stats/TargetConsumersConsumption")->Set(targetConsumersConsumption);
        Counters->GetCounter("Stats/ResultingConsumersConsumption")->Set(resultingConsumersConsumption);
        Counters->GetCounter("Stats/Coefficient")->Set(coefficient);

        for (const auto& consumer : consumers) {
            ui64 limitBytes = consumer.GetLimit(coefficient);
            if (resultingConsumersConsumption + externalConsumption > softLimitBytes && consumer.Config.CanZeroLimit) {
                limitBytes = SafeDiff(limitBytes, resultingConsumersConsumption + externalConsumption - softLimitBytes);
            } 

            LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind << " state "
                << " Consumption: " << consumer.Consumption << " LimitBytes: " << limitBytes
                << " MinBytes: " << consumer.MinBytes << " MaxBytes: " << consumer.MaxBytes);
            auto& counters = GetConsumerCounters(consumer.Kind);
            counters.Consumption->Set(consumer.Consumption);
            counters.LimitBytes->Set(limitBytes);
            counters.LimitMinBytes->Set(consumer.MinBytes);
            counters.LimitMaxBytes->Set(consumer.MaxBytes);

            ApplyLimit(consumer, limitBytes);
        }

        ctx.Schedule(Interval, new TEvents::TEvWakeup());
    }

    double BinarySearchCoefficient(const TVector<TConsumerState>& consumers, ui64 availableMemory) {
        static const ui32 BinarySearchIterations = 20;

        double left = 0, right = 1;
        for (ui32 iteration = 0; iteration < BinarySearchIterations; iteration++) {
            double middle = (left + right) / 2;

            ui64 value = 0;
            for (const auto& consumer : consumers) {
                value += Max(consumer.Consumption, consumer.GetLimit(middle));
            }

            if (value > availableMemory) {
                right = middle;
            } else {
                left = middle;
            }
        }

        return left;
    }

    void ApplyLimit(TConsumerState consumer, ui64 limitBytes) const {
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
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxBytes"),
        }).first->second;
    }

    TConsumerState GetConsumerState(const TConsumerSnapshot& consumer, ui64 availableMemory) const {
        auto config = GetConsumerConfig(consumer.Kind);
        
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

        TConsumerState result{consumer, config, minBytes.value_or(0), maxBytes.value_or(0)};

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

    static ui64 SafeDiff(ui64 a, ui64 b) {
        return a - Min(a, b);
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