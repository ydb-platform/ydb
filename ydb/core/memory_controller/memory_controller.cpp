#include "memory_controller.h"
#include "memtable_collection.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NMemory {

namespace {

ui64 SafeDiff(ui64 a, ui64 b) {
    return a - Min(a, b);
}

}

namespace {

using namespace NActors;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

class TMemoryConsumer : public IMemoryConsumer {
public:
    TMemoryConsumer(EMemoryConsumerKind kind, TActorId actorId)
        : Kind(kind)
        , ActorId(actorId)
    {
    }

    ui64 GetConsumption() const {
        return Consumption;
    }

    void SetConsumption(ui64 value) override {
        Consumption = value;
    }

public:
    const EMemoryConsumerKind Kind;
    const TActorId ActorId;
private:
    std::atomic<ui64> Consumption = 0;
};

struct TConsumerConfig {
    std::optional<float> MinPercent;
    std::optional<ui64> MinBytes;
    std::optional<float> MaxPercent;
    std::optional<ui64> MaxBytes;
    bool CanZeroLimit = false;
};

struct TConsumerState {
    const EMemoryConsumerKind Kind;
    const TActorId ActorId;
    const ui64 Consumption;
    const TConsumerConfig Config;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;

    TConsumerState(const TMemoryConsumer& consumer, TConsumerConfig config)
        : Kind(consumer.Kind)
        , ActorId(consumer.ActorId)
        , Consumption(consumer.GetConsumption())
        , Config(config)
    {
    }

    ui64 GetLimit(double coefficient) const {
        Y_DEBUG_ABORT_UNLESS(MinBytes <= MaxBytes);
        return static_cast<ui64>(MinBytes + coefficient * (MaxBytes - MinBytes));
    }
};

struct TConsumerCounters {
    TCounterPtr Consumption;
    TCounterPtr Reservation;
    TCounterPtr LimitBytes;
    TCounterPtr LimitMinBytes;
    TCounterPtr LimitMaxBytes;
};

class TMemoryController : public TActorBootstrapped<TMemoryController> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MEMORY_CONTROLLER;
    }

    TMemoryController(
            TDuration interval,
            TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
            const NKikimrConfig::TMemoryControllerConfig& config,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : Interval(interval)
        , MemTables(std::make_shared<TMemTableMemoryConsumersCollection>(counters, 
            Consumers.emplace(EMemoryConsumerKind::MemTable, MakeIntrusive<TMemoryConsumer>(EMemoryConsumerKind::MemTable, TActorId{})).first->second))
        , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
        , Config(config)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), 
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                    NKikimrConsole::TConfigItem::MemoryControllerConfigItem}));

        HandleWakeup(ctx);

        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Bootstrapped");
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);

            HFunc(TEvConsumerRegister, Handle);

            HFunc(TEvMemTableRegister, Handle);
            HFunc(TEvMemTableUnregister, Handle);
            HFunc(TEvMemTableCompacted, Handle);
        }
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        Config.Swap(ev->Get()->Record.MutableConfig()->MutableMemoryControllerConfig());
        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Config updated " << Config.DebugString());
    }

    void HandleWakeup(const TActorContext& ctx) noexcept {
        auto processMemoryInfo = ProcessMemoryInfoProvider->Get();

        ui64 hardLimitBytes = GetHardLimitBytes(processMemoryInfo);
        // TODO: pass hard limit to node whiteboard and mem observer
        ui64 softLimitBytes = GetSoftLimitBytes(hardLimitBytes);
        ui64 targetUtilizationBytes = GetTargetUtilizationBytes(hardLimitBytes);

        TVector<TConsumerState> consumers(::Reserve(Consumers.size()));
        ui64 consumersConsumption = 0;
        for (const auto& consumer : Consumers) {
            consumers.push_back(BuildConsumerState(*consumer.second, hardLimitBytes));
            consumersConsumption += consumers.back().Consumption;
        }

        // allocatedMemory = otherConsumption + consumersConsumption
        ui64 otherConsumption = SafeDiff(processMemoryInfo.AllocatedMemory, consumersConsumption);

        ui64 externalConsumption = 0;
        if (!processMemoryInfo.CGroupLimit.has_value() && processMemoryInfo.AnonRss.has_value() 
                && processMemoryInfo.MemTotal.has_value() && processMemoryInfo.MemAvailable.has_value()) {
            // externalConsumption + AnonRss + MemAvailable = MemTotal
            externalConsumption = SafeDiff(processMemoryInfo.MemTotal.value(),
                processMemoryInfo.AnonRss.value() + processMemoryInfo.MemAvailable.value());
        }

        // targetConsumersConsumption + otherConsumption = targetUtilizationBytes
        ui64 targetConsumersConsumption = SafeDiff(targetUtilizationBytes, otherConsumption + externalConsumption);

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
            // Note: take Max with current consumer consumption because memory free may happen with a delay, or don't happen at all 
            resultingConsumersConsumption += Max(consumer.Consumption, consumer.GetLimit(coefficient));
        }

        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats:"
            << " AnonRss: " << processMemoryInfo.AnonRss << " CGroupLimit: " << processMemoryInfo.CGroupLimit 
            << " MemTotal: " << processMemoryInfo.MemTotal << " MemAvailable: " << processMemoryInfo.MemAvailable
            << " AllocatedMemory: " << processMemoryInfo.AllocatedMemory << " AllocatorCachesMemory: " << processMemoryInfo.AllocatorCachesMemory
            << " HardLimitBytes: " << hardLimitBytes << " SoftLimitBytes: " << softLimitBytes << " TargetUtilizationBytes: " << targetUtilizationBytes
            << " ConsumersConsumption: " << consumersConsumption << " OtherConsumption: " << otherConsumption<< " ExternalConsumption: " << externalConsumption
            << " TargetConsumersConsumption: " << targetConsumersConsumption << " ResultingConsumersConsumption: " << resultingConsumersConsumption
            << " Coefficient: " << coefficient);
        Counters->GetCounter("Stats/AnonRss")->Set(processMemoryInfo.AnonRss.value_or(0));
        Counters->GetCounter("Stats/CGroupLimit")->Set(processMemoryInfo.CGroupLimit.value_or(0));
        Counters->GetCounter("Stats/MemTotal")->Set(processMemoryInfo.MemTotal.value_or(0));
        Counters->GetCounter("Stats/MemAvailable")->Set(processMemoryInfo.MemAvailable.value_or(0));
        Counters->GetCounter("Stats/AllocatedMemory")->Set(processMemoryInfo.AllocatedMemory);
        Counters->GetCounter("Stats/AllocatorCachesMemory")->Set(processMemoryInfo.AllocatorCachesMemory);
        Counters->GetCounter("Stats/HardLimitBytes")->Set(hardLimitBytes);
        Counters->GetCounter("Stats/SoftLimitBytes")->Set(softLimitBytes);
        Counters->GetCounter("Stats/TargetUtilizationBytes")->Set(targetUtilizationBytes);
        Counters->GetCounter("Stats/ConsumersConsumption")->Set(consumersConsumption);
        Counters->GetCounter("Stats/ExternalConsumption")->Set(externalConsumption);
        Counters->GetCounter("Stats/OtherConsumption")->Set(otherConsumption);
        Counters->GetCounter("Stats/TargetConsumersConsumption")->Set(targetConsumersConsumption);
        Counters->GetCounter("Stats/ResultingConsumersConsumption")->Set(resultingConsumersConsumption);
        Counters->GetCounter("Stats/Coefficient")->Set(coefficient * 1e9);

        ui64 consumersLimitBytes = 0;
        for (const auto& consumer : consumers) {
            ui64 limitBytes = consumer.GetLimit(coefficient);
            if (resultingConsumersConsumption + otherConsumption + externalConsumption > softLimitBytes && consumer.Config.CanZeroLimit) {
                limitBytes = SafeDiff(limitBytes, resultingConsumersConsumption + otherConsumption + externalConsumption - softLimitBytes);
            }
            consumersLimitBytes += limitBytes;

            LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind << " state:"
                << " Consumption: " << consumer.Consumption << " LimitBytes: " << limitBytes
                << " MinBytes: " << consumer.MinBytes << " MaxBytes: " << consumer.MaxBytes);
            auto& counters = GetConsumerCounters(consumer.Kind);
            counters.Consumption->Set(consumer.Consumption);
            counters.Reservation->Set(SafeDiff(limitBytes, consumer.Consumption));
            counters.LimitBytes->Set(limitBytes);
            counters.LimitMinBytes->Set(consumer.MinBytes);
            counters.LimitMaxBytes->Set(consumer.MaxBytes);

            ApplyLimit(consumer, limitBytes);
        }

        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()), 
            NNodeWhiteboard::TEvWhiteboard::CreateCachesConsumptionUpdateRequest(consumersConsumption, consumersLimitBytes));

        ctx.Schedule(Interval, new TEvents::TEvWakeup());
    }

    void Handle(TEvConsumerRegister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        auto consumer = Consumers.emplace(msg->Kind, MakeIntrusive<TMemoryConsumer>(msg->Kind, ev->Sender));
        Y_ABORT_UNLESS(consumer.second, "Consumer kinds should be unique");
        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << msg->Kind << " " << ev->Sender << " registered");
        Send(ev->Sender, new TEvConsumerRegistered(consumer.first->second));
    }

    void Handle(TEvMemTableRegister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        auto consumer = MemTables->Register(ev->Sender, msg->Table);
        LOG_TRACE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "MemTable " << ev->Sender << " " << msg->Table << " registered");
        Send(ev->Sender, new TEvMemTableRegistered(msg->Table, std::move(consumer)));
    }

    void Handle(TEvMemTableUnregister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        MemTables->Unregister(ev->Sender, msg->Table);
        LOG_TRACE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "MemTable " << ev->Sender << " " << msg->Table << " unregistered");
    }

    void Handle(TEvMemTableCompacted::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        if (auto consumer = dynamic_cast<TMemTableMemoryConsumer*>(msg->MemoryConsumer.Get())) {
            ui32 table = MemTables->CompactionComplete(consumer);
            LOG_TRACE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "MemTable " << ev->Sender << " " << table << " compacted");
        }
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

    void ApplyLimit(const TConsumerState& consumer, ui64 limitBytes) const {
        switch (consumer.Kind) {
            case EMemoryConsumerKind::MemTable:
                ApplyMemTableLimit(limitBytes);
                break;
            default:
                Send(consumer.ActorId, new TEvConsumerLimit(limitBytes));
                break;

        }
    }

    void ApplyMemTableLimit(ui64 limitBytes) const {
        auto consumers = MemTables->SelectForCompaction(limitBytes);
        for (const auto& consumer : consumers) {
            LOG_TRACE_S(TlsActivationContext->AsActorContext(), NKikimrServices::MEMORY_CONTROLLER, "Request MemTable compaction of table " <<
                consumer.first->Table << " with " << consumer.second << " bytes");
            Send(consumer.first->Owner, new TEvMemTableCompact(consumer.first->Table, consumer.second));
        }
    }

    TConsumerCounters& GetConsumerCounters(EMemoryConsumerKind consumer) {
        auto it = ConsumerCounters.FindPtr(consumer);
        if (it) {
            return *it;
        }

        return ConsumerCounters.emplace(consumer, TConsumerCounters{
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Consumption"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Reservation"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxBytes"),
        }).first->second;
    }

    TConsumerState BuildConsumerState(const TMemoryConsumer& consumer, ui64 availableMemory) const {
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

        TConsumerState result(std::move(consumer), config);

        result.MinBytes = minBytes.value_or(0);
        result.MaxBytes = maxBytes.value_or(0);
        if (result.MinBytes > result.MaxBytes) {
            result.MinBytes = result.MaxBytes;
        }

        return result;
    }

    TConsumerConfig GetConsumerConfig(EMemoryConsumerKind consumer) const {
        TConsumerConfig result;

        switch (consumer) {
            case EMemoryConsumerKind::MemTable: {
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
            case EMemoryConsumerKind::SharedCache: {
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
            default:
                Y_ABORT("Unhandled consumer");
        }

        return result;
    }

    ui64 GetHardLimitBytes(const TProcessMemoryInfo& info) const {
        if (Config.HasHardLimitBytes()) {
            ui64 hardLimitBytes = Config.GetHardLimitBytes();
            if (info.CGroupLimit.has_value()) {
                hardLimitBytes = Min(hardLimitBytes, info.CGroupLimit.value());
            }
            return Config.GetHardLimitBytes();
        }
        if (info.CGroupLimit.has_value()) {
            return info.CGroupLimit.value();
        }
        if (info.MemTotal) {
            return info.MemTotal.value();
        }
        return 512_MB;
    }

    ui64 GetSoftLimitBytes(ui64 hardLimitBytes) const {
        if (Config.HasSoftLimitPercent() && Config.HasSoftLimitBytes()) {
            return Min(GetPercent(Config.GetSoftLimitPercent(), hardLimitBytes), Config.GetSoftLimitBytes());
        }
        if (Config.HasSoftLimitBytes()) {
            return Config.GetSoftLimitBytes();
        }
        return GetPercent(Config.GetSoftLimitPercent(), hardLimitBytes);
    }

    ui64 GetTargetUtilizationBytes(ui64 hardLimitBytes) const {
        if (Config.HasTargetUtilizationPercent() && Config.HasTargetUtilizationBytes()) {
            return Min(GetPercent(Config.GetTargetUtilizationPercent(), hardLimitBytes), Config.GetTargetUtilizationBytes());
        }
        if (Config.HasTargetUtilizationBytes()) {
            return Config.GetTargetUtilizationBytes();
        }
        return GetPercent(Config.GetTargetUtilizationPercent(), hardLimitBytes);
    }

    ui64 GetPercent(float percent, ui64 value) const {
        return static_cast<ui64>(static_cast<double>(value) * (percent / 100.0));
    }

private:
    const TDuration Interval;
    TMap<EMemoryConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
    std::shared_ptr<TMemTableMemoryConsumersCollection> MemTables;
    const TIntrusiveConstPtr<IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
    NKikimrConfig::TMemoryControllerConfig Config;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<EMemoryConsumerKind, TConsumerCounters> ConsumerCounters;
};

}

IActor* CreateMemoryController(
        TDuration interval,
        TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
        const NKikimrConfig::TMemoryControllerConfig& config, 
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TMemoryController(
        interval,
        std::move(processMemoryInfoProvider),
        config,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}