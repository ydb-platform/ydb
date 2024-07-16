#include "memory_controller.h"
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

TProcessMemoryInfo TProcessMemoryInfoProvider::IProcessMemoryInfoProvider::Get() const {
    std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();

    TProcessMemoryInfo result{
        TAllocState::GetAllocatedMemoryEstimate(),
        {}, {}
    };

    if (memoryUsage) {
        result.AnonRss.emplace(memoryUsage->AnonRss);
    }
    if (memoryUsage && memoryUsage->CGroupLimit) {
        result.CGroupLimit.emplace(memoryUsage->CGroupLimit);
    }

    return result;
}

namespace {

ui64 SafeDiff(ui64 a, ui64 b) {
    return a - Min(a, b);
}

class TMemTableMemoryConsumersCollection;

class TMemTableMemoryConsumer : public IMemTableMemoryConsumer {
public:
   TMemTableMemoryConsumer(TActorId owner, ui32 table, std::weak_ptr<TMemTableMemoryConsumersCollection> collection)
        : Collection(std::move(collection))
        , Owner(owner)
        , Table(table)
    {}

    ui64 GetConsumption() const {
        return Consumption;
    }

    void SetConsumption(ui64 consumption);

private:
    const std::weak_ptr<TMemTableMemoryConsumersCollection> Collection;
    std::atomic<ui64> Consumption = 0;

public:
    const TActorId Owner;
    const ui32 Table;
};

class TMemTableMemoryConsumersCollection : public std::enable_shared_from_this<TMemTableMemoryConsumersCollection> {
    friend TMemTableMemoryConsumer;

public:
    TMemTableMemoryConsumersCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NMemory::IMemoryConsumer> memoryConsumer)
        : Counters(counters)
        , MemTableTotalBytesCounter(Counters->GetCounter("MemTable/TotalBytes"))
        , MemTableCompactingBytesCounter(Counters->GetCounter("MemTable/CompactingBytes"))
        , MemTableCompactedBytesCounter(Counters->GetCounter("MemTable/CompactedBytes", true))
        , MemoryConsumer(std::move(memoryConsumer))
    {}

    TIntrusivePtr<TMemTableMemoryConsumer> Register(TActorId owner, ui32 table) {
        auto &ptr = Consumers[{owner, table}];
        if (!ptr) {
            ptr = MakeIntrusive<TMemTableMemoryConsumer>(owner, table, weak_from_this());
            NonCompacting.insert(ptr);
        }
        return ptr;
    }

    void Unregister(TActorId owner, ui32 table) {
        auto it = Consumers.find({owner, table});
        if (it != Consumers.end()) {
            auto& consumer = it->second;
            CompactionComplete(consumer);
            consumer->SetConsumption(0);
            Y_DEBUG_ABORT_UNLESS(NonCompacting.contains(consumer));
            NonCompacting.erase(consumer);
            Consumers.erase(it);
        }
    }

    void CompactionComplete(TIntrusivePtr<TMemTableMemoryConsumer> consumer) {
        auto it = Compacting.find(consumer);
        if (it != Compacting.end()) {
            MemTableCompactedBytesCounter->Add(it->second);
            ChangeTotalCompacting(-it->second);
            NonCompacting.insert(it->first);
            Compacting.erase(it);
        }
    }

    /**
     * @return consumers and their consumptions that should be compacted
     */
    TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> SelectForCompaction(ui64 limitBytes) {
        TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> result;

        ui64 toCompact = SafeDiff(TotalConsumption, limitBytes);
        if (toCompact <= TotalCompacting) {
            // nothing to compact more
            return result;
        }

        for (const auto &r : NonCompacting) {
            ui64 consumption = r->GetConsumption();
            if (consumption) {
                result.emplace_back(r, consumption);
            }
        }

        Sort(result, [](const auto &x, const auto &y) { return x.second > y.second; });

        size_t take = 0;
        for (auto it = result.begin(); it != result.end() && toCompact > TotalCompacting; it++) {
            auto reg = it->first;

            Compacting[reg] = it->second;
            Y_ABORT_UNLESS(NonCompacting.erase(reg));

            ChangeTotalCompacting(it->second);

            take++;
        }

        result.resize(take);
        return result;
    }

private:
    void ChangeTotalConsumption(ui64 delta) {
        ui64 consumption = TotalConsumption.fetch_add(delta) + delta;
        MemoryConsumer->SetConsumption(consumption);
        MemTableTotalBytesCounter->Set(consumption);
    }

    void ChangeTotalCompacting(ui64 delta) {
        ui64 compacting = (TotalCompacting += delta);
        MemTableCompactingBytesCounter->Set(compacting);
    }

private:
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const ::NMonitoring::TDynamicCounters::TCounterPtr MemTableTotalBytesCounter, MemTableCompactingBytesCounter, MemTableCompactedBytesCounter;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    TMap<std::pair<TActorId, ui32>, TIntrusivePtr<TMemTableMemoryConsumer>> Consumers;
    THashSet<TIntrusivePtr<TMemTableMemoryConsumer>> NonCompacting;
    THashMap<TIntrusivePtr<TMemTableMemoryConsumer>, ui64> Compacting;
    std::atomic<ui64> TotalConsumption = 0;

    // Approximate value, updates only on compaction start/stop
    // Counts only self triggered compactions
    ui64 TotalCompacting = 0;
};

void TMemTableMemoryConsumer::SetConsumption(ui64 newConsumption) {
    ui64 before = Consumption.exchange(newConsumption);
    if (auto t = Collection.lock()) {
        t->ChangeTotalConsumption(newConsumption - before);
    }
}

}

namespace {

using namespace NActors;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

struct TConsumerConfig {
    std::optional<float> MinPercent;
    std::optional<ui64> MinBytes;
    std::optional<float> MaxPercent;
    std::optional<ui64> MaxBytes;
    bool CanZeroLimit = false;
};

struct TConsumerState {
    TIntrusivePtr<TMemoryConsumer> Consumer_;
    EMemoryConsumerKind Kind;
    ui64 Consumption;
    TConsumerConfig Config;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;

    TConsumerState(TIntrusivePtr<TMemoryConsumer> consumer, TConsumerConfig config)
        : Consumer_(std::move(consumer))
        , Kind(Consumer_->GetKind())
        , Consumption(Consumer_->GetConsumption())
        , Config(config)
    {
    }

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

    TMemoryController(
            TDuration interval,
            TIntrusivePtr<TMemoryConsumersCollection> consumersCollection,
            TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
            const NKikimrConfig::TMemoryControllerConfig& config,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : Interval(interval)
        , ConsumersCollection(std::move(consumersCollection))
        , MemTableMemoryConsumersCollection(std::make_shared<TMemTableMemoryConsumersCollection>(counters, ConsumersCollection->Register(EMemoryConsumerKind::MemTable)))
        , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
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

            hFunc(TEvMemTableRegister, Handle);
            hFunc(TEvMemTableUnregister, Handle);
            hFunc(TEvMemTableCompacted, Handle);
        }
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        Config.Swap(ev->Get()->Record.MutableConfig()->MutableMemoryControllerConfig());
        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Config updated " << Config.DebugString());
    }

    void HandleWakeup(const TActorContext& ctx) noexcept {
        auto processMemoryInfo = ProcessMemoryInfoProvider->Get();

        ui64 hardLimitBytes = GetHardLimitBytes(processMemoryInfo);
        ui64 softLimitBytes = GetSoftLimitBytes(hardLimitBytes);
        ui64 targetUtilizationBytes = GetTargetUtilizationBytes(hardLimitBytes);

        auto consumers_ = ConsumersCollection->GetConsumers();
        TVector<TConsumerState> consumers(::Reserve(consumers_.size()));
        ui64 consumersConsumption = 0;
        for (auto& consumer : consumers_) {
            consumers.push_back(BuildConsumerState(std::move(consumer), hardLimitBytes));
            consumersConsumption += consumers.back().Consumption;
        }
        consumers_.clear();

        // allocatedMemory = externalConsumption + consumersConsumption
        ui64 externalConsumption = SafeDiff(processMemoryInfo.AllocatedMemory, consumersConsumption);
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
            // Note: take Max with current consumer consumption because memory free may happen with a delay, or don't happen at all 
            resultingConsumersConsumption += Max(consumer.Consumption, consumer.GetLimit(coefficient));
        }

        LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats"
            << " AnonRss: " << processMemoryInfo.AnonRss << " CGroupLimit: " << processMemoryInfo.CGroupLimit << " AllocatedMemory: " << processMemoryInfo.AllocatedMemory
            << " HardLimitBytes: " << hardLimitBytes << " SoftLimitBytes: " << softLimitBytes << " TargetUtilizationBytes: " << targetUtilizationBytes
            << " ConsumersConsumption: " << consumersConsumption << " ExternalConsumption: " << externalConsumption 
            << " TargetConsumersConsumption: " << targetConsumersConsumption << " ResultingConsumersConsumption: " << resultingConsumersConsumption
            << " Coefficient: " << coefficient);
        Counters->GetCounter("Stats/AnonRss")->Set(processMemoryInfo.AnonRss.value_or(0));
        Counters->GetCounter("Stats/CGroupLimit")->Set(processMemoryInfo.CGroupLimit.value_or(0));
        Counters->GetCounter("Stats/AllocatedMemory")->Set(processMemoryInfo.AllocatedMemory);
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

    void Handle(TEvMemTableRegister::TPtr &ev) {
        const auto *msg = ev->Get();
        auto consumer = MemTableMemoryConsumersCollection->Register(ev->Sender, msg->Table);
        Send(ev->Sender, new TEvMemTableRegistered(msg->Table, std::move(consumer)));
    }

    void Handle(TEvMemTableUnregister::TPtr &ev) {
        const auto *msg = ev->Get();
        MemTableMemoryConsumersCollection->Unregister(ev->Sender, msg->Table);
    }

    void Handle(TEvMemTableCompacted::TPtr &ev) {
        const auto *msg = ev->Get();
        if (auto consumer = dynamic_cast<TMemTableMemoryConsumer*>(msg->Consumer.Get())) {
            MemTableMemoryConsumersCollection->CompactionComplete(consumer);
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
        consumer.Consumer_->SetLimit(limitBytes);
        switch (consumer.Kind) {
            case EMemoryConsumerKind::SharedCache:
                Send(MakeSharedPageCacheId(), new TEvMemoryLimit(limitBytes));
                break;
            case EMemoryConsumerKind::MemTable:
                ApplyMemTableLimit(limitBytes);
                break;
            default:
                Y_ABORT("Unhandled consumer");
        }
    }

    void ApplyMemTableLimit(ui64 limitBytes) const {
        auto consumers = MemTableMemoryConsumersCollection->SelectForCompaction(limitBytes);
        for (const auto& consumer : consumers) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::MEMORY_CONTROLLER, "Request MemTable compaction of table " <<
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
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinBytes"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxBytes"),
        }).first->second;
    }

    TConsumerState BuildConsumerState(TIntrusivePtr<TMemoryConsumer>&& consumer, ui64 availableMemory) const {
        auto config = GetConsumerConfig(consumer->GetKind());
        
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
            default:
                Y_ABORT("Unhandled consumer");
        }

        return result;
    }

    ui64 GetHardLimitBytes(const TProcessMemoryInfo& info) const {
        if (Config.HasHardLimitBytes()) {
            return Config.GetHardLimitBytes();
        }
        if (info.CGroupLimit.has_value()) {
            return info.CGroupLimit.value();
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
    const TIntrusivePtr<TMemoryConsumersCollection> ConsumersCollection;
    std::shared_ptr<TMemTableMemoryConsumersCollection> MemTableMemoryConsumersCollection;
    const TIntrusiveConstPtr<IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
    NKikimrConfig::TMemoryControllerConfig Config;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<EMemoryConsumerKind, TConsumerCounters> ConsumerCounters;
};

}

IActor* CreateMemoryController(
        TDuration interval,
        TIntrusivePtr<TMemoryConsumersCollection> consumersCollection,
        TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
        const NKikimrConfig::TMemoryControllerConfig& config, 
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TMemoryController(
        interval,
        std::move(consumersCollection),
        std::move(processMemoryInfoProvider),
        config,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}