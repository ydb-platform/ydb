#include "memory_controller.h"
#include "memtable_collection.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/protos/memory_stats.pb.h>
#include <ydb/core/tablet/resource_broker.h>
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

ui64 GetPercent(float percent, ui64 value) {
    return static_cast<ui64>(static_cast<double>(value) * (percent / 100.0));
}

}

namespace {

using namespace NActors;
using namespace NResourceBroker;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

struct TResourceBrokerLimits {
    ui64 LimitBytes;
    ui64 QueryExecutionLimitBytes;

    auto operator<=>(const TResourceBrokerLimits&) const = default;

    TString ToString() const noexcept {
        TStringBuilder result;
        result << "LimitBytes: " << LimitBytes;
        result << " QueryExecutionLimitBytes: " << QueryExecutionLimitBytes;
        return result;
    }
};

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

struct TConsumerState {
    const EMemoryConsumerKind Kind;
    const TActorId ActorId;
    const ui64 Consumption;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;
    bool CanZeroLimit = false;

    TConsumerState(const TMemoryConsumer& consumer)
        : Kind(consumer.Kind)
        , ActorId(consumer.ActorId)
        , Consumption(consumer.GetConsumption())
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

            HFunc(TEvResourceBroker::TEvConfigureResult, Handle);
        }
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        Config.Swap(ev->Get()->Record.MutableConfig()->MutableMemoryControllerConfig());
        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Config updated " << Config.DebugString());
    }

    void HandleWakeup(const TActorContext& ctx) noexcept {
        auto processMemoryInfo = ProcessMemoryInfoProvider->Get();

        bool hasMemTotalHardLimit = false;
        ui64 hardLimitBytes = GetHardLimitBytes(processMemoryInfo, hasMemTotalHardLimit);
        ui64 softLimitBytes = GetSoftLimitBytes(hardLimitBytes);
        ui64 targetUtilizationBytes = GetTargetUtilizationBytes(hardLimitBytes);
        ui64 activitiesLimitBytes = GetActivitiesLimitBytes(hardLimitBytes);

        TVector<TConsumerState> consumers(::Reserve(Consumers.size()));
        ui64 consumersConsumption = 0;
        for (const auto& consumer : Consumers) {
            consumers.push_back(BuildConsumerState(*consumer.second, hardLimitBytes));
            consumersConsumption += consumers.back().Consumption;
        }

        // allocatedMemory = otherConsumption + consumersConsumption
        ui64 otherConsumption = SafeDiff(processMemoryInfo.AllocatedMemory, consumersConsumption);

        ui64 externalConsumption = 0;
        if (hasMemTotalHardLimit && processMemoryInfo.AnonRss.has_value() 
                && processMemoryInfo.MemTotal.has_value() && processMemoryInfo.MemAvailable.has_value()) {
            // externalConsumption + AnonRss + MemAvailable = MemTotal
            externalConsumption = SafeDiff(processMemoryInfo.MemTotal.value(),
                processMemoryInfo.AnonRss.value() + processMemoryInfo.MemAvailable.value());
        }

        // targetConsumersConsumption + otherConsumption + externalConsumption = targetUtilizationBytes
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
            << " HardLimit: " << hardLimitBytes << " SoftLimit: " << softLimitBytes << " TargetUtilization: " << targetUtilizationBytes
            << " ActivitiesLimitBytes: " << activitiesLimitBytes
            << " ConsumersConsumption: " << consumersConsumption << " OtherConsumption: " << otherConsumption << " ExternalConsumption: " << externalConsumption
            << " TargetConsumersConsumption: " << targetConsumersConsumption << " ResultingConsumersConsumption: " << resultingConsumersConsumption
            << " Coefficient: " << coefficient);

        Counters->GetCounter("Stats/AnonRss")->Set(processMemoryInfo.AnonRss.value_or(0));
        Counters->GetCounter("Stats/CGroupLimit")->Set(processMemoryInfo.CGroupLimit.value_or(0));
        Counters->GetCounter("Stats/MemTotal")->Set(processMemoryInfo.MemTotal.value_or(0));
        Counters->GetCounter("Stats/MemAvailable")->Set(processMemoryInfo.MemAvailable.value_or(0));
        Counters->GetCounter("Stats/AllocatedMemory")->Set(processMemoryInfo.AllocatedMemory);
        Counters->GetCounter("Stats/AllocatorCachesMemory")->Set(processMemoryInfo.AllocatorCachesMemory);
        Counters->GetCounter("Stats/HardLimit")->Set(hardLimitBytes);
        Counters->GetCounter("Stats/SoftLimit")->Set(softLimitBytes);
        Counters->GetCounter("Stats/TargetUtilization")->Set(targetUtilizationBytes);
        Counters->GetCounter("Stats/ActivitiesLimitBytes")->Set(activitiesLimitBytes);
        Counters->GetCounter("Stats/ConsumersConsumption")->Set(consumersConsumption);
        Counters->GetCounter("Stats/OtherConsumption")->Set(otherConsumption);
        Counters->GetCounter("Stats/ExternalConsumption")->Set(externalConsumption);
        Counters->GetCounter("Stats/TargetConsumersConsumption")->Set(targetConsumersConsumption);
        Counters->GetCounter("Stats/ResultingConsumersConsumption")->Set(resultingConsumersConsumption);
        Counters->GetCounter("Stats/Coefficient")->Set(coefficient * 1e9);

        auto *memoryStatsUpdate = new NNodeWhiteboard::TEvWhiteboard::TEvMemoryStatsUpdate();
        auto& memoryStats = memoryStatsUpdate->Record;
        if (processMemoryInfo.AnonRss.has_value()) memoryStats.SetAnonRss(processMemoryInfo.AnonRss.value());
        if (processMemoryInfo.CGroupLimit.has_value()) memoryStats.SetCGroupLimit(processMemoryInfo.CGroupLimit.value());
        if (processMemoryInfo.MemTotal.has_value()) memoryStats.SetMemTotal(processMemoryInfo.MemTotal.value());
        if (processMemoryInfo.MemAvailable.has_value()) memoryStats.SetMemAvailable(processMemoryInfo.MemAvailable.value());
        memoryStats.SetAllocatedMemory(processMemoryInfo.AllocatedMemory);
        memoryStats.SetAllocatorCachesMemory(processMemoryInfo.AllocatorCachesMemory);
        memoryStats.SetHardLimit(hardLimitBytes);
        memoryStats.SetSoftLimit(softLimitBytes);
        memoryStats.SetTargetUtilization(targetUtilizationBytes);
        memoryStats.SetConsumersConsumption(consumersConsumption);
        memoryStats.SetOtherConsumption(otherConsumption);
        if (hasMemTotalHardLimit) memoryStats.SetExternalConsumption(externalConsumption);

        ui64 consumersLimitBytes = 0;
        for (const auto& consumer : consumers) {
            ui64 limitBytes = consumer.GetLimit(coefficient);
            if (resultingConsumersConsumption + otherConsumption + externalConsumption > softLimitBytes && consumer.CanZeroLimit) {
                limitBytes = SafeDiff(limitBytes, resultingConsumersConsumption + otherConsumption + externalConsumption - softLimitBytes);
            }
            consumersLimitBytes += limitBytes;

            LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind << " state:"
                << " Consumption: " << consumer.Consumption << " Limit: " << limitBytes
                << " Min: " << consumer.MinBytes << " Max: " << consumer.MaxBytes);
            auto& counters = GetConsumerCounters(consumer.Kind);
            counters.Consumption->Set(consumer.Consumption);
            counters.Reservation->Set(SafeDiff(limitBytes, consumer.Consumption));
            counters.LimitBytes->Set(limitBytes);
            counters.LimitMinBytes->Set(consumer.MinBytes);
            counters.LimitMaxBytes->Set(consumer.MaxBytes);
            SetMemoryStats(consumer, memoryStats, limitBytes);

            ApplyLimit(consumer, limitBytes);
        }

        Counters->GetCounter("Stats/ConsumersLimit")->Set(consumersLimitBytes);
        memoryStats.SetConsumersLimit(consumersLimitBytes);

        // Note: for now ResourceBroker and its queues aren't MemoryController consumers and don't share limits with other caches
        ApplyResourceBrokerLimits(hardLimitBytes, activitiesLimitBytes);

        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()), memoryStatsUpdate);

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

    void Handle(TEvResourceBroker::TEvConfigureResult::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        LOG_LOG_S(ctx, 
            msg->Record.GetSuccess() ? NActors::NLog::PRI_INFO : NActors::NLog::PRI_ERROR, 
            NKikimrServices::MEMORY_CONTROLLER, 
            "ResourceBroker configure result " << msg->Record.ShortDebugString());
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

    void ApplyResourceBrokerLimits(ui64 hardLimitBytes, ui64 activitiesLimitBytes) {
        ui64 queryExecutionLimitBytes = GetQueryExecutionLimitBytes(hardLimitBytes);

        TResourceBrokerLimits newLimits{
            activitiesLimitBytes,
            queryExecutionLimitBytes
        };
        
        if (newLimits == CurrentResourceBrokerLimits) {
            return;
        }

        CurrentResourceBrokerLimits = newLimits;

        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::MEMORY_CONTROLLER, "Consumer QueryExecution state:"
            << " Limit: " << newLimits.QueryExecutionLimitBytes);

        Counters->GetCounter("Consumer/QueryExecution/Limit")->Set(newLimits.QueryExecutionLimitBytes);

        TAutoPtr<TEvResourceBroker::TEvConfigure> configure = new TEvResourceBroker::TEvConfigure();
        configure->Merge = true;
        configure->Record.MutableResourceLimit()->SetMemory(activitiesLimitBytes);

        auto queue = configure->Record.AddQueues();
        queue->SetName(NLocalDb::KqpResourceManagerQueue);
        queue->MutableLimit()->SetMemory(queryExecutionLimitBytes);

        Send(MakeResourceBrokerID(), configure.Release());
    }

    TConsumerCounters& GetConsumerCounters(EMemoryConsumerKind consumer) {
        auto it = ConsumerCounters.FindPtr(consumer);
        if (it) {
            return *it;
        }

        return ConsumerCounters.emplace(consumer, TConsumerCounters{
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Consumption"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Reservation"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Limit"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMin"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMax"),
        }).first->second;
    }

    void SetMemoryStats(const TConsumerState& consumer, NKikimrMemory::TMemoryStats& stats, ui64 limitBytes) const {
        switch (consumer.Kind) {
            case EMemoryConsumerKind::MemTable: {
                stats.SetMemTableConsumption(consumer.Consumption);
                stats.SetMemTableLimit(limitBytes);
                break;
            }
            case EMemoryConsumerKind::SharedCache: {
                stats.SetSharedCacheConsumption(consumer.Consumption);
                stats.SetSharedCacheLimit(limitBytes);
                break;
            }
            default:
                Y_ABORT("Unhandled consumer");
        }
    }

    TConsumerState BuildConsumerState(const TMemoryConsumer& consumer, ui64 hardLimitBytes) const {
        TConsumerState result(consumer);
        
        switch (consumer.Kind) {
            case EMemoryConsumerKind::MemTable: {
                result.MinBytes = GetMemTableMinBytes(hardLimitBytes);
                result.MaxBytes = GetMemTableMaxBytes(hardLimitBytes);
                break;
            }
            case EMemoryConsumerKind::SharedCache: {
                result.MinBytes = GetSharedCacheMinBytes(hardLimitBytes);
                result.MaxBytes = GetSharedCacheMaxBytes(hardLimitBytes);
                result.CanZeroLimit = true;
                break;
            }
            default:
                Y_ABORT("Unhandled consumer");
        }

        if (result.MinBytes > result.MaxBytes) {
            result.MinBytes = result.MaxBytes;
        }

        return result;
    }

    ui64 GetHardLimitBytes(const TProcessMemoryInfo& info, bool& hasMemTotalHardLimit) const {
        if (Config.HasHardLimitBytes()) {
            ui64 hardLimitBytes = Config.GetHardLimitBytes();
            if (info.CGroupLimit.has_value()) {
                hardLimitBytes = Min(hardLimitBytes, info.CGroupLimit.value());
            }
            return hardLimitBytes;
        }
        if (info.CGroupLimit.has_value()) {
            return info.CGroupLimit.value();
        }
        if (info.MemTotal) {
            hasMemTotalHardLimit = true;
            return info.MemTotal.value();
        }
        return 512_MB; // fallback
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

    ui64 GetActivitiesLimitBytes(ui64 hardLimitBytes) const {
        if (Config.HasActivitiesLimitPercent() && Config.HasActivitiesLimitBytes()) {
            return Min(GetPercent(Config.GetActivitiesLimitPercent(), hardLimitBytes), Config.GetActivitiesLimitBytes());
        }
        if (Config.HasActivitiesLimitBytes()) {
            return Config.GetActivitiesLimitBytes();
        }
        return GetPercent(Config.GetActivitiesLimitPercent(), hardLimitBytes);
    }
    
    ui64 GetMemTableMinBytes(ui64 hardLimitBytes) const {
        if (Config.HasMemTableMinPercent() && Config.HasMemTableMinBytes()) {
            return Max(GetPercent(Config.GetMemTableMinPercent(), hardLimitBytes), Config.GetMemTableMinBytes());
        }
        if (Config.HasMemTableMinBytes()) {
            return Config.GetMemTableMinBytes();
        }
        return GetPercent(Config.GetMemTableMinPercent(), hardLimitBytes);
    }

    ui64 GetMemTableMaxBytes(ui64 hardLimitBytes) const {
        if (Config.HasMemTableMaxPercent() && Config.HasMemTableMaxBytes()) {
            return Min(GetPercent(Config.GetMemTableMaxPercent(), hardLimitBytes), Config.GetMemTableMaxBytes());
        }
        if (Config.HasMemTableMaxBytes()) {
            return Config.GetMemTableMaxBytes();
        }
        return GetPercent(Config.GetMemTableMaxPercent(), hardLimitBytes);
    }

    ui64 GetSharedCacheMinBytes(ui64 hardLimitBytes) const {
        if (Config.HasSharedCacheMinPercent() && Config.HasSharedCacheMinBytes()) {
            return Max(GetPercent(Config.GetSharedCacheMinPercent(), hardLimitBytes), Config.GetSharedCacheMinBytes());
        }
        if (Config.HasSharedCacheMinBytes()) {
            return Config.GetSharedCacheMinBytes();
        }
        return GetPercent(Config.GetSharedCacheMinPercent(), hardLimitBytes);
    }

    ui64 GetSharedCacheMaxBytes(ui64 hardLimitBytes) const {
        if (Config.HasSharedCacheMaxPercent() && Config.HasSharedCacheMaxBytes()) {
            return Min(GetPercent(Config.GetSharedCacheMaxPercent(), hardLimitBytes), Config.GetSharedCacheMaxBytes());
        }
        if (Config.HasSharedCacheMaxBytes()) {
            return Config.GetSharedCacheMaxBytes();
        }
        return GetPercent(Config.GetSharedCacheMaxPercent(), hardLimitBytes);
    }

    ui64 GetQueryExecutionLimitBytes(ui64 hardLimitBytes) const {
        if (Config.HasQueryExecutionLimitPercent() && Config.HasQueryExecutionLimitBytes()) {
            return Min(GetPercent(Config.GetQueryExecutionLimitPercent(), hardLimitBytes), Config.GetQueryExecutionLimitBytes());
        }
        if (Config.HasQueryExecutionLimitBytes()) {
            return Config.GetQueryExecutionLimitBytes();
        }
        return GetPercent(Config.GetQueryExecutionLimitPercent(), hardLimitBytes);
    }

private:
    const TDuration Interval;
    TMap<EMemoryConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
    std::shared_ptr<TMemTableMemoryConsumersCollection> MemTables;
    const TIntrusiveConstPtr<IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
    NKikimrConfig::TMemoryControllerConfig Config;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<EMemoryConsumerKind, TConsumerCounters> ConsumerCounters;
    std::optional<TResourceBrokerLimits> CurrentResourceBrokerLimits;
};

}

IActor* CreateMemoryController(
        TDuration interval,
        TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
        const NKikimrConfig::TMemoryControllerConfig& config, 
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TMemoryController(
        interval,
        std::move(processMemoryInfoProvider),
        config,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}