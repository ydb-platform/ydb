#include "memory_controller.h"
#include "memory_controller_config.h"
#include "memtable_collection.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/localdb.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/protos/memory_stats.pb.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/minikql/aligned_page_pool.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/memory_pool.h>

#include <util/stream/format.h>

#include <tcmalloc/malloc_extension.h>

namespace NKikimr::NMemory {

namespace {

ui64 SafeDiff(ui64 a, ui64 b) {
    return a - Min(a, b);
}

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

TString HumanReadableBytes(std::optional<ui64> bytes) {
    return bytes.has_value() ? TString(TStringBuilder() << HumanReadableBytes(bytes.value())) : "none";
}

}

namespace {

using namespace NActors;
using namespace NResourceBroker;
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

struct TConsumerState {
    const EMemoryConsumerKind Kind;
    const TActorId ActorId;
    const ui64 Consumption;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;
    bool CanZeroLimit = false;
    std::optional<ui64> ExactLimit;

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
            const TResourceBrokerConfig& resourceBrokerConfig,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : Interval(interval)
        , MemTables(std::make_shared<TMemTableMemoryConsumersCollection>(counters,
            Consumers.emplace(EMemoryConsumerKind::MemTable, MakeIntrusive<TMemoryConsumer>(EMemoryConsumerKind::MemTable, TActorId{})).first->second))
        , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
        , Config(config)
        , ResourceBrokerSelfConfig(resourceBrokerConfig)
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                    NKikimrConsole::TConfigItem::MemoryControllerConfigItem}));

        // When profiling memory it's convenient to set initial tcmalloc soft limit
#ifdef PROFILE_MEMORY_ALLOCATIONS
        auto processMemoryInfo = ProcessMemoryInfoProvider->Get();
        bool hasMemTotalHardLimit = false;
        ui64 hardLimitBytes = GetHardLimitBytes(Config, processMemoryInfo, hasMemTotalHardLimit);
        ui64 softLimitBytes = GetSoftLimitBytes(Config, hardLimitBytes);

        tcmalloc::MallocExtension::SetMemoryLimit(softLimitBytes, tcmalloc::MallocExtension::LimitKind::kSoft);

        LOG_NOTICE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Set tcmalloc soft limit " << softLimitBytes);
#endif

        HandleWakeup(ctx);

        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Bootstrapped with config " << Config.ShortDebugString());
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
        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Config updated " << Config.ShortDebugString());
    }

    void HandleWakeup(const TActorContext& ctx) noexcept {
        auto processMemoryInfo = ProcessMemoryInfoProvider->Get();

        bool hasMemTotalHardLimit = false;
        ui64 hardLimitBytes = GetHardLimitBytes(Config, processMemoryInfo, hasMemTotalHardLimit);
        ui64 softLimitBytes = GetSoftLimitBytes(Config, hardLimitBytes);
        ui64 targetUtilizationBytes = GetTargetUtilizationBytes(Config, hardLimitBytes);
        ui64 activitiesLimitBytes = ResourceBrokerSelfConfig.LimitBytes
            ? ResourceBrokerSelfConfig.LimitBytes // for backward compatibility
            : GetActivitiesLimitBytes(Config, hardLimitBytes);

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
            << " AnonRss: " << HumanReadableBytes(processMemoryInfo.AnonRss) << " CGroupLimit: " << HumanReadableBytes(processMemoryInfo.CGroupLimit)
            << " MemTotal: " << HumanReadableBytes(processMemoryInfo.MemTotal) << " MemAvailable: " << HumanReadableBytes(processMemoryInfo.MemAvailable)
            << " AllocatedMemory: " << HumanReadableBytes(processMemoryInfo.AllocatedMemory) << " AllocatorCachesMemory: " << HumanReadableBytes(processMemoryInfo.AllocatorCachesMemory)
            << " HardLimit: " << HumanReadableBytes(hardLimitBytes) << " SoftLimit: " << HumanReadableBytes(softLimitBytes) << " TargetUtilization: " << HumanReadableBytes(targetUtilizationBytes)
            << " ActivitiesLimitBytes: " << HumanReadableBytes(activitiesLimitBytes)
            << " ConsumersConsumption: " << HumanReadableBytes(consumersConsumption) << " OtherConsumption: " << HumanReadableBytes(otherConsumption) << " ExternalConsumption: " << HumanReadableBytes(externalConsumption)
            << " TargetConsumersConsumption: " << HumanReadableBytes(targetConsumersConsumption) << " ResultingConsumersConsumption: " << HumanReadableBytes(resultingConsumersConsumption)
            << " Coefficient: " << coefficient);

        Counters->GetCounter("Stats/AnonRss")->Set(processMemoryInfo.AnonRss.value_or(0));
        Counters->GetCounter("Stats/CGroupLimit")->Set(processMemoryInfo.CGroupLimit.value_or(0));
        Counters->GetCounter("Stats/MemTotal")->Set(processMemoryInfo.MemTotal.value_or(0));
        Counters->GetCounter("Stats/MemAvailable")->Set(processMemoryInfo.MemAvailable.value_or(0));
        Counters->GetCounter("Stats/MemMapsCount")->Set(GetMemoryMapsCount());
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
        Counters->GetCounter("Stats/ArrowAllocatedMemory")->Set(arrow::default_memory_pool()->bytes_allocated());
        Counters->GetCounter("Stats/ArrowYqlAllocatedMemory")->Set(NYql::NUdf::GetYqlMemoryPool()->bytes_allocated());

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
        if (hasMemTotalHardLimit) memoryStats.SetExternalConsumption(externalConsumption);

        ui64 consumersLimitBytes = 0;
        for (const auto& consumer : consumers) {
            const bool isExactLimitConsumer = consumer.ExactLimit.has_value();
            ui64 limitBytes;
            if (isExactLimitConsumer) {
                limitBytes = consumer.ExactLimit.value();
            } else {
                limitBytes = consumer.GetLimit(coefficient);
                if (resultingConsumersConsumption + otherConsumption + externalConsumption > softLimitBytes && consumer.CanZeroLimit) {
                    limitBytes = SafeDiff(limitBytes, resultingConsumersConsumption + otherConsumption + externalConsumption - softLimitBytes);
                }
            }
            consumersLimitBytes += limitBytes;

            LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind << " state:"
                << " Consumption: " << HumanReadableBytes(consumer.Consumption) << " Limit: " << HumanReadableBytes(limitBytes)
                << " Min: " << HumanReadableBytes(consumer.MinBytes) << " Max: " << HumanReadableBytes(consumer.MaxBytes));
            auto& counters = GetConsumerCounters(consumer.Kind, !isExactLimitConsumer);
            counters.Consumption->Set(consumer.Consumption);
            counters.Reservation->Set(SafeDiff(limitBytes, consumer.Consumption));
            counters.LimitBytes->Set(limitBytes);
            if (counters.LimitMinBytes) {
                counters.LimitMinBytes->Set(consumer.MinBytes);
            }
            if (counters.LimitMaxBytes) {
                counters.LimitMaxBytes->Set(consumer.MaxBytes);
            }
            SetMemoryStats(consumer, memoryStats, limitBytes);

            ApplyLimit(consumer, limitBytes);
        }

        Counters->GetCounter("Stats/ConsumersLimit")->Set(consumersLimitBytes);

        ProcessResourceBrokerConfig(ctx, memoryStats, hardLimitBytes, activitiesLimitBytes);

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
            case EMemoryConsumerKind::SharedCache:
            case EMemoryConsumerKind::BlobCache:
            case EMemoryConsumerKind::DataAccessorCache:
                Send(consumer.ActorId, new TEvConsumerLimit(limitBytes));
                break;
            case EMemoryConsumerKind::ScanGroupedMemoryLimiter:
            case EMemoryConsumerKind::CompGroupedMemoryLimiter:
                Send(consumer.ActorId, new TEvConsumerLimit(limitBytes * NKikimr::NOlap::TGlobalLimits::GroupedMemoryLimiterSoftLimitCoefficient, limitBytes));
                break;
        }
    }

    void ApplyMemTableLimit(ui64 limitBytes) const {
        auto consumers = MemTables->SelectForCompaction(limitBytes);
        for (const auto& consumer : consumers) {
            LOG_TRACE_S(TlsActivationContext->AsActorContext(), NKikimrServices::MEMORY_CONTROLLER, "Request MemTable compaction of table " <<
                consumer.first->Table << " with " << HumanReadableBytes(consumer.second));
            Send(consumer.first->Owner, new TEvMemTableCompact(consumer.first->Table, consumer.second));
        }
    }

    void ProcessResourceBrokerConfig(const TActorContext& ctx, NKikimrMemory::TMemoryStats& memoryStats, ui64 hardLimitBytes,
                                     ui64 activitiesLimitBytes) {
        ui64 queryExecutionConsumption = TAlignedPagePool::GetGlobalPagePoolSize();
        ui64 queryExecutionLimitBytes = ResourceBrokerSelfConfig.QueryExecutionLimitBytes
            ? ResourceBrokerSelfConfig.QueryExecutionLimitBytes // for backward compatibility
            : GetQueryExecutionLimitBytes(Config, hardLimitBytes);
        LOG_INFO_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer QueryExecution state:" << " Consumption: " << HumanReadableBytes(queryExecutionConsumption) << " Limit: " << HumanReadableBytes(queryExecutionLimitBytes));
        Counters->GetCounter("Consumer/QueryExecution/Consumption")->Set(queryExecutionConsumption);
        Counters->GetCounter("Consumer/QueryExecution/Limit")->Set(queryExecutionLimitBytes);
        memoryStats.SetQueryExecutionConsumption(queryExecutionConsumption);
        memoryStats.SetQueryExecutionLimit(queryExecutionLimitBytes);

        ui64 columnTablesCompactionLimitBytes = GetColumnTablesCompactionLimitBytes(Config, hardLimitBytes);

        // Note: for now ResourceBroker and its queues aren't MemoryController consumers and don't share limits with other caches
        ApplyResourceBrokerConfig({
            .LimitBytes = activitiesLimitBytes,
            .QueryExecutionLimitBytes = queryExecutionLimitBytes,
            .ColumnTablesCompactionLimitBytes = columnTablesCompactionLimitBytes});
    }

    void ApplyResourceBrokerConfig(TResourceBrokerConfig config) {
        if (config == CurrentResourceBrokerConfig) {
            return;
        }

        TAutoPtr<TEvResourceBroker::TEvConfigure> configure = new TEvResourceBroker::TEvConfigure();
        configure->Merge = true;

        auto& record = configure->Record;
        record.MutableResourceLimit()->SetMemory(config.LimitBytes);

        // Compaction uses 4 queues, but there is only one memory limit setting in the configuration,
        // so the coefficients are used to split allocated memory between the queues.
        using TGlobalOlapLimits = NKikimr::NOlap::TGlobalLimits;
        AddLimitToQueueConfig(record, NLocalDb::KqpResourceManagerQueue, config.QueryExecutionLimitBytes);
        AddLimitToQueueConfig(record, NLocalDb::ColumnShardCompactionIndexationQueue,
            config.ColumnTablesCompactionLimitBytes * TGlobalOlapLimits::CompactionIndexationQueueLimitCoefficient);
        AddLimitToQueueConfig(record, NLocalDb::ColumnShardCompactionTtlQueue,
            config.ColumnTablesCompactionLimitBytes * TGlobalOlapLimits::CompactionTtlQueueLimitCoefficient);
        AddLimitToQueueConfig(record, NLocalDb::ColumnShardCompactionGeneralQueue,
            config.ColumnTablesCompactionLimitBytes * TGlobalOlapLimits::CompactionGeneralQueueLimitCoefficient);
        AddLimitToQueueConfig(record, NLocalDb::ColumnShardCompactionNormalizerQueue,
            config.ColumnTablesCompactionLimitBytes * TGlobalOlapLimits::CompactionNormalizerQueueLimitCoefficient);

        Send(MakeResourceBrokerID(), configure.Release());

        CurrentResourceBrokerConfig.emplace(std::move(config));
    }

    void AddLimitToQueueConfig(NKikimrResourceBroker::TResourceBrokerConfig& record, const TString& name, const ui64 limitBytes) {
        auto queue = record.AddQueues();
        queue->SetName(name);
        queue->MutableLimit()->SetMemory(limitBytes);
    }

    TConsumerCounters& GetConsumerCounters(EMemoryConsumerKind consumer, const bool minMaxRequired) {
        auto it = ConsumerCounters.FindPtr(consumer);
        if (it) {
            return *it;
        }

        TCounterPtr limitMinBytes = minMaxRequired ? Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMin") : nullptr;
        TCounterPtr limitMaxBytes = minMaxRequired ? Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMax") : nullptr;

        return ConsumerCounters.emplace(consumer, TConsumerCounters{
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Consumption"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Reservation"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Limit"),
            limitMinBytes,
            limitMaxBytes,
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
            case EMemoryConsumerKind::ScanGroupedMemoryLimiter: {
                stats.SetColumnTablesReadExecutionConsumption(consumer.Consumption);
                stats.SetColumnTablesReadExecutionLimit(limitBytes);
                break;
            }
            case EMemoryConsumerKind::CompGroupedMemoryLimiter: {
                stats.SetColumnTablesCompactionConsumption(consumer.Consumption);
                stats.SetColumnTablesCompactionLimit(limitBytes);
                break;
            }
            case EMemoryConsumerKind::DataAccessorCache:
            case EMemoryConsumerKind::BlobCache: {
                stats.SetColumnTablesCacheConsumption(
                    (stats.HasColumnTablesCacheConsumption() ? stats.GetColumnTablesCacheConsumption() : 0) + consumer.Consumption);
                stats.SetColumnTablesCacheLimit((stats.HasColumnTablesCacheLimit() ? stats.GetColumnTablesCacheLimit() : 0) + limitBytes);
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
                result.MinBytes = GetMemTableMinBytes(Config, hardLimitBytes);
                result.MaxBytes = GetMemTableMaxBytes(Config, hardLimitBytes);
                break;
            }
            case EMemoryConsumerKind::SharedCache: {
                result.MinBytes = GetSharedCacheMinBytes(Config, hardLimitBytes);
                result.MaxBytes = GetSharedCacheMaxBytes(Config, hardLimitBytes);
                result.CanZeroLimit = true;
                break;
            }
            case EMemoryConsumerKind::ScanGroupedMemoryLimiter: {
                result.ExactLimit = GetColumnTablesReadExecutionLimitBytes(Config, hardLimitBytes);
                break;
            }
            case EMemoryConsumerKind::CompGroupedMemoryLimiter: {
                result.ExactLimit = GetColumnTablesCompactionLimitBytes(Config, hardLimitBytes) *
                    NKikimr::NOlap::TGlobalLimits::GroupedMemoryLimiterCompactionLimitCoefficient;
                break;
            }
            case EMemoryConsumerKind::BlobCache: {
                result.ExactLimit = GetColumnTablesCacheLimitBytes(Config, hardLimitBytes) * NKikimr::NOlap::TGlobalLimits::BlobCacheCoefficient;
                break;
            }
            case EMemoryConsumerKind::DataAccessorCache: {
                result.ExactLimit = GetColumnTablesCacheLimitBytes(Config, hardLimitBytes) * NKikimr::NOlap::TGlobalLimits::DataAccessorCoefficient;
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

private:
    const TDuration Interval;
    TMap<EMemoryConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
    std::shared_ptr<TMemTableMemoryConsumersCollection> MemTables;
    const TIntrusiveConstPtr<IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
    NKikimrConfig::TMemoryControllerConfig Config;
    TResourceBrokerConfig ResourceBrokerSelfConfig;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<EMemoryConsumerKind, TConsumerCounters> ConsumerCounters;
    std::optional<TResourceBrokerConfig> CurrentResourceBrokerConfig;
};

}

IActor* CreateMemoryController(
        TDuration interval,
        TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
        const NKikimrConfig::TMemoryControllerConfig& config,
        const TResourceBrokerConfig& resourceBrokerSelfConfig,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TMemoryController(
        interval,
        std::move(processMemoryInfoProvider),
        config,
        resourceBrokerSelfConfig,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}
