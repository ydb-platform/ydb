#include "memory_controller.h"
#include "memory_controller_config.h"
#include "consumer_collection.h"
#include "memtable_collection.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/localdb.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/memory_controller_iface.h_serialized.h>
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
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::MEMORY_CONTROLLER

namespace NKikimr::NMemory {

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

TString HumanReadableBytes(std::optional<ui64> bytes) {
    return bytes.has_value() ? TString(TStringBuilder() << HumanReadableBytes(bytes.value())) : "none";
}

namespace {

using namespace NActors;
using namespace NResourceBroker;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

ui64 SafeDiff(ui64 a, ui64 b) {
    return a - Min(a, b);
}

ui64 GetMemoryMapsCountOrZero() {
    try {
        return GetMemoryMapsCount();
    } catch (const yexception&) {
        return 0;
    }
}

class TMemoryConsumer : public IMemoryConsumer {
public:
    explicit TMemoryConsumer(EMemoryConsumerKind kind)
        : Kind(kind)
    {
    }

    virtual ui64 GetConsumption() const {
        return Used;
    }

    ui64 GetDemand() const {
        return Demand;
    }

    ui64 GetReclaimable() const {
        return Reclaimable;
    }

    void SetReport(TConsumerReport report) override {
        Used = report.Used;
        Demand = report.Demand;
        Reclaimable = report.Reclaimable;
    }

public:
    const EMemoryConsumerKind Kind;
private:
    std::atomic<ui64> Used = 0;
    std::atomic<ui64> Demand = 0;
    std::atomic<ui64> Reclaimable = 0;
};

class TColumnTablesPortionsMetaDataCacheMemoryConsumer: public TMemoryConsumer {
public:
    TColumnTablesPortionsMetaDataCacheMemoryConsumer()
        : TMemoryConsumer(EMemoryConsumerKind::ColumnTablesPortionsMetaDataCache) {
    }

    ui64 GetConsumption() const override {
        return NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner::GetNodePortionsConsumption();
    }
};

struct TConsumerState {
    const EMemoryConsumerKind Kind;
    const ui64 Consumption;
    const ui64 Demand;
    const ui64 Reclaimable;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;
    bool CanZeroLimit = false;

    TConsumerState(const TMemoryConsumer& consumer)
        : Kind(consumer.Kind)
        , Consumption(consumer.GetConsumption())
        // Three atomics are not a snapshot: clamp torn reads instead of asserting invariants.
        , Demand(Max(consumer.GetDemand(), Consumption))
        , Reclaimable(Min(consumer.GetReclaimable(), Consumption))
    {
    }

    TConsumerState(EMemoryConsumerKind kind, const TConsumerReport& report)
        : Kind(kind)
        , Consumption(report.Used)
        , Demand(Max(report.Demand, Consumption))
        , Reclaimable(Min(report.Reclaimable, Consumption))
    {
    }

    ui64 GetLimit(double coefficient) const {
        Y_DEBUG_ABORT_UNLESS(MinBytes <= MaxBytes);
        return static_cast<ui64>(MinBytes + coefficient * (MaxBytes - MinBytes));
    }
};

struct TConsumerCounters {
    TCounterPtr Consumption;
    TCounterPtr Demand;
    TCounterPtr Reclaimable;
    TCounterPtr Reservation;
    TCounterPtr LimitBytes;
    TCounterPtr LimitMinBytes;
    TCounterPtr LimitMaxBytes;
};

using TLimitBytesGetter = ui64 (*)(const NKikimrConfig::TMemoryControllerConfig& config, ui64 hardLimitBytes);
// A stats writer adds one consumer's report to its fields of TMemoryStats
using TStatsWriter = void (*)(NKikimrMemory::TMemoryStats& stats, const TConsumerState& consumer, bool summed, bool withLimit, ui64 limitBytes);

#define MEMORY_STATS_WRITER(name) \
    void Write##name##Stats(NKikimrMemory::TMemoryStats& stats, const TConsumerState& consumer, bool summed, bool withLimit, ui64 limitBytes) { \
        if (!summed) { \
            Y_ASSERT(!stats.Has##name##Consumption()); \
        } \
        if (withLimit) { \
            Y_ASSERT(!stats.Has##name##Limit()); \
        } \
        const ui64 base = summed ? stats.Get##name##Consumption() : 0; \
        const ui64 baseDemand = summed ? stats.Get##name##Demand() : 0; \
        const ui64 baseReclaimable = summed ? stats.Get##name##Reclaimable() : 0; \
        stats.Set##name##Consumption(base + consumer.Consumption); \
        stats.Set##name##Demand(baseDemand + consumer.Demand); \
        stats.Set##name##Reclaimable(baseReclaimable + consumer.Reclaimable); \
        if (withLimit) { \
            stats.Set##name##Limit(limitBytes); \
        } \
    }

MEMORY_STATS_WRITER(MemTable)
MEMORY_STATS_WRITER(SharedCache)
MEMORY_STATS_WRITER(Compaction)
MEMORY_STATS_WRITER(QueryExecution)

#undef MEMORY_STATS_WRITER

enum class ELimitDelivery {
    MemTableCompaction, // MC selects memtables and asks them to compact
    LimitShares, // every registrant of the kind is told its own ceiling
    PortionsCacheSetter,
};

struct TConsumerTraits {
    EMemoryConsumerKind Kind;
    bool ElasticLimit; // false: the limit is not flexible, only the consumption is accounted
    bool CanZeroLimit;
    TLimitBytesGetter GetMinBytes;
    TLimitBytesGetter GetMaxBytes;
    ELimitDelivery LimitDelivery;
    TStatsWriter WriteStats;
    bool StatsSummed; // several kinds add up into the same fields
    bool StatsWithLimit; // the kind writes the limit field
};

// One row per EMemoryConsumerKind, in the enum order
constexpr TConsumerTraits ConsumerTraits[] = {
    {
        .Kind = EMemoryConsumerKind::SharedCache,
        .ElasticLimit = true,
        .CanZeroLimit = true,
        .GetMinBytes = &GetSharedCacheMinBytes,
        .GetMaxBytes = &GetSharedCacheMaxBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteSharedCacheStats,
        .StatsSummed = true,
        .StatsWithLimit = true,
    },
    {
        .Kind = EMemoryConsumerKind::MemTable,
        .ElasticLimit = true,
        .CanZeroLimit = false,
        .GetMinBytes = &GetMemTableMinBytes,
        .GetMaxBytes = &GetMemTableMaxBytes,
        .LimitDelivery = ELimitDelivery::MemTableCompaction,
        .WriteStats = &WriteMemTableStats,
        .StatsSummed = false,
        .StatsWithLimit = true,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesScanGroupedMemory,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesScanGroupedMemoryLimitBytes,
        .GetMaxBytes = &GetColumnTablesScanGroupedMemoryLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteQueryExecutionStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesCompGroupedMemory,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesCompGroupedMemoryLimitBytes,
        .GetMaxBytes = &GetColumnTablesCompGroupedMemoryLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteCompactionStats,
        .StatsSummed = false,
        .StatsWithLimit = true,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesBlobCache,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesBlobCacheLimitBytes,
        .GetMaxBytes = &GetColumnTablesBlobCacheLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteSharedCacheStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesDataAccessorCache,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesDataAccessorCacheLimitBytes,
        .GetMaxBytes = &GetColumnTablesDataAccessorCacheLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteSharedCacheStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesColumnDataCache,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesColumnDataCacheLimitBytes,
        .GetMaxBytes = &GetColumnTablesColumnDataCacheLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteSharedCacheStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesDeduplicationGroupedMemory,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetColumnTablesDeduplicationGroupedMemoryLimitBytes,
        .GetMaxBytes = &GetColumnTablesDeduplicationGroupedMemoryLimitBytes,
        .LimitDelivery = ELimitDelivery::LimitShares,
        .WriteStats = &WriteQueryExecutionStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
    {
        .Kind = EMemoryConsumerKind::ColumnTablesPortionsMetaDataCache,
        .ElasticLimit = false,
        .CanZeroLimit = false,
        .GetMinBytes = &GetPortionsMetaDataCacheLimitBytes,
        .GetMaxBytes = &GetPortionsMetaDataCacheLimitBytes,
        .LimitDelivery = ELimitDelivery::PortionsCacheSetter,
        .WriteStats = &WriteSharedCacheStats,
        .StatsSummed = true,
        .StatsWithLimit = false,
    },
};

constexpr bool ConsumerTraitsFollowEnumOrder() {
    for (size_t i = 0; i < std::size(ConsumerTraits); ++i) {
        if (static_cast<size_t>(ConsumerTraits[i].Kind) != i) {
            return false;
        }
    }
    return true;
}

static_assert(std::size(ConsumerTraits) == GetEnumItemsCount<EMemoryConsumerKind>(), "expected one traits row per EMemoryConsumerKind");
static_assert(ConsumerTraitsFollowEnumOrder(), "expected ConsumerTraits[i].Kind == i");

const TConsumerTraits& GetConsumerTraits(EMemoryConsumerKind kind) {
    const size_t index = static_cast<size_t>(kind);
    Y_ABORT_UNLESS(index < std::size(ConsumerTraits));
    return ConsumerTraits[index];
}

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
            Consumers.emplace(EMemoryConsumerKind::MemTable, MakeIntrusive<TMemoryConsumer>(EMemoryConsumerKind::MemTable)).first->second))
        , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
        , Config(config)
        , ResourceBrokerSelfConfig(resourceBrokerConfig)
        , Counters(counters)
    {
        Consumers.emplace(EMemoryConsumerKind::ColumnTablesPortionsMetaDataCache, MakeIntrusive<TColumnTablesPortionsMetaDataCacheMemoryConsumer>());
    }

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

        YDB_LOG_NOTICE_CTX(ctx, "Set tcmalloc soft limit",
            {"softLimitBytes", softLimitBytes});
#endif

        HandleWakeup(ctx);

        YDB_LOG_INFO_CTX(ctx, "Bootstrapped with config",
            {"config", Config});
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);

            HFunc(TEvConsumerRegister, Handle);
            HFunc(TEvConsumerUnregister, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);

            HFunc(TEvMemTableRegister, Handle);
            HFunc(TEvMemTableUnregister, Handle);
            HFunc(TEvMemTableCompacted, Handle);

            HFunc(TEvResourceBroker::TEvConfigureResult, Handle);
        }
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        Config.Swap(ev->Get()->Record.MutableConfig()->MutableMemoryControllerConfig());
        YDB_LOG_INFO_CTX(ctx, "Config updated",
            {"config", Config});
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

        TVector<TConsumerState> consumers(::Reserve(Consumers.size() + Collections.size()));
        ui64 consumersConsumption = 0;
        for (const auto& consumer : Consumers) {
            consumers.push_back(BuildConsumerState(*consumer.second, hardLimitBytes));
            consumersConsumption += consumers.back().Consumption;
        }
        for (const auto& [kind, collection] : Collections) {
            consumers.push_back(BuildConsumerState(kind, collection, hardLimitBytes));
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
        // Sum(consumers[i].MinBytes + coefficient * (consumers[i].MaxBytes - consumers[i].MinBytes)) <= targetConsumersConsumption
        auto coefficient = BinarySearchCoefficient(consumers, targetConsumersConsumption);

        ui64 resultingConsumersConsumption = 0;
        for (const auto& consumer : consumers) {
            // Note: take Max with current consumer consumption because memory free may happen with a delay, or don't happen at all
            resultingConsumersConsumption += GetResultingConsumption(consumer, coefficient);
        }

        YDB_LOG_INFO_CTX(ctx, "Periodic memory stats",
            {"anonRss", HumanReadableBytes(processMemoryInfo.AnonRss)},
            {"CGroupLimit", HumanReadableBytes(processMemoryInfo.CGroupLimit)},
            {"memTotal", HumanReadableBytes(processMemoryInfo.MemTotal)},
            {"memAvailable", HumanReadableBytes(processMemoryInfo.MemAvailable)},
            {"allocatedMemory", HumanReadableBytes(processMemoryInfo.AllocatedMemory)},
            {"allocatorCachesMemory", HumanReadableBytes(processMemoryInfo.AllocatorCachesMemory)},
            {"hardLimit", HumanReadableBytes(hardLimitBytes)},
            {"softLimit", HumanReadableBytes(softLimitBytes)},
            {"targetUtilization", HumanReadableBytes(targetUtilizationBytes)},
            {"activitiesLimitBytes", HumanReadableBytes(activitiesLimitBytes)},
            {"consumersConsumption", HumanReadableBytes(consumersConsumption)},
            {"otherConsumption", HumanReadableBytes(otherConsumption)},
            {"externalConsumption", HumanReadableBytes(externalConsumption)},
            {"targetConsumersConsumption", HumanReadableBytes(targetConsumersConsumption)},
            {"resultingConsumersConsumption", HumanReadableBytes(resultingConsumersConsumption)},
            {"coefficient", coefficient});

        Counters->GetCounter("Stats/AnonRss")->Set(processMemoryInfo.AnonRss.value_or(0));
        Counters->GetCounter("Stats/CGroupLimit")->Set(processMemoryInfo.CGroupLimit.value_or(0));
        Counters->GetCounter("Stats/MemTotal")->Set(processMemoryInfo.MemTotal.value_or(0));
        Counters->GetCounter("Stats/MemAvailable")->Set(processMemoryInfo.MemAvailable.value_or(0));
        Counters->GetCounter("Stats/MemMapsCount")->Set(GetMemoryMapsCountOrZero());
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
            ui64 limitBytes = consumer.GetLimit(coefficient);
            if (resultingConsumersConsumption + otherConsumption + externalConsumption > softLimitBytes && consumer.CanZeroLimit) {
                limitBytes = SafeDiff(limitBytes, resultingConsumersConsumption + otherConsumption + externalConsumption - softLimitBytes);
            }
            consumersLimitBytes += limitBytes;

            YDB_LOG_INFO_CTX(ctx, "Consumer state",
                {"consumerKind", consumer.Kind},
                {"consumption", HumanReadableBytes(consumer.Consumption)},
                {"demand", HumanReadableBytes(consumer.Demand)},
                {"reclaimable", HumanReadableBytes(consumer.Reclaimable)},
                {"limit", HumanReadableBytes(limitBytes)},
                {"min", HumanReadableBytes(consumer.MinBytes)},
                {"max", HumanReadableBytes(consumer.MaxBytes)});
            auto& counters = GetConsumerCounters(consumer.Kind);
            counters.Consumption->Set(consumer.Consumption);
            counters.Demand->Set(consumer.Demand);
            counters.Reclaimable->Set(consumer.Reclaimable);
            counters.Reservation->Set(SafeDiff(limitBytes, consumer.Consumption));
            counters.LimitBytes->Set(limitBytes);
            counters.LimitMinBytes->Set(consumer.MinBytes);
            counters.LimitMaxBytes->Set(consumer.MaxBytes);
            AddMemoryStats(consumer, memoryStats, limitBytes);

            ApplyLimit(consumer, limitBytes);
        }

        Counters->GetCounter("Stats/ConsumersLimit")->Set(consumersLimitBytes);

        ProcessResourceBrokerConfig(ctx, memoryStats, hardLimitBytes, activitiesLimitBytes);

        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()), memoryStatsUpdate);

        ctx.Schedule(Interval, new TEvents::TEvWakeup());
    }

    void Handle(TEvConsumerRegister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        // A kind the controller feeds itself has no registrant and must not be taken over
        Y_ABORT_UNLESS(!Consumers.contains(msg->Kind), "Consumer kind is owned by the memory controller");
        TIntrusivePtr<IMemoryConsumer> consumer = Collections[msg->Kind].Register(ev->Sender);
        YDB_LOG_INFO_CTX(ctx, "Consumer registered",
            {"msgKind", msg->Kind},
            {"sender", ev->Sender});
        Send(ev->Sender, new TEvConsumerRegistered(std::move(consumer)));
    }

    void Handle(TEvConsumerUnregister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        auto it = Collections.find(msg->Kind);
        if (it == Collections.end() || !it->second.Unregister(ev->Sender)) {
            YDB_LOG_WARN_CTX(ctx, "Consumer unregister ignored",
                {"msgKind", msg->Kind},
                {"sender", ev->Sender});
            return;
        }
        if (it->second.IsEmpty()) {
            Collections.erase(it);
            // Nothing updates the gauges of a removed kind any more, so zero them instead of leaving the last values
            ResetConsumerCounters(msg->Kind);
        }
        YDB_LOG_INFO_CTX(ctx, "Consumer unregistered",
            {"msgKind", msg->Kind},
            {"sender", ev->Sender});
    }

    void ResetConsumerCounters(EMemoryConsumerKind kind) {
        auto& counters = GetConsumerCounters(kind);
        counters.Consumption->Set(0);
        counters.Demand->Set(0);
        counters.Reclaimable->Set(0);
        counters.Reservation->Set(0);
        counters.LimitBytes->Set(0);
        counters.LimitMinBytes->Set(0);
        counters.LimitMaxBytes->Set(0);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext& ctx) {
        // Only limit sends are tracked, so an undelivered one marks a dead registrant
        if (ev->Get()->SourceType != EvConsumerLimit) {
            return;
        }
        for (auto it = Collections.begin(); it != Collections.end();) {
            if (it->second.Unregister(ev->Sender)) {
                YDB_LOG_INFO_CTX(ctx, "Consumer registrant died",
                    {"msgKind", it->first},
                    {"registrant", ev->Sender});
                Counters->GetCounter("Stats/ConsumerRegistrantDeaths", true)->Inc();
                if (it->second.IsEmpty()) {
                    ResetConsumerCounters(it->first);
                    it = Collections.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    void Handle(TEvMemTableRegister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        auto consumer = MemTables->Register(ev->Sender, msg->Table);
        YDB_LOG_TRACE_CTX(ctx, "MemTable registered",
            {"sender", ev->Sender},
            {"table", msg->Table});
        Send(ev->Sender, new TEvMemTableRegistered(msg->Table, std::move(consumer)));
    }

    void Handle(TEvMemTableUnregister::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        MemTables->Unregister(ev->Sender, msg->Table);
        YDB_LOG_TRACE_CTX(ctx, "MemTable unregistered",
            {"sender", ev->Sender},
            {"table", msg->Table});
    }

    void Handle(TEvMemTableCompacted::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        if (auto consumer = dynamic_cast<TMemTableMemoryConsumer*>(msg->MemoryConsumer.Get())) {
            ui32 table = MemTables->CompactionComplete(consumer);
            YDB_LOG_TRACE_CTX(ctx, "MemTable compacted",
                {"sender", ev->Sender},
                {"table", table});
        }
    }

    void Handle(TEvResourceBroker::TEvConfigureResult::TPtr &ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        YDB_LOG_CTX(ctx, msg->Record.GetSuccess() ? NActors::NLog::PRI_INFO : NActors::NLog::PRI_ERROR, "ResourceBroker configure result",
            {"ev", msg->Record});
    }

    double BinarySearchCoefficient(const TVector<TConsumerState>& consumers, ui64 availableMemory) {
        static const ui32 BinarySearchIterations = 20;

        double left = 0, right = 1;
        for (ui32 iteration = 0; iteration < BinarySearchIterations; iteration++) {
            double middle = (left + right) / 2;

            ui64 value = 0;
            for (const auto& consumer : consumers) {
                value += GetTargetConsumption(consumer, middle);
            }

            if (value > availableMemory) {
                right = middle;
            } else {
                left = middle;
            }
        }

        return left;
    }

    ui64 GetTargetConsumption(const TConsumerState& consumer, const double coefficient) const {
        return GetConsumerTraits(consumer.Kind).ElasticLimit
            ? consumer.GetLimit(coefficient)
            : consumer.Consumption;
    }

    ui64 GetResultingConsumption(const TConsumerState& consumer, const double coefficient) const {
        return GetConsumerTraits(consumer.Kind).ElasticLimit
            ? Max(consumer.Consumption, consumer.GetLimit(coefficient))
            : consumer.Consumption;
    }

    void ApplyLimit(const TConsumerState& consumer, ui64 limitBytes) const {
        switch (GetConsumerTraits(consumer.Kind).LimitDelivery) {
            case ELimitDelivery::MemTableCompaction:
                ApplyMemTableLimit(limitBytes);
                break;
            case ELimitDelivery::LimitShares:
                SendLimitShares(consumer.Kind, limitBytes);
                break;
            case ELimitDelivery::PortionsCacheSetter:
                NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner::SetPortionsCacheLimit(limitBytes);
                break;
        }
    }

    void SendLimitShares(EMemoryConsumerKind kind, ui64 limitBytes) const {
        const auto* collection = Collections.FindPtr(kind);
        if (!collection) {
            return;
        }
        for (const auto& share : collection->ComputeLimitShares(limitBytes)) {
            // Delivery tracking turns a send to a dead registrant into TEvUndelivered, which drops its entry
            Send(share.Registrant, new TEvConsumerLimit(share.Bytes), IEventHandle::FlagTrackDelivery);
        }
    }

    void ApplyMemTableLimit(ui64 limitBytes) const {
        auto consumers = MemTables->SelectForCompaction(limitBytes);
        for (const auto& consumer : consumers) {
            YDB_LOG_TRACE("Request MemTable compaction of table with",
                {"table", consumer.first->Table},
                {"limit", HumanReadableBytes(consumer.second)});
            Send(consumer.first->Owner, new TEvMemTableCompact(consumer.first->Table, consumer.second));
        }
    }

    void ProcessResourceBrokerConfig(const TActorContext& ctx, NKikimrMemory::TMemoryStats& memoryStats, ui64 hardLimitBytes, ui64 activitiesLimitBytes) {
        TResourceBrokerConfig config{
            .LimitBytes = activitiesLimitBytes,
            .QueueLimits = {
                {NLocalDb::KqpResourceManagerQueue, GetQueryExecutionLimitBytes(Config, hardLimitBytes)},
                {NLocalDb::ColumnShardCompactionIndexationQueue, GetColumnTablesCompactionIndexationQueueLimitBytes(Config, hardLimitBytes)},
                {NLocalDb::ColumnShardCompactionTtlQueue, GetColumnTablesTtlQueueLimitBytes(Config, hardLimitBytes)},
                {NLocalDb::ColumnShardCompactionGeneralQueue, GetColumnTablesGeneralQueueQueueLimitBytes(Config, hardLimitBytes)},
                {NLocalDb::ColumnShardCompactionNormalizerQueue, GetColumnTablesNormalizerQueueLimitBytes(Config, hardLimitBytes)},
            }
        };

        for (auto &[name, limitBytes] : ResourceBrokerSelfConfig.QueueLimits) {
            if (config.QueueLimits.contains(name)) {
                config.QueueLimits[name] = limitBytes; // for backward compatibility
            }
        }

        // TODO: counters and logs for all column table queues
        ui64 queryExecutionConsumption = TAlignedPagePool::GetGlobalPagePoolSize();
        YDB_LOG_INFO_CTX(ctx, "Consumer QueryExecution state",
            {"consumption", HumanReadableBytes(queryExecutionConsumption)},
            {"limit", HumanReadableBytes(config.QueueLimits[NLocalDb::KqpResourceManagerQueue])});
        Counters->GetCounter("Consumer/QueryExecution/Consumption")->Set(queryExecutionConsumption);
        Counters->GetCounter("Consumer/QueryExecution/Limit")->Set(config.QueueLimits[NLocalDb::KqpResourceManagerQueue]);
        memoryStats.SetQueryExecutionConsumption(memoryStats.GetQueryExecutionConsumption() + queryExecutionConsumption);
        memoryStats.SetQueryExecutionLimit(config.QueueLimits[NLocalDb::KqpResourceManagerQueue]);

        // Note: for now ResourceBroker and its queues aren't MemoryController consumers and don't share limits with other caches
        ApplyResourceBrokerConfig(config);
    }

    void ApplyResourceBrokerConfig(TResourceBrokerConfig config) {
        if (config == CurrentResourceBrokerConfig) {
            return;
        }

        YDB_LOG_INFO("Apply ResourceBroker",
            {"config", config});

        TAutoPtr<TEvResourceBroker::TEvConfigure> configure = new TEvResourceBroker::TEvConfigure();
        configure->Merge = true;

        auto& record = configure->Record;
        record.MutableResourceLimit()->SetMemory(config.LimitBytes);

        for (auto &[name, limitBytes] : config.QueueLimits) {
            auto queue = record.AddQueues();
            queue->SetName(name);
            queue->MutableLimit()->SetMemory(limitBytes);
        }

        Send(MakeResourceBrokerID(), configure.Release());

        CurrentResourceBrokerConfig.emplace(std::move(config));
    }

    TConsumerCounters& GetConsumerCounters(EMemoryConsumerKind consumer) {
        auto it = ConsumerCounters.FindPtr(consumer);
        if (it) {
            return *it;
        }

        return ConsumerCounters.emplace(consumer, TConsumerCounters{
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Consumption"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Demand"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Reclaimable"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Reservation"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Limit"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMin"),
            Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMax")
        }).first->second;
    }

    void AddMemoryStats(const TConsumerState& consumer, NKikimrMemory::TMemoryStats& stats, ui64 limitBytes) const {
        const auto& traits = GetConsumerTraits(consumer.Kind);
        traits.WriteStats(stats, consumer, traits.StatsSummed, traits.StatsWithLimit, limitBytes);
    }

    TConsumerState BuildConsumerState(const TMemoryConsumer& consumer, ui64 hardLimitBytes) const {
        TConsumerState result(consumer);
        SetLimitBounds(result, hardLimitBytes);
        return result;
    }

    TConsumerState BuildConsumerState(EMemoryConsumerKind kind, const TConsumerCollection& collection, ui64 hardLimitBytes) const {
        TConsumerState result(kind, collection.GetTotal());
        SetLimitBounds(result, hardLimitBytes);
        return result;
    }

    void SetLimitBounds(TConsumerState& result, ui64 hardLimitBytes) const {
        const auto& traits = GetConsumerTraits(result.Kind);
        result.MinBytes = traits.GetMinBytes(Config, hardLimitBytes);
        result.MaxBytes = traits.GetMaxBytes(Config, hardLimitBytes);
        result.CanZeroLimit = traits.CanZeroLimit;

        if (result.MinBytes > result.MaxBytes) {
            result.MinBytes = result.MaxBytes;
        }
    }

private:
    const TDuration Interval;
    // Kinds whose single aggregate the controller feeds itself, with no event-registered actors behind it
    TMap<EMemoryConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
    // Kinds fed by event-registered actors; each holds one entry per registrant, keyed by its TActorId
    TMap<EMemoryConsumerKind, TConsumerCollection> Collections;
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
