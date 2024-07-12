#include <ydb/core/base/counters.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NMemory {

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

namespace {
    using namespace NActors;
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    struct TConsumerCounters {
        TCounterPtr Consumption;
        TCounterPtr LimitBytes;
        TCounterPtr LimitMinPercent;
        TCounterPtr LimitMaxPercent;
        TCounterPtr LimitMinBytes;
        TCounterPtr LimitMaxBytes;
    };

    struct TConsumerConfig {
        ui32 MinPercent;
        ui32 MaxPercent;
        ui64 MinBytes;
        ui64 MaxBytes;
    };

    class TMemoryController : public TActorBootstrapped<TMemoryController> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::MEMORY_CONTROLLER;
        }

        TMemoryController(TDuration interval, TIntrusivePtr<IMemoryConsumers> consumers, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
            : Interval(interval)
            , Consumers_(std::move(consumers))
            , Consumers(dynamic_cast<TMemoryConsumers&>(*Consumers_))
            , Counters(counters)
        {}

        void Bootstrap(const TActorContext& ctx) {
            
            LOG_NOTICE_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Bootstrapped");

            Become(&TThis::StateWork);
            ctx.Schedule(Interval, new TEvents::TEvWakeup());
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            }
        }

        void HandleWakeup(const TActorContext& ctx) noexcept {
            std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();
            auto allocatedMemory = TAllocState::GetAllocatedMemoryEstimate();

            auto consumers = Consumers.GetConsumersSnapshot();
            ui64 consumersConsumption = 0;
            for (const auto& consumer : consumers) {
                consumersConsumption += consumer.Consumption;
            }
            // allocatedMemory = externalConsumption + consumersConsumption
            ui64 externalConsumption = allocatedMemory - Min(allocatedMemory, consumersConsumption);
            ui64 availableMemory = memoryUsage->CGroupLimit; // TODO: get from config too

            LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Periodic memory stats"
                << " AnonRss: " << memoryUsage->AnonRss << " CGroupLimit: " << memoryUsage->CGroupLimit
                << " AvailableMemory: " << availableMemory << " AllocatedMemory: " << allocatedMemory
                << " ConsumersConsumption: " << consumersConsumption << " ExternalConsumption: " << externalConsumption);
            Counters->GetCounter("Stats/AnonRss")->Set(memoryUsage->AnonRss);
            Counters->GetCounter("Stats/CGroupLimit")->Set(memoryUsage->CGroupLimit);
            Counters->GetCounter("Stats/AllocatedMemory")->Set(allocatedMemory);
            Counters->GetCounter("Stats/ConsumersConsumption")->Set(consumersConsumption);
            Counters->GetCounter("Stats/ExternalConsumption")->Set(externalConsumption);

            TVector<std::pair<ui64, ui64>> consumerLimitBounds;
            for (const auto& consumer : consumers) {
                auto config = GetConsumerConfig(consumer.Kind);
                consumerLimitBounds.emplace_back(
                    Max(config.MinBytes, GetPercent(config.MinPercent, availableMemory)),
                    Min(config.MaxBytes, GetPercent(config.MaxPercent, availableMemory))
                );
            }

            for (auto index : xrange(consumers.size())) {
                auto& consumer = consumers[index];
                auto config = GetConsumerConfig(consumer.Kind);
                auto& bounds = consumerLimitBounds[index];
                auto& counters = GetConsumerCounters(consumer.Kind);

                ui64 limitBytes = bounds.first; // TODO

                LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_CONTROLLER, "Consumer " << consumer.Kind 
                    << " Consumption: " << consumer.Consumption << " LimitBytes: " << limitBytes
                    << " MinPercent: " << config.MinPercent << " MaxPercent: " << config.MaxPercent
                    << " MinBytes: " << config.MinBytes << " MaxBytes: " << config.MaxBytes);
                counters.Consumption->Set(consumer.Consumption);
                counters.LimitBytes->Set(limitBytes);
                counters.LimitMinPercent->Set(config.MinPercent);
                counters.LimitMaxPercent->Set(config.MaxPercent);
                counters.LimitMinBytes->Set(config.MinBytes);
                counters.LimitMaxBytes->Set(config.MaxBytes);

                ApplyLimit(consumer, limitBytes);
            }

            ctx.Schedule(Interval, new TEvents::TEvWakeup());
        }

        void ApplyLimit(TMemoryConsumerSnapshot consumer, ui64 limitBytes) {
            // TODO: dont send if queued already
            switch (consumer.Kind) {
                case EConsumerKind::SharedCache:
                    Send(MakeSharedPageCacheId(), new TEvMemoryLimit(limitBytes));
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
                Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/Limit"),
                Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinPercent"),
                Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxPercent"),
                Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMinBytes"),
                Counters->GetCounter(TStringBuilder() << "Consumer/" << consumer << "/LimitMaxBytes"),
            }).first->second;
        }

        TConsumerConfig GetConsumerConfig(EConsumerKind consumer) const {
            return {
                // TODO
                10, 10, 1000, 1000
            };
        }

        ui64 GetPercent(ui32 percent, ui64 value) {
            return value * percent / 100;
        }

    private:
        const TDuration Interval;
        const TIntrusivePtr<IMemoryConsumers> Consumers_;
        const TMemoryConsumers& Consumers;
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        TMap<EConsumerKind, TConsumerCounters> ConsumerCounters;
    };
}

TIntrusivePtr<IMemoryConsumers> CreateMemoryConsumers() {
    return MakeIntrusive<TMemoryConsumers>();
}

IActor* CreateMemoryController(TDuration interval, TIntrusivePtr<IMemoryConsumers> consumers, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TMemoryController(interval, consumers,
        GetServiceCounters(counters, "utils")->GetSubgroup("component", "memory_controller"));
}

}